from __future__ import annotations
import argparse
import glob
import os
import sys
import uuid
import pandas as pd
from dotenv import load_dotenv

from .libs.contracts import IngestionRequest
from .libs.supabase_client import SupabaseClientWrapper
from .libs.csv_utils import safe_read_csv, safe_read_excel_or_csv
from .agents.amazon_mtr_agent import AmazonMTRAgent
from .agents.amazon_str_agent import AmazonSTRAgent
from .agents.flipkart_agent import FlipkartAgent
from .agents.pepperfry_agent import PepperfryAgent
from .agents.schema_validator_agent import SchemaValidatorAgent
from .agents.universal_agent import UniversalAgent
from .agents.item_master_resolver import ItemMasterResolver
from .agents.ledger_mapper import LedgerMapper
from .agents.approval_agent import ApprovalAgent
from .agents.tax_engine import TaxEngine
from .agents.invoice_numbering import InvoiceNumberingAgent
from .agents.pivoter import PivotGeneratorAgent
from .agents.batch_splitter import BatchSplitterAgent
from .agents.tally_exporter import TallyExporterAgent
from .agents.seller_invoice_parser import SellerInvoiceParserAgent
from .agents.expense_mapper import ExpenseMapperAgent
from .agents.expense_tally_exporter import ExpenseTallyExporterAgent
from .agents.exception_handler import ExceptionHandler
from .agents.approval_workflow import ApprovalWorkflowAgent
from .agents.mis_generator import MISGeneratorAgent
from .agents.audit_logger import AuditLoggerAgent


def get_latest_processed_file(supa: SupabaseClientWrapper, run_id: uuid.UUID, file_patterns: list[str]) -> str:
    """
    Get the latest processed file, trying database first, then local files.
    
    Args:
        supa: Supabase client
        run_id: Current run ID
        file_patterns: List of glob patterns to try (in order of preference)
    
    Returns:
        Path to the latest file
    """
    # First try to get from database records (for Supabase storage)
    reports = supa.list_reports(run_id)
    
    if reports:
        # Use the most recent report from database
        latest_report = max(reports, key=lambda x: x.get('created_at', ''))
        latest_file = latest_report['file_path']
        print(f"  📁 Using file from database: {latest_file}")
        return latest_file
    
    # Fallback to local files
    for pattern in file_patterns:
        files = glob.glob(pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            print(f"  📁 Using local file: {latest_file}")
            return latest_file
    
    raise FileNotFoundError(f"No processed files found for patterns: {file_patterns}")


def run_pipeline(args: argparse.Namespace) -> int:
    load_dotenv()
    supa = SupabaseClientWrapper()

    run_id = uuid.uuid4()
    channel = args.channel
    gstin = args.gstin
    month = args.month
    
    # Handle full pipeline option
    if getattr(args, 'full_pipeline', False):
        args.enable_mapping = True
        args.enable_tax_invoice = True
        args.enable_pivot_batch = True
        args.enable_tally_export = True
        args.enable_expense_processing = True
        args.enable_exception_handling = True
        args.enable_mis_audit = True

    supa.insert_run_start(run_id, channel=channel, gstin=gstin, month=month)

    validator = SchemaValidatorAgent()
    uploaded_paths: list[str] = []
    status = "success"

    try:
        if args.agent == "amazon_mtr":
            req = IngestionRequest(run_id=run_id, channel=channel, gstin=gstin, month=month, report_type="amazon_mtr", file_path=args.input)
            path = AmazonMTRAgent().process(req, supa)
            df = safe_read_excel_or_csv(args.input)
            res = validator.validate(df, ["invoice_date", "gst_rate", "state_code"])  # validate raw has needed fields
            if not res.success:
                status = "failed"
            uploaded_paths.append(path)

        elif args.agent == "amazon_str":
            req = IngestionRequest(run_id=run_id, channel=channel, gstin=gstin, month=month, report_type="amazon_str", file_path=args.input)
            asin_map = {}
            if args.asin_map and os.path.exists(args.asin_map):
                m = safe_read_csv(args.asin_map)
                if {"asin", "sku"}.issubset({c.lower() for c in m.columns}):
                    m.columns = [c.lower() for c in m.columns]
                    asin_map = dict(zip(m["asin"], m["sku"]))
            path = AmazonSTRAgent().process(req, supa, asin_to_sku=asin_map)
            uploaded_paths.append(path)

        elif args.agent == "flipkart":
            req = IngestionRequest(run_id=run_id, channel=channel, gstin=gstin, month=month, report_type="flipkart", file_path=args.input)
            path = FlipkartAgent().process(req, supa)
            uploaded_paths.append(path)

        elif args.agent == "pepperfry":
            if not args.returns:
                status = "failed"
            else:
                req = IngestionRequest(run_id=run_id, channel=channel, gstin=gstin, month=month, report_type="pepperfry", file_path=args.input)
                path = PepperfryAgent().process(args.input, args.returns, req, supa)
                uploaded_paths.append(path)
        else:
            print(f"Unknown agent: {args.agent}", file=sys.stderr)
            status = "failed"

    except Exception as e:  # pragma: no cover - surfaced to user
        print(f"Error: {e}", file=sys.stderr)
        status = "failed"

    # Part-2: Item & Ledger Master Mapping (if enabled)
    if getattr(args, 'enable_mapping', False) and status == "success" and uploaded_paths:
        print("\n🔍 Starting Part-2: Item & Ledger Master Mapping...")
            
        try:
            # Initialize mapping agents
            item_resolver = ItemMasterResolver(supa)
            ledger_mapper = LedgerMapper(supa)
            
            # Process the latest normalized file
            try:
                latest_file = get_latest_processed_file(supa, run_id, ["ingestion_layer/data/normalized/*.csv"])
                df = safe_read_csv(latest_file)
                
                print(f"  📊 Processing {len(df)} records for mapping...")
                
                # Step 1: Resolve item mappings
                df, item_result = item_resolver.process_dataset(df)
                item_stats = item_resolver.get_mapping_stats(df)
                
                print(f"  📦 Item Mapping: {item_stats['mapped_items']}/{item_stats['total_items']} mapped ({item_stats['coverage_pct']}%)")
                if item_result.pending_approvals > 0:
                    print(f"  ⏳ {item_result.pending_approvals} item mappings pending approval")
                
                # Step 2: Resolve ledger mappings
                df, ledger_result = ledger_mapper.process_dataset(df)
                ledger_stats = ledger_mapper.get_mapping_stats(df)
                
                print(f"  📋 Ledger Mapping: {ledger_stats['mapped_records']}/{ledger_stats['total_records']} mapped ({ledger_stats['coverage_pct']}%)")
                if ledger_result.pending_approvals > 0:
                    print(f"  ⏳ {ledger_result.pending_approvals} ledger mappings pending approval")
                
                # Save enriched dataset
                enriched_path = latest_file.replace('.csv', '_enriched.csv')
                df.to_csv(enriched_path, index=False)
                print(f"  💾 Enriched dataset saved: {os.path.basename(enriched_path)}")
                
                # Check if approvals are needed
                total_pending = item_result.pending_approvals + ledger_result.pending_approvals
                if total_pending > 0:
                    status = "awaiting_approval"
                    print(f"  ⚠️  Status changed to 'awaiting_approval' - {total_pending} approvals needed")
                    
                    if getattr(args, 'interactive_approval', False):
                        print("\n🔍 Starting interactive approval session...")
                        approval_agent = ApprovalAgent(supa)
                        approval_agent.interactive_approval_session(approver=getattr(args, 'approver', None) or "manual")
                        
            except FileNotFoundError as e:
                print(f"  ❌ No processed files found: {e}")
                status = "mapping_failed"
                        
        except Exception as e:
            print(f"  ❌ Error in Part-2 mapping: {e}")
            status = "mapping_failed"

    # Part-3: Tax Engine & Invoice Numbering (if enabled)
    if getattr(args, 'enable_tax_invoice', False) and status in ["success", "awaiting_approval"] and uploaded_paths:
        print("\n🧮 Starting Part-3: Tax Engine & Invoice Numbering...")
        
        try:
            # Initialize Part-3 agents
            tax_engine = TaxEngine(supa)
            invoice_agent = InvoiceNumberingAgent(supa)
            
            # Process the latest enriched file (from Part-2) or normalized file (from Part-1)
            enriched_files = glob.glob("ingestion_layer/data/normalized/*_enriched.csv")
            if enriched_files:
                latest_file = max(enriched_files, key=os.path.getctime)
                print(f"  📁 Using enriched dataset: {os.path.basename(latest_file)}")
            else:
                normalized_files = glob.glob("ingestion_layer/data/normalized/*.csv")
                if normalized_files:
                    latest_file = max(normalized_files, key=os.path.getctime)
                    print(f"  📁 Using normalized dataset: {os.path.basename(latest_file)}")
                else:
                    raise FileNotFoundError("No processed datasets found for Part-3")
            
            df = safe_read_csv(latest_file)
            print(f"  📊 Processing {len(df)} records for tax computation and invoice numbering...")
            
            # Step 1: Tax Engine Processing
            print(f"  🧮 Step 1: Computing GST taxes...")
            df_with_tax, tax_result = tax_engine.process_dataset(df, channel, gstin, run_id)
            
            if tax_result.success:
                tax_summary = tax_engine.get_tax_summary(df_with_tax)
                print(f"    ✅ Tax computation: {tax_result.successful_computations}/{tax_result.processed_records} records")
                print(f"    💰 Total taxable: ₹{tax_summary['total_taxable_amount']:,.2f}")
                print(f"    🏛️  Total tax: ₹{tax_summary['total_tax']:,.2f}")
                print(f"    📊 Intrastate: {tax_summary['intrastate_records']}, Interstate: {tax_summary['interstate_records']}")
            else:
                print(f"    ❌ Tax computation failed: {tax_result.error_message}")
                status = "tax_computation_failed"
            
            # Step 2: Invoice Numbering (only if tax computation succeeded)
            if tax_result.success:
                print(f"  📋 Step 2: Generating invoice numbers...")
                df_final, invoice_result = invoice_agent.process_dataset(df_with_tax, channel, gstin, month, run_id)
                
                if invoice_result.success:
                    invoice_summary = invoice_agent.get_numbering_summary(df_final, channel)
                    print(f"    ✅ Invoice generation: {invoice_result.successful_generations}/{invoice_result.processed_records} records")
                    print(f"    🔢 Unique invoices: {invoice_result.unique_invoice_numbers}")
                    print(f"    🗺️  States covered: {invoice_summary['states_covered']}")
                    print(f"    📄 Pattern: {invoice_summary['pattern_example']}")
                    
                    # Save final enriched dataset
                    final_path = latest_file.replace('.csv', '_final.csv')
                    df_final.to_csv(final_path, index=False)
                    print(f"    💾 Final dataset saved: {os.path.basename(final_path)}")
                    
                    # Update status
                    if status == "awaiting_approval":
                        status = "awaiting_approval_with_tax_invoice"
                    else:
                        status = "success"
                        
                else:
                    print(f"    ❌ Invoice numbering failed: {invoice_result.error_message}")
                    status = "invoice_numbering_failed"
            
        except Exception as e:
            print(f"  ❌ Error in Part-3 processing: {e}")
            status = "part3_failed"

    # Part-4: Pivoting & Batch Splitting (if enabled)
    if getattr(args, 'enable_pivot_batch', False) and status in ["success", "awaiting_approval", "awaiting_approval_with_tax_invoice"] and uploaded_paths:
        print("\n📊 Starting Part-4: Pivoting & Batch Splitting...")
        
        try:
            # Initialize Part-4 agents
            pivot_agent = PivotGeneratorAgent(supa)
            batch_agent = BatchSplitterAgent(supa)
            
            # Process the latest final file (from Part-3) or enriched file (from Part-2) or normalized file (from Part-1)
            final_files = glob.glob("ingestion_layer/data/normalized/*_final.csv")
            if final_files:
                latest_file = max(final_files, key=os.path.getctime)
                print(f"  📁 Using final dataset: {os.path.basename(latest_file)}")
            else:
                enriched_files = glob.glob("ingestion_layer/data/normalized/*_enriched.csv")
                if enriched_files:
                    latest_file = max(enriched_files, key=os.path.getctime)
                    print(f"  📁 Using enriched dataset: {os.path.basename(latest_file)}")
                else:
                    normalized_files = glob.glob("ingestion_layer/data/normalized/*.csv")
                    if normalized_files:
                        latest_file = max(normalized_files, key=os.path.getctime)
                        print(f"  📁 Using normalized dataset: {os.path.basename(latest_file)}")
                    else:
                        raise FileNotFoundError("No processed datasets found for Part-4")
            
            df = safe_read_csv(latest_file)
            print(f"  📊 Processing {len(df)} records for pivoting and batch splitting...")
            
            # Step 1: Pivot Generation
            print(f"  📊 Step 1: Generating pivot summaries...")
            pivot_df, pivot_result = pivot_agent.process_dataset(df, channel, gstin, month, run_id)
            
            if pivot_result.success:
                pivot_summary = pivot_agent.get_pivot_summary(pivot_df)
                print(f"    ✅ Pivot generation: {pivot_result.pivot_records} summary records from {pivot_result.processed_records} transactions")
                print(f"    💰 Total taxable: ₹{pivot_summary['total_taxable_amount']:,.2f}")
                print(f"    🏛️  Total tax: ₹{pivot_summary['total_tax_amount']:,.2f}")
                print(f"    📋 Unique ledgers: {pivot_summary['unique_ledgers']}, FGs: {pivot_summary['unique_fgs']}")
                print(f"    📊 GST rates: {pivot_summary['unique_gst_rates']}")
                
                # Save pivot CSV
                pivot_path = latest_file.replace('.csv', '_pivot.csv')
                pivot_agent.export_pivot_csv(pivot_df, pivot_path)
                print(f"    💾 Pivot data saved: {os.path.basename(pivot_path)}")
            else:
                print(f"    ❌ Pivot generation failed: {pivot_result.error_message}")
                status = "pivot_generation_failed"
            
            # Step 2: Batch Splitting (only if pivot generation succeeded)
            if pivot_result.success and len(pivot_df) > 0:
                print(f"  📄 Step 2: Splitting into GST rate batches...")
                batch_files, batch_result = batch_agent.process_pivot_data(
                    pivot_df, channel, gstin, month, run_id
                )
                
                if batch_result.success:
                    batch_summary = batch_agent.get_batch_summary(batch_result.batch_summaries)
                    print(f"    ✅ Batch splitting: {batch_result.batch_files_created} files created")
                    print(f"    📊 GST rates processed: {batch_result.gst_rates_processed}")
                    print(f"    📄 Total records split: {batch_result.total_records_split}")
                    print(f"    ✅ Validation: {'PASSED' if batch_result.validation_passed else 'FAILED'}")
                    
                    # Display batch breakdown
                    print(f"    📋 Batch breakdown:")
                    for breakdown in batch_summary["batch_breakdown"]:
                        print(f"      {breakdown['gst_rate']}: {breakdown['records']} records, {breakdown['taxable']} taxable, {breakdown['tax']} tax")
                    
                    # Update status
                    if status == "awaiting_approval":
                        status = "awaiting_approval_with_pivot_batch"
                    elif status == "awaiting_approval_with_tax_invoice":
                        status = "awaiting_approval_complete"
                    else:
                        status = "summarized"  # Final status for complete pipeline
                        
                else:
                    print(f"    ❌ Batch splitting failed: {batch_result.error_message}")
                    status = "batch_splitting_failed"
            
        except Exception as e:
            print(f"  ❌ Error in Part-4 processing: {e}")
            status = "part4_failed"

    # Part-5: Tally Export (X2Beta Templates) (if enabled)
    if getattr(args, 'enable_tally_export', False) and status in ["success", "awaiting_approval", "awaiting_approval_with_tax_invoice", "awaiting_approval_with_pivot_batch", "awaiting_approval_complete", "summarized"] and uploaded_paths:
        print("\n🏭 Starting Part-5: Tally Export (X2Beta Templates)...")
        
        try:
            # Initialize Part-5 agent
            tally_exporter = TallyExporterAgent(supa)
            
            # Validate template availability for GSTIN
            template_validation = tally_exporter.validate_template_availability(gstin)
            
            if not template_validation['available']:
                print(f"  ❌ X2Beta template validation failed: {template_validation['error']}")
                status = "tally_template_missing"
            else:
                print(f"  ✅ X2Beta template validated: {template_validation['template_name']}")
                print(f"    Company: {template_validation['company_name']}")
                print(f"    State: {template_validation['state_name']}")
                
                # Process batch files from Part-4
                batch_directory = "ingestion_layer/data/batches"
                
                if os.path.exists(batch_directory):
                    export_result = tally_exporter.process_batch_files(
                        batch_directory, gstin, channel, month, run_id, "ingestion_layer/exports"
                    )
                    
                    if export_result.success:
                        export_summary = tally_exporter.get_export_summary([
                            {
                                'success': True,
                                'record_count': export_result.total_records,
                                'total_taxable': export_result.total_taxable,
                                'total_tax': export_result.total_tax,
                                'gst_rate': rate,
                                'file_size': 0  # Will be calculated per file
                            } for rate in export_result.gst_rates_processed or []
                        ])
                        
                        print(f"    ✅ Tally export: {export_result.exported_files}/{export_result.processed_files} files exported")
                        print(f"    💰 Total taxable: ₹{export_result.total_taxable:,.2f}")
                        print(f"    🏛️  Total tax: ₹{export_result.total_tax:,.2f}")
                        print(f"    📊 GST rates processed: {len(export_result.gst_rates_processed or [])}")
                        print(f"    📄 X2Beta files created:")
                        
                        for export_path in export_result.export_paths:
                            filename = os.path.basename(export_path)
                            print(f"      - {filename}")
                        
                        # Update status based on previous status
                        if status == "awaiting_approval":
                            status = "awaiting_approval_with_tally_export"
                        elif status == "awaiting_approval_with_tax_invoice":
                            status = "awaiting_approval_with_tally_export"
                        elif status == "awaiting_approval_with_pivot_batch":
                            status = "awaiting_approval_complete_with_tally"
                        elif status == "awaiting_approval_complete":
                            status = "awaiting_approval_complete_with_tally"
                        elif status == "summarized":
                            status = "exported"  # Final status for complete pipeline with Tally export
                        else:
                            status = "exported"
                        
                    else:
                        print(f"    ❌ Tally export failed: {export_result.error_message}")
                        status = "tally_export_failed"
                else:
                    print(f"  ⚠️  Batch directory not found: {batch_directory}")
                    print(f"    Run Part-4 first to generate batch files")
                    status = "batch_files_missing"
            
        except Exception as e:
            print(f"  ❌ Error in Part-5 processing: {e}")
            status = "part5_failed"

    # Part-6: Seller Invoices & Credit Notes (Expense Processing) (if enabled)
    if getattr(args, 'enable_expense_processing', False) and status in ["success", "awaiting_approval", "awaiting_approval_with_tax_invoice", "awaiting_approval_with_pivot_batch", "awaiting_approval_complete", "summarized", "exported"] and uploaded_paths:
        print("\n💰 Starting Part-6: Seller Invoices & Credit Notes (Expense Processing)...")
        
        try:
            # Check if seller invoice files are provided
            seller_invoice_files = getattr(args, 'seller_invoices', None)
            
            if seller_invoice_files:
                # Parse seller invoice files if provided
                if isinstance(seller_invoice_files, str):
                    seller_invoice_files = [seller_invoice_files]
                
                # Initialize Part-6 agents
                invoice_parser = SellerInvoiceParserAgent(supa)
                expense_mapper = ExpenseMapperAgent(supa)
                expense_exporter = ExpenseTallyExporterAgent(supa)
                
                print(f"  📄 Processing {len(seller_invoice_files)} seller invoice files...")
                
                # Step 1: Parse seller invoices
                parse_result = invoice_parser.process_multiple_invoices(seller_invoice_files, channel, run_id)
                
                if parse_result.success:
                    print(f"    ✅ Invoice parsing: {parse_result.processed_records} line items processed")
                    print(f"    📊 Processed invoices: {parse_result.metadata.get('processed_files', 0)}")
                    
                    # Step 2: Map expenses to ledger accounts
                    mapping_result = expense_mapper.process_parsed_invoices(run_id, gstin)
                    
                    if mapping_result.success:
                        print(f"    ✅ Expense mapping: {mapping_result.processed_records} expenses mapped")
                        
                        # Display mapping summary
                        summary = mapping_result.metadata.get('summary', {})
                        if summary:
                            print(f"    💰 Total amount: ₹{summary.get('total_amount', 0):,.2f}")
                            print(f"    📈 Expense types: {len(summary.get('expense_types', {}))}")
                            print(f"    🏛️  GST summary: ₹{summary.get('gst_summary', {}).get('total_gst', 0):,.2f}")
                        
                        # Step 3: Export expenses to X2Beta format
                        expense_export_result = expense_exporter.export_expenses_to_x2beta(
                            run_id, gstin, channel, month, "ingestion_layer/exports"
                        )
                        
                        if expense_export_result.success:
                            print(f"    ✅ Expense export: {expense_export_result.exported_files} X2Beta files created")
                            print(f"    💰 Total taxable: ₹{expense_export_result.total_taxable:,.2f}")
                            print(f"    🏛️  Total tax: ₹{expense_export_result.total_tax:,.2f}")
                            print(f"    📄 Expense types: {', '.join(expense_export_result.expense_types_processed)}")
                            
                            # Create combined sales + expense export if Part-5 was also run
                            if getattr(args, 'enable_tally_export', False) and status in ["exported"]:
                                print(f"    🔗 Creating combined sales + expense export...")
                                
                                # Find the latest sales export file
                                export_dir = "ingestion_layer/exports"
                                sales_files = [f for f in os.listdir(export_dir) if f.endswith('_x2beta.xlsx') and 'expense' not in f and 'combined' not in f]
                                
                                if sales_files:
                                    latest_sales_file = os.path.join(export_dir, max(sales_files))
                                    combined_result = expense_exporter.create_combined_sales_expense_export(
                                        run_id, gstin, channel, month, export_dir, latest_sales_file
                                    )
                                    
                                    if combined_result.success:
                                        print(f"    ✅ Combined export: Sales + Expense X2Beta file created")
                                        combined_files = [p for p in combined_result.export_paths if 'combined' in p]
                                        for combined_file in combined_files:
                                            print(f"      - {os.path.basename(combined_file)}")
                            
                            # Update status
                            if status == "exported":
                                status = "exported_with_expenses"
                            else:
                                status = "expense_processed"
                        else:
                            print(f"    ❌ Expense export failed: {expense_export_result.error_message}")
                            status = "expense_export_failed"
                    else:
                        print(f"    ❌ Expense mapping failed: {mapping_result.error_message}")
                        status = "expense_mapping_failed"
                else:
                    print(f"    ❌ Invoice parsing failed: {parse_result.error_message}")
                    status = "invoice_parsing_failed"
            else:
                print(f"  ℹ️  No seller invoice files provided - skipping expense processing")
                print(f"    Use --seller-invoices to provide invoice files for processing")
            
        except Exception as e:
            print(f"  ❌ Error in Part-6 processing: {e}")
            status = "part6_failed"

    # Part-7: Exception Handling & Approval Workflows (if enabled)
    if (not getattr(args, 'skip_exception_handling', False) and 
        getattr(args, 'enable_exception_handling', True) and 
        status in ["success", "awaiting_approval"] and uploaded_paths):
        print("\n🔍 Starting Part-7: Exception Handling & Approval Workflows...")
        
        try:
            # Initialize Part-7 agents
            exception_handler = ExceptionHandler(supa)
            approval_workflow = ApprovalWorkflowAgent(supa)
            
            # Process the latest dataset for exception detection
            try:
                latest_file = get_latest_processed_file(supa, run_id, [
                    "ingestion_layer/data/normalized/*_final.csv",
                    "ingestion_layer/data/normalized/*_enriched.csv", 
                    "ingestion_layer/data/normalized/*.csv"
                ])
                df = safe_read_csv(latest_file)
                
                print(f"  📊 Analyzing {len(df)} records for exceptions...")
                
                # Step 1: Detect mapping exceptions
                print(f"  🔍 Step 1: Detecting mapping exceptions...")
                mapping_result = exception_handler.detect_mapping_exceptions(df, run_id, "sales")
                
                if mapping_result.exceptions_detected > 0:
                    print(f"    ⚠️  Found {mapping_result.exceptions_detected} mapping exceptions")
                    print(f"    📋 Requires approval: {mapping_result.requires_approval}")
                else:
                    print(f"    ✅ No mapping exceptions detected")
                
                # Step 2: Detect GST exceptions
                print(f"  🔍 Step 2: Detecting GST exceptions...")
                gst_result = exception_handler.detect_gst_exceptions(df, run_id, "sales")
                
                if gst_result.exceptions_detected > 0:
                    print(f"    ⚠️  Found {gst_result.exceptions_detected} GST exceptions")
                    print(f"    📋 Requires approval: {gst_result.requires_approval}")
                else:
                    print(f"    ✅ No GST exceptions detected")
                
                # Step 3: Detect invoice exceptions
                print(f"  🔍 Step 3: Detecting invoice exceptions...")
                invoice_result = exception_handler.detect_invoice_exceptions(df, run_id, "sales")
                
                if invoice_result.exceptions_detected > 0:
                    print(f"    ⚠️  Found {invoice_result.exceptions_detected} invoice exceptions")
                    print(f"    📋 Requires approval: {invoice_result.requires_approval}")
                else:
                    print(f"    ✅ No invoice exceptions detected")
                
                # Step 4: Detect data quality exceptions
                print(f"  🔍 Step 4: Detecting data quality exceptions...")
                data_result = exception_handler.detect_data_quality_exceptions(df, run_id, "sales")
                
                if data_result.exceptions_detected > 0:
                    print(f"    ⚠️  Found {data_result.exceptions_detected} data quality exceptions")
                    print(f"    📋 Requires approval: {data_result.requires_approval}")
                else:
                    print(f"    ✅ No data quality exceptions detected")
                
                # Step 5: Save exceptions to database
                print(f"  💾 Step 5: Saving exceptions to database...")
                save_success = exception_handler.save_exceptions_to_database()
                
                if save_success:
                    print(f"    ✅ Exceptions saved successfully")
                else:
                    print(f"    ❌ Failed to save exceptions")
                
                # Step 6: Process approval workflow
                print(f"  🔄 Step 6: Processing approval workflow...")
                
                # Create approval requests for exceptions that require approval
                total_exceptions = (mapping_result.exceptions_detected + 
                                  gst_result.exceptions_detected + 
                                  invoice_result.exceptions_detected + 
                                  data_result.exceptions_detected)
                
                total_approvals_needed = (mapping_result.requires_approval + 
                                        gst_result.requires_approval + 
                                        invoice_result.requires_approval + 
                                        data_result.requires_approval)
                
                if total_approvals_needed > 0:
                    print(f"    📋 {total_approvals_needed} items require human approval")
                    
                    # Get approval summary
                    approval_summary = approval_workflow.get_approval_summary(run_id)
                    
                    if approval_summary.pending_requests > 0:
                        status = "awaiting_approval"
                        print(f"    ⏳ Status changed to 'awaiting_approval' - {approval_summary.pending_requests} approvals pending")
                    else:
                        print(f"    ✅ All approvals processed automatically")
                else:
                    print(f"    ✅ No approvals required")
                
                # Step 7: Exception summary
                print(f"  📊 Step 7: Exception Summary...")
                exception_summary = exception_handler.get_exception_summary()
                
                print(f"    📈 Total exceptions: {exception_summary['total_exceptions']}")
                if exception_summary['by_severity']:
                    for severity, count in exception_summary['by_severity'].items():
                        print(f"    📊 {severity.title()}: {count}")
                
                # Determine final processing status
                critical_exceptions = (mapping_result.critical_exceptions + 
                                     gst_result.critical_exceptions + 
                                     invoice_result.critical_exceptions + 
                                     data_result.critical_exceptions)
                
                if critical_exceptions > 0:
                    status = "critical_exceptions"
                    print(f"    🚨 {critical_exceptions} critical exceptions detected - processing halted")
                elif total_approvals_needed > 0 and approval_summary.pending_requests > 0:
                    status = "awaiting_approval"
                elif total_exceptions > 0:
                    print(f"    ⚠️  {total_exceptions} exceptions detected but processing can continue")
                else:
                    print(f"    ✅ No exceptions detected - processing completed successfully")
                
            except FileNotFoundError as e:
                print(f"  ❌ No processed files found for exception analysis: {e}")
                # Continue without exception handling if no files found
                
        except Exception as e:
            print(f"  ❌ Error in Part-7 processing: {e}")
            # Don't fail the entire pipeline for Part-7 errors
            print(f"  ℹ️  Continuing without exception handling...")

    # Part-8: MIS & Audit Trail (if enabled)
    if getattr(args, 'enable_mis_audit', False) and status in ["success", "awaiting_approval"] and uploaded_paths:
        print("\n📊 Starting Part-8: MIS & Audit Trail...")
        
        try:
            # Initialize Part-8 agents
            audit_agent = AuditLoggerAgent(supa)
            mis_agent = MISGeneratorAgent(supa)
            
            # Start audit session
            session_id = audit_agent.start_audit_session(
                run_id=run_id,
                channel=channel,
                gstin=gstin,
                month=month,
                input_file=args.input
            )
            
            print(f"  🔍 Audit session started: {session_id}")
            
            # Generate MIS report
            print(f"  📊 Generating MIS report...")
            mis_result = mis_agent.generate_mis_report(
                run_id=run_id,
                channel=channel,
                gstin=gstin,
                month=month,
                report_type="monthly",
                export_formats=["csv", "excel", "database"]
            )
            
            if mis_result.success:
                print(f"  ✅ MIS report generated successfully")
                print(f"     📄 CSV Export: {mis_result.csv_export_path}")
                if mis_result.excel_export_path:
                    print(f"     📊 Excel Export: {mis_result.excel_export_path}")
                print(f"     💾 Database ID: {mis_result.report_id}")
                print(f"     ⏱️  Processing time: {mis_result.processing_time_seconds:.2f} seconds")
                
                # Log key metrics
                if mis_result.mis_report:
                    report = mis_result.mis_report
                    print(f"  📈 Key Metrics:")
                    print(f"     💰 Net Sales: ₹{report.sales_metrics.net_sales:,.2f}")
                    print(f"     💸 Total Expenses: ₹{report.expense_metrics.total_expenses:,.2f}")
                    print(f"     📊 Gross Profit: ₹{report.profitability_metrics.gross_profit:,.2f}")
                    print(f"     📈 Profit Margin: {report.profitability_metrics.profit_margin:.1f}%")
                    print(f"     🏛️  GST Liability: ₹{report.gst_metrics.gst_liability:,.2f}")
                    print(f"     ⭐ Quality Score: {report.data_quality_score:.1f}%")
                    
                    # Add MIS export paths to uploaded_paths for summary
                    if mis_result.csv_export_path:
                        uploaded_paths.append(mis_result.csv_export_path)
                    if mis_result.excel_export_path:
                        uploaded_paths.append(mis_result.excel_export_path)
                
            else:
                print(f"  ❌ MIS report generation failed: {mis_result.error_message}")
                # Don't fail the entire pipeline for MIS errors
                print(f"  ℹ️  Continuing without MIS report...")
            
            # Generate audit trail summary
            print(f"  🔍 Generating audit trail summary...")
            audit_summary = audit_agent.get_audit_summary(run_id)
            performance_metrics = audit_agent.get_performance_metrics()
            
            if audit_summary:
                print(f"  📋 Audit Summary:")
                print(f"     📊 Total Events: {audit_summary.get('total_events', 0)}")
                print(f"     ❌ Error Count: {audit_summary.get('error_count', 0)}")
                print(f"     ✋ Approval Count: {audit_summary.get('approval_count', 0)}")
                print(f"     ⏱️  Duration: {audit_summary.get('duration_seconds', 0):.2f} seconds")
            
            if performance_metrics.get('operation_metrics'):
                print(f"  ⚡ Performance Metrics:")
                for operation, metrics in performance_metrics['operation_metrics'].items():
                    print(f"     {operation}: {metrics['count']} ops, avg {metrics['average_time']:.2f}s")
            
            # End audit session with final metrics
            final_metrics = {
                'mis_generated': mis_result.success,
                'processing_time_seconds': mis_result.processing_time_seconds if mis_result.success else 0,
                'audit_events': audit_summary.get('total_events', 0) if audit_summary else 0,
                'performance_operations': len(performance_metrics.get('operation_metrics', {}))
            }
            
            session_summary = audit_agent.end_audit_session(
                session_id=session_id,
                status="completed" if mis_result.success else "partial",
                final_metrics=final_metrics
            )
            
            print(f"  ✅ Part-8 completed successfully")
            print(f"     🔍 Audit session: {session_summary.get('session_id', 'N/A')}")
            print(f"     📊 MIS report: {'Generated' if mis_result.success else 'Failed'}")
            print(f"     ⏱️  Total Part-8 time: {session_summary.get('duration_seconds', 0):.2f} seconds")
            
        except Exception as e:
            print(f"  ❌ Error in Part-8 processing: {e}")
            # Don't fail the entire pipeline for Part-8 errors
            print(f"  ℹ️  Continuing without MIS & audit trail...")

        # Finish run
        supa.update_run_finish(run_id, status=status)

        print(f"\nRun Summary:")
        print(f"  Run ID: {run_id}")
        print(f"  Status: {status}")
        if uploaded_paths:
            print("  Uploaded files:")
            for p in uploaded_paths:
                print(f"   - {p}")
        
        if status == "awaiting_approval":
            print(f"\nNext Steps:")
            print("  Run approval workflow:")
            print(f"  python -m ingestion_layer.approval_cli --approver {args.approver or 'manual'}")
        
        return 0 if status in ["success", "awaiting_approval"] else 1


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ingestion & Normalization Orchestrator")
    p.add_argument("--agent", required=True, choices=["amazon_mtr", "amazon_str", "flipkart", "pepperfry"]) 
    p.add_argument("--input", required=True, help="Path to input CSV")
    p.add_argument("--returns", help="Path to returns CSV (Pepperfry only)")
    p.add_argument("--asin-map", dest="asin_map", help="Path to ASIN->SKU map CSV (Amazon STR only)")
    p.add_argument("--channel", required=True)
    p.add_argument("--gstin", required=True)
    p.add_argument("--month", required=True)
    # Part-2 arguments
    p.add_argument("--enable-mapping", action="store_true", help="Enable Part-2 item & ledger mapping")
    p.add_argument("--interactive-approval", action="store_true", help="Start interactive approval session")
    p.add_argument("--approver", help="Name of the approver (default: manual)")
    
    # Part-3 arguments
    p.add_argument("--enable-tax-invoice", action="store_true", help="Enable Part-3 tax computation & invoice numbering")
    
    # Part-4 arguments
    p.add_argument("--enable-pivot-batch", action="store_true", help="Enable Part-4 pivoting & batch splitting")
    
    # Part-5 arguments
    p.add_argument("--enable-tally-export", action="store_true", help="Enable Part-5 Tally export (X2Beta templates)")
    
    # Part-6 arguments
    p.add_argument("--enable-expense-processing", action="store_true", help="Enable Part-6 seller invoice & expense processing")
    p.add_argument("--seller-invoices", nargs='+', help="Path(s) to seller invoice files (PDF/Excel)")
    
    # Part-7 arguments
    p.add_argument("--enable-exception-handling", action="store_true", help="Enable Part-7 exception handling & approval workflows")
    p.add_argument("--skip-exception-handling", action="store_true", help="Skip Part-7 exception handling (for testing)")
    
    # Part-8 arguments
    p.add_argument("--enable-mis-audit", action="store_true", help="Enable Part-8 MIS & audit trail generation")
    p.add_argument("--mis-export-formats", nargs='+', default=["csv", "database"], help="MIS export formats (csv, excel, database)")
    
    # Complete pipeline
    p.add_argument("--full-pipeline", action="store_true", help="Enable complete pipeline (Parts 1+2+3+4+5+6+7+8)")
    
    return p


if __name__ == "__main__":
    parser = build_arg_parser()
    sys.exit(run_pipeline(parser.parse_args()))
