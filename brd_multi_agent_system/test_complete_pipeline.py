#!/usr/bin/env python3
"""
Test complete pipeline without Supabase dependencies
"""
import sys
import os
import pandas as pd
import uuid
sys.path.append('.')

from ingestion_layer.agents.amazon_mtr_agent import AmazonMTRAgent
from ingestion_layer.agents.item_master_resolver import ItemMasterResolver
from ingestion_layer.agents.ledger_mapper import LedgerMapper
from ingestion_layer.agents.tax_engine import TaxEngine
from ingestion_layer.agents.invoice_numbering import InvoiceNumberingAgent
from ingestion_layer.agents.pivoter import PivotGeneratorAgent
from ingestion_layer.agents.batch_splitter import BatchSplitterAgent
from ingestion_layer.libs.contracts import IngestionRequest


class MockSupabase:
    """Mock Supabase client to avoid file upload issues."""
    
    def __init__(self):
        self.data = {}
    
    def insert_run_start(self, run_id, **kwargs):
        print(f"📝 Mock: Started run {run_id}")
    
    def update_run_finish(self, run_id, status):
        print(f"📝 Mock: Finished run {run_id} with status: {status}")
    
    def upload_file(self, file_path, bucket_path):
        print(f"📝 Mock: Would upload {file_path} to {bucket_path}")
        return f"mock://{bucket_path}"
    
    @property
    def client(self):
        return MockClient()


class MockClient:
    """Mock Supabase client."""
    
    def table(self, table_name):
        return MockTable(table_name)


class MockTable:
    """Mock Supabase table."""
    
    def __init__(self, table_name):
        self.table_name = table_name
    
    def insert(self, data):
        print(f"📝 Mock: Would insert {len(data)} records to {self.table_name}")
        return MockInsert()
    
    def select(self, columns="*"):
        return MockSelect()


class MockSelect:
    """Mock select query."""
    
    def eq(self, column, value):
        return self
    
    def in_(self, column, values):
        return self
    
    def execute(self):
        return MockResponse([])


class MockInsert:
    """Mock insert query."""
    
    def execute(self):
        return MockResponse([])


class MockResponse:
    """Mock response."""
    
    def __init__(self, data):
        self.data = data


def test_complete_pipeline():
    """Test the complete pipeline with mock Supabase."""
    
    print("🚀 TESTING COMPLETE PIPELINE (Parts 1+2+3+4)")
    print("=" * 60)
    
    # Initialize mock Supabase
    supa = MockSupabase()
    
    # Configuration
    run_id = uuid.uuid4()
    channel = "amazon"
    gstin = "06ABGCS4796R1ZA"
    month = "2025-08"
    input_file = "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx"
    
    print(f"📋 Configuration:")
    print(f"    Run ID: {run_id}")
    print(f"    Channel: {channel}")
    print(f"    GSTIN: {gstin}")
    print(f"    Month: {month}")
    print(f"    Input: {os.path.basename(input_file)}")
    
    try:
        # Part 1: Ingestion & Normalization
        print(f"\n📥 PART 1: Ingestion & Normalization")
        print("-" * 40)
        
        req = IngestionRequest(
            run_id=run_id, 
            channel=channel, 
            gstin=gstin, 
            month=month, 
            report_type="amazon_mtr", 
            file_path=input_file
        )
        
        agent = AmazonMTRAgent()
        result = agent.process(req)
        
        if result.success:
            print(f"✅ Part 1 Success: {result.processed_records} records processed")
            
            # Load the normalized data
            normalized_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                              if f.startswith("amazon_mtr_") and f.endswith(".csv")]
            
            if not normalized_files:
                print("❌ No normalized files found")
                return False
            
            latest_file = f"ingestion_layer/data/normalized/{max(normalized_files)}"
            df = pd.read_csv(latest_file)
            print(f"    📊 Loaded {len(df)} normalized records")
            
        else:
            print(f"❌ Part 1 Failed: {result.error_message}")
            return False
        
        # Part 2: Item & Ledger Mapping
        print(f"\n🗺️  PART 2: Item & Ledger Mapping")
        print("-" * 40)
        
        # Item Master Resolver
        item_resolver = ItemMasterResolver(supa)
        df_with_items, item_result = item_resolver.resolve_items(df, run_id)
        
        if item_result.success:
            print(f"✅ Item mapping: {item_result.mapped_count}/{len(df)} items resolved")
        else:
            print(f"⚠️  Item mapping had issues: {item_result.errors}")
        
        # Ledger Mapper
        ledger_mapper = LedgerMapper(supa)
        df_enriched, ledger_result = ledger_mapper.map_ledgers(df_with_items, channel, run_id)
        
        if ledger_result.success:
            print(f"✅ Ledger mapping: {ledger_result.mapped_count}/{len(df)} ledgers resolved")
            
            # Save enriched data
            enriched_path = latest_file.replace('.csv', '_enriched.csv')
            df_enriched.to_csv(enriched_path, index=False)
            print(f"    💾 Enriched data saved: {os.path.basename(enriched_path)}")
            
        else:
            print(f"⚠️  Ledger mapping had issues: {ledger_result.errors}")
        
        # Part 3: Tax Engine & Invoice Numbering
        print(f"\n🧮 PART 3: Tax Engine & Invoice Numbering")
        print("-" * 40)
        
        # Tax Engine
        tax_engine = TaxEngine(supa)
        df_with_tax, tax_result = tax_engine.process_dataset(df_enriched, channel, gstin, run_id)
        
        if tax_result.success:
            tax_summary = tax_engine.get_tax_summary(df_with_tax)
            print(f"✅ Tax computation: {tax_result.successful_computations}/{tax_result.processed_records} records")
            print(f"    💰 Total taxable: ₹{tax_summary['total_taxable_amount']:,.2f}")
            print(f"    🏛️  Total tax: ₹{tax_summary['total_tax']:,.2f}")
        else:
            print(f"❌ Tax computation failed: {tax_result.error_message}")
            return False
        
        # Invoice Numbering
        invoice_agent = InvoiceNumberingAgent(supa)
        df_final, invoice_result = invoice_agent.process_dataset(df_with_tax, channel, gstin, month, run_id)
        
        if invoice_result.success:
            print(f"✅ Invoice numbering: {invoice_result.successful_generations}/{invoice_result.processed_records} records")
            print(f"    🔢 Unique invoices: {invoice_result.unique_invoice_numbers}")
            
            # Save final data
            final_path = latest_file.replace('.csv', '_final.csv')
            df_final.to_csv(final_path, index=False)
            print(f"    💾 Final data saved: {os.path.basename(final_path)}")
            
        else:
            print(f"❌ Invoice numbering failed: {invoice_result.error_message}")
            return False
        
        # Part 4: Pivoting & Batch Splitting
        print(f"\n📊 PART 4: Pivoting & Batch Splitting")
        print("-" * 40)
        
        # Pivot Generation
        pivot_agent = PivotGeneratorAgent(supa)
        pivot_df, pivot_result = pivot_agent.process_dataset(df_final, channel, gstin, month, run_id)
        
        if pivot_result.success:
            pivot_summary = pivot_agent.get_pivot_summary(pivot_df)
            print(f"✅ Pivot generation: {pivot_result.pivot_records} summary records from {pivot_result.processed_records} transactions")
            print(f"    💰 Total taxable: ₹{pivot_summary['total_taxable_amount']:,.2f}")
            print(f"    📋 Unique ledgers: {pivot_summary['unique_ledgers']}, FGs: {pivot_summary['unique_fgs']}")
            
            # Save pivot data
            pivot_path = latest_file.replace('.csv', '_pivot.csv')
            pivot_agent.export_pivot_csv(pivot_df, pivot_path)
            print(f"    💾 Pivot data saved: {os.path.basename(pivot_path)}")
            
        else:
            print(f"❌ Pivot generation failed: {pivot_result.error_message}")
            return False
        
        # Batch Splitting
        batch_agent = BatchSplitterAgent(supa)
        batch_files, batch_result = batch_agent.process_pivot_data(
            pivot_df, channel, gstin, month, run_id, "ingestion_layer/data/batches"
        )
        
        if batch_result.success:
            batch_summary = batch_agent.get_batch_summary(batch_result.batch_summaries)
            print(f"✅ Batch splitting: {batch_result.batch_files_created} files created")
            print(f"    📊 GST rates processed: {batch_result.gst_rates_processed}")
            print(f"    📄 Files created:")
            
            for file_path in batch_files:
                filename = os.path.basename(file_path)
                print(f"      - {filename}")
            
        else:
            print(f"❌ Batch splitting failed: {batch_result.error_message}")
            return False
        
        # Final Summary
        print(f"\n" + "=" * 60)
        print("🎉 COMPLETE PIPELINE SUCCESS!")
        print("=" * 60)
        
        print(f"📊 Processing Summary:")
        print(f"    Input records: {result.processed_records}")
        print(f"    Item mapping: {item_result.mapped_count} resolved")
        print(f"    Ledger mapping: {ledger_result.mapped_count} resolved")
        print(f"    Tax computations: {tax_result.successful_computations} successful")
        print(f"    Invoice numbers: {invoice_result.unique_invoice_numbers} unique")
        print(f"    Pivot records: {pivot_result.pivot_records} summaries")
        print(f"    Batch files: {batch_result.batch_files_created} GST rate files")
        
        print(f"\n💰 Financial Summary:")
        print(f"    Total taxable: ₹{tax_summary['total_taxable_amount']:,.2f}")
        print(f"    Total tax: ₹{tax_summary['total_tax']:,.2f}")
        print(f"    Intrastate: {tax_summary['intrastate_records']} transactions")
        print(f"    Interstate: {tax_summary['interstate_records']} transactions")
        
        print(f"\n📁 Output Files:")
        print(f"    Normalized: {os.path.basename(latest_file)}")
        print(f"    Enriched: {os.path.basename(enriched_path)}")
        print(f"    Final: {os.path.basename(final_path)}")
        print(f"    Pivot: {os.path.basename(pivot_path)}")
        print(f"    Batches: {len(batch_files)} files in ingestion_layer/data/batches/")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("🚀 COMPLETE PIPELINE TEST")
    print("Testing all 4 parts without Supabase upload dependencies")
    print("=" * 70)
    
    success = test_complete_pipeline()
    
    if success:
        print(f"\n🎉 All parts working correctly!")
        print(f"🚀 The complete pipeline is ready for production")
        print(f"\nTo fix the Supabase upload issue:")
        print(f"1. Set up your Supabase storage bucket named 'multi-agent-system'")
        print(f"2. Configure proper credentials in .env file")
        print(f"3. Run the pipeline again with --full-pipeline")
    else:
        print(f"\n❌ Pipeline test failed - check the errors above")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
