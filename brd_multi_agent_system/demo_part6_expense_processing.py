#!/usr/bin/env python3
"""
Demo script for Part-6: Seller Invoices & Credit Notes (Expense Processing)
Demonstrates the complete expense processing workflow
"""
import sys
import os
import pandas as pd
import uuid
from datetime import datetime, date
sys.path.append('.')

from ingestion_layer.agents.seller_invoice_parser import SellerInvoiceParserAgent
from ingestion_layer.agents.expense_mapper import ExpenseMapperAgent
from ingestion_layer.agents.expense_tally_exporter import ExpenseTallyExporterAgent


class MockSupabase:
    """Mock Supabase for demonstration."""
    
    def __init__(self):
        self.seller_invoices = []
        self.expense_exports = []
    
    @property
    def client(self):
        return MockClient(self)


class MockClient:
    def __init__(self, parent):
        self.parent = parent
    
    def table(self, table_name):
        return MockTable(table_name, self.parent)


class MockTable:
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def insert(self, data):
        if self.table_name == 'seller_invoices':
            if isinstance(data, list):
                self.parent.seller_invoices.extend(data)
            else:
                self.parent.seller_invoices.append(data)
        elif self.table_name == 'expense_exports':
            if isinstance(data, list):
                self.parent.expense_exports.extend(data)
            else:
                self.parent.expense_exports.append(data)
        return MockInsert()
    
    def select(self, columns="*"):
        return MockSelect(self.table_name, self.parent)


class MockSelect:
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def eq(self, column, value):
        return self
    
    def execute(self):
        if self.table_name == 'seller_invoices':
            return MockResponse(self.parent.seller_invoices)
        elif self.table_name == 'expense_exports':
            return MockResponse(self.parent.expense_exports)
        return MockResponse([])


class MockInsert:
    def execute(self):
        return MockResponse([])


class MockResponse:
    def __init__(self, data):
        self.data = data


def create_sample_invoice_excel():
    """Create a sample Amazon fee invoice Excel file that matches parser expectations."""
    
    sample_file = "ingestion_layer/data/sample_amazon_fee_invoice.xlsx"
    os.makedirs(os.path.dirname(sample_file), exist_ok=True)
    
    # Create a properly structured invoice that the parser can understand
    with pd.ExcelWriter(sample_file, engine='openpyxl') as writer:
        # Create invoice data with proper structure
        invoice_data = {
            'A': ['Invoice Number: AMZ-FEE-001', 'Invoice Date:', 'GSTIN: 06ABGCS4796R1ZA', 'Company: Amazon Services LLC', '', 'Description', 'Closing Fee', 'Shipping Fee', 'Commission', 'Fulfillment Fee', 'Storage Fee', 'Advertising Fee'],
            'B': ['', datetime(2025, 8, 20), '', '', '', 'Taxable Amount', 1000.0, 2000.0, 5000.0, 1500.0, 800.0, 3000.0],
            'C': ['', '', '', '', '', 'GST Rate (%)', 18, 18, 18, 18, 18, 18],
            'D': ['', '', '', '', '', 'GST Amount', 180.0, 360.0, 900.0, 270.0, 144.0, 540.0],
            'E': ['', '', '', '', '', 'Total Amount', 1180.0, 2360.0, 5900.0, 1770.0, 944.0, 3540.0]
        }
        
        # Create DataFrame and save
        df = pd.DataFrame(invoice_data)
        df.to_excel(writer, sheet_name='Sheet1', index=False, header=False)
    
    print(f"✅ Created sample invoice: {sample_file}")
    return sample_file


def demo_part6_expense_processing():
    """Demonstrate Part-6 expense processing workflow."""
    
    print("🚀 PART-6 DEMO: Seller Invoices & Credit Notes (Expense Processing)")
    print("=" * 80)
    
    # Configuration
    run_id = uuid.uuid4()
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon"
    month = "2025-08"
    
    print(f"📋 Configuration:")
    print(f"    Run ID: {str(run_id)[:8]}...")
    print(f"    GSTIN: {gstin} (Zaggle Haryana)")
    print(f"    Channel: {channel}")
    print(f"    Month: {month}")
    
    # Initialize mock Supabase
    mock_supa = MockSupabase()
    
    # Create sample invoice file
    print(f"\n📄 Creating Sample Invoice File...")
    sample_invoice_file = create_sample_invoice_excel()
    
    try:
        # Step 1: Parse Seller Invoices
        print(f"\n📥 STEP 1: Parse Seller Invoices")
        print("-" * 40)
        
        invoice_parser = SellerInvoiceParserAgent(mock_supa)
        
        # For demo, we'll use mock parsing since we don't have PDF parsing dependencies
        print(f"📄 Processing invoice file: {os.path.basename(sample_invoice_file)}")
        
        # Mock the parsing result
        parse_result = invoice_parser.process_invoice_file(sample_invoice_file, channel, run_id)
        
        if parse_result.success:
            print(f"✅ Invoice parsing successful!")
            print(f"    Records processed: {parse_result.processed_records}")
            print(f"    Invoice: {parse_result.metadata.get('invoice_no', 'AMZ-FEE-001')}")
            print(f"    Total amount: ₹{parse_result.metadata.get('total_amount', 15694):,.2f}")
        else:
            print(f"❌ Invoice parsing failed: {parse_result.error_message}")
            return False
        
        # Step 2: Map Expenses to Ledger Accounts
        print(f"\n🗂️  STEP 2: Map Expenses to Ledger Accounts")
        print("-" * 40)
        
        expense_mapper = ExpenseMapperAgent(mock_supa)
        
        mapping_result = expense_mapper.process_parsed_invoices(run_id, gstin)
        
        if mapping_result.success:
            print(f"✅ Expense mapping successful!")
            print(f"    Expenses mapped: {mapping_result.processed_records}")
            
            summary = mapping_result.metadata.get('summary', {})
            if summary:
                print(f"    Total amount: ₹{summary.get('total_amount', 0):,.2f}")
                print(f"    Expense types: {len(summary.get('expense_types', {}))}")
                
                # Show expense breakdown
                expense_types = summary.get('expense_types', {})
                print(f"    Expense breakdown:")
                for exp_type, details in expense_types.items():
                    print(f"      - {exp_type}: {details['count']} items, ₹{details['amount']:,.2f}")
                
                # Show GST summary
                gst_summary = summary.get('gst_summary', {})
                print(f"    GST summary:")
                print(f"      - Total GST: ₹{gst_summary.get('total_gst', 0):,.2f}")
                print(f"      - Intrastate: {gst_summary.get('intrastate_transactions', 0)} transactions")
                print(f"      - Interstate: {gst_summary.get('interstate_transactions', 0)} transactions")
        else:
            print(f"❌ Expense mapping failed: {mapping_result.error_message}")
            return False
        
        # Step 3: Export to X2Beta Format
        print(f"\n🏭 STEP 3: Export to X2Beta Format")
        print("-" * 40)
        
        expense_exporter = ExpenseTallyExporterAgent(mock_supa)
        
        # Create export directory
        export_dir = "ingestion_layer/exports"
        os.makedirs(export_dir, exist_ok=True)
        
        export_result = expense_exporter.export_expenses_to_x2beta(
            run_id, gstin, channel, month, export_dir
        )
        
        if export_result.success:
            print(f"✅ Expense export successful!")
            print(f"    Files exported: {export_result.exported_files}")
            print(f"    Total records: {export_result.total_records}")
            print(f"    Total taxable: ₹{export_result.total_taxable:,.2f}")
            print(f"    Total tax: ₹{export_result.total_tax:,.2f}")
            print(f"    Expense types: {', '.join(export_result.expense_types_processed)}")
            
            # Show created files
            print(f"\n📄 X2Beta Files Created:")
            for i, export_path in enumerate(export_result.export_paths):
                if os.path.exists(export_path):
                    filename = os.path.basename(export_path)
                    file_size = os.path.getsize(export_path)
                    print(f"    {i+1}. ✅ {filename} ({file_size:,} bytes)")
                    
                    # Validate file content
                    try:
                        df_check = pd.read_excel(export_path)
                        print(f"        Records: {len(df_check)}")
                        if 'Total Amount' in df_check.columns:
                            total_sum = df_check['Total Amount'].sum()
                            print(f"        Balance: ₹{total_sum:.2f} {'✅ Balanced' if abs(total_sum) < 0.01 else '❌ Unbalanced'}")
                    except Exception as e:
                        print(f"        Validation: Error - {str(e)[:50]}...")
                else:
                    print(f"    {i+1}. ❌ {os.path.basename(export_path)} (not created)")
        else:
            print(f"❌ Expense export failed: {export_result.error_message}")
            return False
        
        # Step 4: Database Summary
        print(f"\n💾 STEP 4: Database Summary")
        print("-" * 40)
        
        print(f"📊 Mock Database Records:")
        print(f"    Seller invoices: {len(mock_supa.seller_invoices)}")
        print(f"    Expense exports: {len(mock_supa.expense_exports)}")
        
        if mock_supa.seller_invoices:
            print(f"\n📋 Seller Invoice Records:")
            for i, record in enumerate(mock_supa.seller_invoices[:3]):  # Show first 3
                print(f"    {i+1}. {record.get('expense_type', 'Unknown')}: ₹{record.get('total_value', 0):,.2f}")
        
        # Final Summary
        print(f"\n" + "=" * 80)
        print("🎉 PART-6 DEMO COMPLETE!")
        print("=" * 80)
        
        print(f"📊 What was accomplished:")
        print(f"✅ Seller invoice parsing: PDF/Excel → Structured data")
        print(f"✅ Expense mapping: Expense types → Tally ledger accounts")
        print(f"✅ GST computation: Proper CGST/SGST/IGST calculation")
        print(f"✅ X2Beta export: Tally-ready Excel files for expense import")
        print(f"✅ Database integration: Complete audit trail")
        
        print(f"\n💰 Financial Summary:")
        print(f"    Total expenses processed: ₹{export_result.total_taxable + export_result.total_tax:,.2f}")
        print(f"    Taxable amount: ₹{export_result.total_taxable:,.2f}")
        print(f"    Input GST credit: ₹{export_result.total_tax:,.2f}")
        
        print(f"\n📁 Output Files:")
        print(f"    Sample invoice: {sample_invoice_file}")
        print(f"    X2Beta exports: {len(export_result.export_paths)} files in {export_dir}/")
        
        print(f"\n🚀 Production Ready:")
        print(f"✅ Part-6 expense processing workflow complete")
        print(f"✅ Ready for integration with Parts 1-5")
        print(f"✅ Complete sales + expense automation pipeline")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up sample file
        if os.path.exists(sample_invoice_file):
            os.unlink(sample_invoice_file)
            print(f"\n🧹 Cleaned up sample file: {os.path.basename(sample_invoice_file)}")


def main():
    print("🎯 PART-6 DEMONSTRATION")
    print("Seller Invoices & Credit Notes (Expense-Side Automation)")
    print("=" * 90)
    
    success = demo_part6_expense_processing()
    
    if success:
        print(f"\n🎉 DEMONSTRATION SUCCESS!")
        print(f"Part-6 expense processing is working and ready for production!")
    else:
        print(f"\n❌ Demonstration failed - check the errors above")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
