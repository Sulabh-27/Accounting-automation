#!/usr/bin/env python3
"""
Populate Part-6 tables with test data for demonstration
"""
import sys
import os
import uuid
from datetime import datetime, date
sys.path.append('.')

from ingestion_layer.libs.supabase_client import SupabaseClientWrapper


def populate_part6_test_data():
    """Populate Part-6 tables with sample expense data."""
    
    print("🚀 POPULATING PART-6 TEST DATA")
    print("Adding sample seller invoices and expense data")
    print("=" * 60)
    
    try:
        # Initialize Supabase client
        supabase = SupabaseClientWrapper()
        
        # Test run_id (you can use an existing one from your runs table)
        run_id = str(uuid.uuid4())
        gstin = "06ABGCS4796R1ZA"
        channel = "amazon"
        
        print(f"📋 Configuration:")
        print(f"    Run ID: {run_id}")
        print(f"    GSTIN: {gstin}")
        print(f"    Channel: {channel}")
        
        # Sample seller invoice data
        seller_invoices = [
            {
                'id': str(uuid.uuid4()),
                'run_id': run_id,
                'channel': channel,
                'gstin': gstin,
                'invoice_no': 'AMZ-FEE-2025-001',
                'invoice_date': '2025-08-15',
                'expense_type': 'Closing Fee',
                'taxable_value': 1000.00,
                'gst_rate': 0.18,
                'cgst': 90.00,
                'sgst': 90.00,
                'igst': 0.00,
                'total_value': 1180.00,
                'ledger_name': 'Amazon Closing Fee',
                'file_path': 'sample_amazon_fee_invoice.pdf',
                'processing_status': 'processed'
            },
            {
                'id': str(uuid.uuid4()),
                'run_id': run_id,
                'channel': channel,
                'gstin': gstin,
                'invoice_no': 'AMZ-SHIP-2025-002',
                'invoice_date': '2025-08-16',
                'expense_type': 'Shipping Fee',
                'taxable_value': 500.00,
                'gst_rate': 0.18,
                'cgst': 45.00,
                'sgst': 45.00,
                'igst': 0.00,
                'total_value': 590.00,
                'ledger_name': 'Amazon Shipping Fee',
                'file_path': 'sample_amazon_shipping_invoice.pdf',
                'processing_status': 'processed'
            },
            {
                'id': str(uuid.uuid4()),
                'run_id': run_id,
                'channel': channel,
                'gstin': gstin,
                'invoice_no': 'AMZ-COMM-2025-003',
                'invoice_date': '2025-08-17',
                'expense_type': 'Commission',
                'taxable_value': 2000.00,
                'gst_rate': 0.18,
                'cgst': 180.00,
                'sgst': 180.00,
                'igst': 0.00,
                'total_value': 2360.00,
                'ledger_name': 'Amazon Commission',
                'file_path': 'sample_amazon_commission_invoice.pdf',
                'processing_status': 'processed'
            }
        ]
        
        # Insert seller invoices
        print(f"\n📄 Inserting {len(seller_invoices)} seller invoices...")
        
        for i, invoice in enumerate(seller_invoices):
            result = supabase.table('seller_invoices').insert(invoice).execute()
            if result.data:
                print(f"    ✅ Invoice {i+1}: {invoice['invoice_no']} - ₹{invoice['total_value']:,.2f}")
            else:
                print(f"    ❌ Failed to insert invoice {i+1}")
        
        # Sample expense mapping data
        expense_mappings = [
            {
                'id': str(uuid.uuid4()),
                'channel': 'amazon',
                'expense_type': 'Closing Fee',
                'ledger_name': 'Amazon Closing Fee',
                'gst_rate': 0.18,
                'is_input_gst': True,
                'is_active': True
            },
            {
                'id': str(uuid.uuid4()),
                'channel': 'amazon',
                'expense_type': 'Shipping Fee',
                'ledger_name': 'Amazon Shipping Fee',
                'gst_rate': 0.18,
                'is_input_gst': True,
                'is_active': True
            },
            {
                'id': str(uuid.uuid4()),
                'channel': 'amazon',
                'expense_type': 'Commission',
                'ledger_name': 'Amazon Commission',
                'gst_rate': 0.18,
                'is_input_gst': True,
                'is_active': True
            },
            {
                'id': str(uuid.uuid4()),
                'channel': 'flipkart',
                'expense_type': 'Collection Fee',
                'ledger_name': 'Flipkart Collection Fee',
                'gst_rate': 0.18,
                'is_input_gst': True,
                'is_active': True
            }
        ]
        
        # Insert expense mappings
        print(f"\n🗂️  Inserting {len(expense_mappings)} expense mappings...")
        
        for i, mapping in enumerate(expense_mappings):
            result = supabase.table('expense_mapping').insert(mapping).execute()
            if result.data:
                print(f"    ✅ Mapping {i+1}: {mapping['channel']} - {mapping['expense_type']}")
            else:
                print(f"    ❌ Failed to insert mapping {i+1}")
        
        # Sample expense export data
        expense_export = {
            'id': str(uuid.uuid4()),
            'run_id': run_id,
            'gstin': gstin,
            'channel': channel,
            'month': '2025-08',
            'file_path': f'ingestion_layer/exports/X2Beta_Expense_{gstin}_2025-08.xlsx',
            'total_records': len(seller_invoices),
            'total_taxable': sum(inv['taxable_value'] for inv in seller_invoices),
            'total_tax': sum(inv['cgst'] + inv['sgst'] + inv['igst'] for inv in seller_invoices),
            'export_status': 'completed'
        }
        
        # Insert expense export
        print(f"\n🏭 Inserting expense export record...")
        result = supabase.table('expense_exports').insert(expense_export).execute()
        if result.data:
            print(f"    ✅ Export record: {expense_export['total_records']} records, ₹{expense_export['total_taxable']:,.2f} taxable")
        else:
            print(f"    ❌ Failed to insert export record")
        
        # Summary
        print(f"\n" + "=" * 60)
        print("🎉 PART-6 TEST DATA POPULATED SUCCESSFULLY!")
        print("=" * 60)
        
        print(f"📊 Data Summary:")
        print(f"    ✅ Seller Invoices: {len(seller_invoices)} records")
        print(f"    ✅ Expense Mappings: {len(expense_mappings)} records")
        print(f"    ✅ Expense Exports: 1 record")
        
        total_amount = sum(inv['total_value'] for inv in seller_invoices)
        total_tax = sum(inv['cgst'] + inv['sgst'] + inv['igst'] for inv in seller_invoices)
        
        print(f"\n💰 Financial Summary:")
        print(f"    Total Taxable: ₹{expense_export['total_taxable']:,.2f}")
        print(f"    Total Tax: ₹{total_tax:,.2f}")
        print(f"    Total Amount: ₹{total_amount:,.2f}")
        
        print(f"\n🔍 Check Supabase Tables:")
        print(f"    📄 seller_invoices: Should now have {len(seller_invoices)} rows")
        print(f"    🗂️  expense_mapping: Should now have {len(expense_mappings)} rows")
        print(f"    🏭 expense_exports: Should now have 1 row")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Failed to populate test data: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("🎯 PART-6 TEST DATA POPULATION")
    print("Adding sample data to demonstrate Part-6 functionality")
    print("=" * 70)
    
    success = populate_part6_test_data()
    
    if success:
        print(f"\n🎉 SUCCESS!")
        print(f"Part-6 tables now have sample data for testing!")
        print(f"Check your Supabase dashboard to see the populated tables.")
    else:
        print(f"\n❌ Failed to populate test data - check the errors above")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
