#!/usr/bin/env python3
"""
Simple demonstration of the complete pipeline using existing data
"""
import sys
import os
import pandas as pd
import uuid
sys.path.append('.')

from ingestion_layer.agents.tax_engine import TaxEngine
from ingestion_layer.agents.invoice_numbering import InvoiceNumberingAgent
from ingestion_layer.agents.pivoter import PivotGeneratorAgent
from ingestion_layer.agents.batch_splitter import BatchSplitterAgent


class MockSupabase:
    """Mock Supabase client."""
    
    def __init__(self):
        pass
    
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


def demo_parts_3_and_4():
    """Demo Parts 3 and 4 with existing normalized data."""
    
    print("🚀 DEMO: Parts 3 & 4 with Existing Data")
    print("=" * 50)
    
    # Find existing normalized data
    normalized_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                       if f.startswith("amazon_mtr_") and f.endswith(".csv") and not f.endswith("_enriched.csv") and not f.endswith("_final.csv")]
    
    if not normalized_files:
        print("❌ No normalized data found. Run Part 1 first!")
        return False
    
    latest_file = f"ingestion_layer/data/normalized/{max(normalized_files)}"
    print(f"📁 Using data: {os.path.basename(latest_file)}")
    
    # Load the data
    df = pd.read_csv(latest_file)
    print(f"📊 Loaded {len(df)} records")
    
    # Add mock FG and ledger data for demonstration
    df['fg'] = df['sku'].apply(lambda x: x.split('-')[0] if '-' in x else x[:10])
    df['ledger_name'] = df['state_code'].apply(lambda x: f"Amazon {x}")
    
    print(f"📋 Added mock FG and ledger mappings")
    
    # Initialize components
    supabase = MockSupabase()
    
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    try:
        # Part 3: Tax Engine & Invoice Numbering
        print(f"\n🧮 PART 3: Tax Engine & Invoice Numbering")
        print("-" * 40)
        
        # Tax Engine
        tax_engine = TaxEngine(supabase)
        df_with_tax, tax_result = tax_engine.process_dataset(df, channel, gstin, run_id)
        
        if tax_result.success:
            tax_summary = tax_engine.get_tax_summary(df_with_tax)
            print(f"✅ Tax computation: {tax_result.successful_computations}/{tax_result.processed_records} records")
            print(f"    💰 Total taxable: ₹{tax_summary['total_taxable_amount']:,.2f}")
            print(f"    🏛️  Total tax: ₹{tax_summary['total_tax']:,.2f}")
            print(f"    📊 Intrastate: {tax_summary['intrastate_records']}, Interstate: {tax_summary['interstate_records']}")
        else:
            print(f"❌ Tax computation failed: {tax_result.error_message}")
            return False
        
        # Invoice Numbering
        invoice_agent = InvoiceNumberingAgent(supabase)
        df_final, invoice_result = invoice_agent.process_dataset(df_with_tax, channel, gstin, month, run_id)
        
        if invoice_result.success:
            print(f"✅ Invoice numbering: {invoice_result.successful_generations}/{invoice_result.processed_records} records")
            print(f"    🔢 Unique invoices: {invoice_result.unique_invoice_numbers}")
            
            # Save final data
            final_path = latest_file.replace('.csv', '_demo_final.csv')
            df_final.to_csv(final_path, index=False)
            print(f"    💾 Final data saved: {os.path.basename(final_path)}")
            
        else:
            print(f"❌ Invoice numbering failed: {invoice_result.error_message}")
            return False
        
        # Part 4: Pivoting & Batch Splitting
        print(f"\n📊 PART 4: Pivoting & Batch Splitting")
        print("-" * 40)
        
        # Pivot Generation
        pivot_agent = PivotGeneratorAgent(supabase)
        pivot_df, pivot_result = pivot_agent.process_dataset(df_final, channel, gstin, month, run_id)
        
        if pivot_result.success:
            pivot_summary = pivot_agent.get_pivot_summary(pivot_df)
            print(f"✅ Pivot generation: {pivot_result.pivot_records} summary records from {pivot_result.processed_records} transactions")
            print(f"    💰 Total taxable: ₹{pivot_summary['total_taxable_amount']:,.2f}")
            print(f"    🏛️  Total tax: ₹{pivot_summary['total_tax_amount']:,.2f}")
            print(f"    📋 Unique ledgers: {pivot_summary['unique_ledgers']}, FGs: {pivot_summary['unique_fgs']}")
            
            # Save pivot data
            pivot_path = latest_file.replace('.csv', '_demo_pivot.csv')
            pivot_agent.export_pivot_csv(pivot_df, pivot_path)
            print(f"    💾 Pivot data saved: {os.path.basename(pivot_path)}")
            
            # Show sample pivot records
            print(f"\n📄 Sample Pivot Records:")
            sample_records = pivot_df[['gst_rate', 'ledger_name', 'fg', 'total_quantity', 'total_taxable', 'total_cgst', 'total_sgst', 'total_igst']].head(5)
            for i, row in sample_records.iterrows():
                tax_type = "Intrastate" if row['total_cgst'] > 0 else "Interstate" if row['total_igst'] > 0 else "Zero GST"
                total_tax = row['total_cgst'] + row['total_sgst'] + row['total_igst']
                print(f"    {row['gst_rate']*100:>2.0f}% | {row['ledger_name'][:20]:<20} | {row['fg'][:10]:<10} | Qty: {row['total_quantity']:>3.0f} | ₹{row['total_taxable']:>7.0f} → ₹{total_tax:>6.2f} ({tax_type})")
            
        else:
            print(f"❌ Pivot generation failed: {pivot_result.error_message}")
            return False
        
        # Batch Splitting
        batch_agent = BatchSplitterAgent(supabase)
        batch_files, batch_result = batch_agent.process_pivot_data(
            pivot_df, channel, gstin, month, run_id, "ingestion_layer/data/batches"
        )
        
        if batch_result.success:
            batch_summary = batch_agent.get_batch_summary(batch_result.batch_summaries)
            print(f"✅ Batch splitting: {batch_result.batch_files_created} files created")
            print(f"    📊 GST rates processed: {batch_result.gst_rates_processed}")
            print(f"    ✅ Validation: {'PASSED' if batch_result.validation_passed else 'FAILED'}")
            
            print(f"\n📋 Batch Files Created:")
            for file_path in batch_files:
                filename = os.path.basename(file_path)
                # Read file to get record count
                batch_df = pd.read_csv(file_path)
                gst_rate = batch_df['gst_rate'].iloc[0] if len(batch_df) > 0 else 0
                print(f"    {filename}: {len(batch_df)} records @ {gst_rate*100:.0f}% GST")
            
        else:
            print(f"❌ Batch splitting failed: {batch_result.error_message}")
            return False
        
        # Final Summary
        print(f"\n" + "=" * 50)
        print("🎉 DEMO COMPLETE!")
        print("=" * 50)
        
        print(f"📊 Processing Summary:")
        print(f"    Tax computations: {tax_result.successful_computations}/{tax_result.processed_records}")
        print(f"    Invoice numbers: {invoice_result.unique_invoice_numbers}")
        print(f"    Pivot records: {pivot_result.pivot_records}")
        print(f"    Batch files: {batch_result.batch_files_created}")
        
        print(f"\n💰 Financial Summary:")
        print(f"    Total taxable: ₹{tax_summary['total_taxable_amount']:,.2f}")
        print(f"    Total tax: ₹{tax_summary['total_tax']:,.2f}")
        print(f"    Intrastate: {tax_summary['intrastate_records']} transactions")
        print(f"    Interstate: {tax_summary['interstate_records']} transactions")
        
        print(f"\n📁 Output Files:")
        print(f"    Final: {os.path.basename(final_path)}")
        print(f"    Pivot: {os.path.basename(pivot_path)}")
        print(f"    Batches: {len(batch_files)} files in ingestion_layer/data/batches/")
        
        return True
        
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("🚀 COMPLETE PIPELINE DEMO")
    print("Demonstrating Parts 3 & 4 with existing normalized data")
    print("=" * 60)
    
    success = demo_parts_3_and_4()
    
    if success:
        print(f"\n🎉 Parts 3 & 4 working correctly!")
        print(f"🚀 The pipeline components are ready")
        print(f"\nTo run the complete pipeline:")
        print(f"1. Fix the Supabase storage bucket configuration")
        print(f"2. Run: python -m ingestion_layer.main --agent amazon_mtr --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 --full-pipeline")
    else:
        print(f"\n❌ Demo failed - check the errors above")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
