#!/usr/bin/env python3
"""
Simple validation of Part-4 with mock data
"""
import sys
import os
import pandas as pd
import uuid
import tempfile
import shutil
sys.path.append('.')

from ingestion_layer.agents.pivoter import PivotGeneratorAgent
from ingestion_layer.agents.batch_splitter import BatchSplitterAgent
from ingestion_layer.libs.pivot_rules import PivotRulesEngine
from ingestion_layer.libs.summarizer import Summarizer


class MockSupabase:
    """Mock Supabase client for testing."""
    
    def __init__(self):
        self.pivot_summaries = []
        self.batch_registry = []
    
    @property
    def client(self):
        return self
    
    def table(self, table_name):
        return MockTable(table_name, self)


class MockTable:
    """Mock Supabase table for testing."""
    
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def insert(self, data):
        if self.table_name == 'pivot_summaries':
            self.parent.pivot_summaries.extend(data)
        elif self.table_name == 'batch_registry':
            self.parent.batch_registry.extend(data)
        return MockInsertResponse(data)
    
    def execute(self):
        return MockResponse([])


class MockResponse:
    """Mock Supabase response for testing."""
    
    def __init__(self, data):
        self.data = data


class MockInsertResponse:
    """Mock Supabase insert response for testing."""
    
    def __init__(self, data):
        self.data = data
    
    def execute(self):
        return MockResponse(self.data)


def test_part4_with_mock_data():
    """Test Part-4 with mock enriched data."""
    
    print("🧪 TESTING PART-4 WITH MOCK DATA")
    print("=" * 50)
    
    # Create mock enriched data (what would come from Parts 1+2+3)
    mock_data = pd.DataFrame([
        # 18% GST items - Haryana (intrastate)
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.18, "ledger_name": "Amazon Haryana", "fg": "FABCON-5L", "quantity": 2, "taxable_value": 2118.0, "cgst": 190.62, "sgst": 190.62, "igst": 0.0, "invoice_no": "AMZ-HR-08-0001"},
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.18, "ledger_name": "Amazon Haryana", "fg": "TOILETCLEANER", "quantity": 1, "taxable_value": 215.0, "cgst": 19.35, "sgst": 19.35, "igst": 0.0, "invoice_no": "AMZ-HR-08-0002"},
        
        # 18% GST items - Delhi (interstate)
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.18, "ledger_name": "Amazon Delhi", "fg": "FABCON-5L", "quantity": 1, "taxable_value": 1059.0, "cgst": 0.0, "sgst": 0.0, "igst": 190.62, "invoice_no": "AMZ-DL-08-0001"},
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.18, "ledger_name": "Amazon Delhi", "fg": "LLQ-LAV-3L-FBA", "quantity": 1, "taxable_value": 449.0, "cgst": 0.0, "sgst": 0.0, "igst": 80.82, "invoice_no": "AMZ-DL-08-0002"},
        
        # 0% GST items
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.0, "ledger_name": "Amazon Delhi", "fg": "FABCON-5L", "quantity": 4, "taxable_value": 4236.0, "cgst": 0.0, "sgst": 0.0, "igst": 0.0, "invoice_no": "AMZ-DL-08-0003"},
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.0, "ledger_name": "Amazon Delhi", "fg": "90-X8YV-Q3DM", "quantity": 1, "taxable_value": 449.0, "cgst": 0.0, "sgst": 0.0, "igst": 0.0, "invoice_no": "AMZ-DL-08-0004"},
        
        # 12% GST items (hypothetical)
        {"gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.12, "ledger_name": "Amazon Karnataka", "fg": "ESSENTIAL-ITEM", "quantity": 5, "taxable_value": 1000.0, "cgst": 0.0, "sgst": 0.0, "igst": 120.0, "invoice_no": "AMZ-KA-08-0001"},
    ])
    
    print(f"📊 Created mock data: {len(mock_data)} records")
    print(f"📋 Columns: {list(mock_data.columns)}")
    
    # Initialize components
    supabase = MockSupabase()
    pivot_agent = PivotGeneratorAgent(supabase)
    batch_agent = BatchSplitterAgent(supabase)
    
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    # Step 1: Test Pivot Generation
    print(f"\n📊 STEP 1: Pivot Generation")
    print("-" * 30)
    
    pivot_df, pivot_result = pivot_agent.process_dataset(mock_data, channel, gstin, month, run_id)
    
    if pivot_result.success:
        pivot_summary = pivot_agent.get_pivot_summary(pivot_df)
        
        print(f"✅ Pivot Results:")
        print(f"    Input → Pivot: {pivot_result.processed_records} → {pivot_result.pivot_records}")
        print(f"    Total taxable: ₹{pivot_summary['total_taxable_amount']:,.2f}")
        print(f"    Total tax: ₹{pivot_summary['total_tax_amount']:,.2f}")
        print(f"    Unique ledgers: {pivot_summary['unique_ledgers']}")
        print(f"    Unique FGs: {pivot_summary['unique_fgs']}")
        
        print(f"\n📄 Pivot Records:")
        for i, row in pivot_df.iterrows():
            tax_type = "Intrastate" if row['total_cgst'] > 0 else "Interstate" if row['total_igst'] > 0 else "Zero GST"
            print(f"    {row['gst_rate']*100:>2.0f}% | {row['ledger_name'][:20]:<20} | {row['fg'][:15]:<15} | Qty: {row['total_quantity']:>2.0f} | ₹{row['total_taxable']:>6.0f} → ₹{row['total_cgst'] + row['total_sgst'] + row['total_igst']:>6.2f} ({tax_type})")
        
    else:
        print(f"❌ Pivot failed: {pivot_result.error_message}")
        return False
    
    # Step 2: Test Batch Splitting
    print(f"\n📄 STEP 2: Batch Splitting")
    print("-" * 30)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        batch_files, batch_result = batch_agent.process_pivot_data(
            pivot_df, channel, gstin, month, run_id, temp_dir
        )
        
        if batch_result.success:
            batch_summary = batch_agent.get_batch_summary(batch_result.batch_summaries)
            
            print(f"✅ Batch Results:")
            print(f"    Files created: {batch_result.batch_files_created}")
            print(f"    GST rates: {batch_result.gst_rates_processed}")
            print(f"    Records split: {batch_result.total_records_split}")
            print(f"    Validation: {'✅ PASSED' if batch_result.validation_passed else '❌ FAILED'}")
            
            print(f"\n📋 Batch Files:")
            for i, file_path in enumerate(batch_files):
                filename = os.path.basename(file_path)
                breakdown = batch_summary["batch_breakdown"][i]
                print(f"    {filename}")
                print(f"      {breakdown['gst_rate']}: {breakdown['records']} records, {breakdown['taxable']} taxable, {breakdown['tax']} tax")
                
                # Validate file content
                df = pd.read_csv(file_path)
                unique_rates = df['gst_rate'].nunique()
                print(f"      Validation: {'✅' if unique_rates == 1 else '❌'} (GST rates: {unique_rates})")
            
        else:
            print(f"❌ Batch splitting failed: {batch_result.error_message}")
            return False
        
    finally:
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    # Summary
    print(f"\n" + "=" * 50)
    print("📋 PART-4 VALIDATION SUMMARY")
    print("=" * 50)
    
    success = pivot_result.success and batch_result.success
    
    print(f"Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Pivot Generation: {'✅' if pivot_result.success else '❌'}")
    print(f"Batch Splitting: {'✅' if batch_result.success else '❌'}")
    print(f"Total Processing: {pivot_result.processed_records} → {pivot_result.pivot_records} → {batch_result.batch_files_created} files")
    
    if success:
        print(f"\n🎉 Part-4 core functionality is working!")
        print(f"🚀 Ready for integration with complete pipeline")
    
    return success


def test_core_components():
    """Test core Part-4 components."""
    
    print("🔧 TESTING CORE COMPONENTS")
    print("=" * 30)
    
    # Test 1: Pivot Rules Engine
    pivot_engine = PivotRulesEngine()
    
    amazon_dims = pivot_engine.get_pivot_dimensions("amazon_mtr")
    amazon_measures = pivot_engine.get_pivot_measures("amazon_mtr")
    
    expected_dims = ["gstin", "month", "gst_rate", "ledger_name", "fg"]
    expected_measures = ["quantity", "taxable_value", "cgst", "sgst", "igst"]
    
    dims_ok = set(amazon_dims) == set(expected_dims)
    measures_ok = set(amazon_measures) == set(expected_measures)
    
    print(f"Pivot Rules Engine: {'✅' if dims_ok and measures_ok else '❌'}")
    
    # Test 2: Summarizer
    summarizer = Summarizer()
    
    test_df = pd.DataFrame([
        {"total_taxable": 1000.0, "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0, "ledger_name": "Amazon A", "fg": "Product A", "gst_rate": 0.18},
        {"total_taxable": 500.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 90.0, "ledger_name": "Amazon B", "fg": "Product B", "gst_rate": 0.18}
    ])
    
    summary = summarizer.generate_pivot_summary(test_df)
    
    expected_total_tax = 270.0  # 90 + 90 + 90
    actual_total_tax = summary.get("total_tax_amount", 0)
    
    summarizer_ok = abs(expected_total_tax - actual_total_tax) < 0.01
    
    print(f"Summarizer: {'✅' if summarizer_ok else '❌'}")
    
    return dims_ok and measures_ok and summarizer_ok


def main():
    print("🚀 PART-4 SIMPLE VALIDATION")
    print("Pivoting & Batch Splitting Core Functionality")
    print("=" * 60)
    
    # Test core components
    components_ok = test_core_components()
    
    # Test with mock data
    workflow_ok = test_part4_with_mock_data()
    
    # Final result
    print(f"\n" + "=" * 60)
    print("🏁 VALIDATION RESULT")
    print("=" * 60)
    
    all_ok = components_ok and workflow_ok
    
    print(f"Status: {'✅ SUCCESS' if all_ok else '❌ FAILED'}")
    print(f"Core Components: {'✅' if components_ok else '❌'}")
    print(f"Workflow: {'✅' if workflow_ok else '❌'}")
    
    if all_ok:
        print(f"\n🎉 Part-4 is working correctly!")
        print(f"🚀 Ready for production integration")
        print(f"\nNext steps:")
        print(f"1. Run complete pipeline: --full-pipeline")
        print(f"2. Set up Supabase schema: ingestion_layer/sql/part4_schema.sql")
        print(f"3. Test with real enriched data from Parts 1+2+3")
    
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
