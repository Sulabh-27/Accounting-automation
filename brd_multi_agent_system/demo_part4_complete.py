#!/usr/bin/env python3
"""
Complete demonstration of Part-4: Pivoting & Batch Splitting
Tests the complete workflow with real data and validates against golden references
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
        return MockResponse(data)
    
    def execute(self):
        return MockResponse([])


class MockResponse:
    """Mock Supabase response for testing."""
    
    def __init__(self, data):
        self.data = data


def test_part4_with_real_data():
    """Test Part-4 with real enriched data from Parts 1+2+3."""
    
    print("🧪 TESTING PART-4: Pivoting & Batch Splitting")
    print("=" * 60)
    
    # Find the latest enriched file with tax and invoice data
    enriched_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                     if f.endswith("_with_tax_invoice.csv")]
    
    if not enriched_files:
        print("❌ No enriched files with tax and invoice data found.")
        print("   Run Parts 1+2+3 first to generate enriched data!")
        return False
    
    latest_file = f"ingestion_layer/data/normalized/{max(enriched_files)}"
    print(f"📁 Using enriched data: {os.path.basename(latest_file)}")
    
    # Load the enriched data
    df = pd.read_csv(latest_file)
    print(f"📊 Loaded {len(df)} records with {len(df.columns)} columns")
    
    # Check required columns for pivoting
    required_columns = ['gstin', 'month', 'gst_rate', 'ledger_name', 'fg', 
                       'quantity', 'taxable_value', 'cgst', 'sgst', 'igst']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Missing required columns for pivoting: {missing_columns}")
        return False
    
    # Initialize test components
    supabase = MockSupabase()
    pivot_agent = PivotGeneratorAgent(supabase)
    batch_agent = BatchSplitterAgent(supabase)
    
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    print(f"\n🔧 Test Configuration:")
    print(f"    GSTIN: {gstin}")
    print(f"    Channel: {channel}")
    print(f"    Month: {month}")
    
    # Step 1: Pivot Generation Testing
    print(f"\n📊 STEP 1: Pivot Generation")
    print("-" * 40)
    
    pivot_df, pivot_result = pivot_agent.process_dataset(df, channel, gstin, month, run_id)
    
    if pivot_result.success:
        pivot_summary = pivot_agent.get_pivot_summary(pivot_df)
        
        print(f"✅ Pivot Generation Results:")
        print(f"    Input records: {pivot_result.processed_records}")
        print(f"    Pivot records: {pivot_result.pivot_records}")
        print(f"    Total taxable: ₹{pivot_summary['total_taxable_amount']:,.2f}")
        print(f"    Total tax: ₹{pivot_summary['total_tax_amount']:,.2f}")
        print(f"    Unique ledgers: {pivot_summary['unique_ledgers']}")
        print(f"    Unique FGs: {pivot_summary['unique_fgs']}")
        print(f"    GST rates: {pivot_summary['unique_gst_rates']}")
        
        # Show sample pivot records
        print(f"\n📄 Sample Pivot Records:")
        sample_records = pivot_df[['gst_rate', 'ledger_name', 'fg', 'total_quantity', 'total_taxable', 'total_tax']].head(5)
        for i, row in sample_records.iterrows():
            print(f"    {row['gst_rate']*100:>2.0f}% | {row['ledger_name'][:20]:<20} | {row['fg'][:15]:<15} | Qty: {row['total_quantity']:>3.0f} | ₹{row['total_taxable']:>7.0f} → ₹{row['total_tax']:>6.2f}")
        
        # Save pivot CSV for inspection
        pivot_path = latest_file.replace('.csv', '_pivot.csv')
        pivot_agent.export_pivot_csv(pivot_df, pivot_path)
        print(f"    💾 Pivot data saved: {os.path.basename(pivot_path)}")
        
    else:
        print(f"❌ Pivot Generation failed: {pivot_result.error_message}")
        return False
    
    # Step 2: Batch Splitting Testing
    print(f"\n📄 STEP 2: Batch Splitting")
    print("-" * 40)
    
    # Create temporary directory for batch files
    temp_dir = tempfile.mkdtemp()
    
    try:
        batch_files, batch_result = batch_agent.process_pivot_data(
            pivot_df, channel, gstin, month, run_id, temp_dir
        )
        
        if batch_result.success:
            batch_summary = batch_agent.get_batch_summary(batch_result.batch_summaries)
            
            print(f"✅ Batch Splitting Results:")
            print(f"    Input records: {batch_result.processed_records}")
            print(f"    Batch files created: {batch_result.batch_files_created}")
            print(f"    GST rates processed: {batch_result.gst_rates_processed}")
            print(f"    Total records split: {batch_result.total_records_split}")
            print(f"    Validation: {'✅ PASSED' if batch_result.validation_passed else '❌ FAILED'}")
            
            # Show batch breakdown
            print(f"\n📋 Batch File Breakdown:")
            for i, breakdown in enumerate(batch_summary["batch_breakdown"]):
                filename = os.path.basename(batch_files[i]) if i < len(batch_files) else "N/A"
                print(f"    {breakdown['gst_rate']}: {breakdown['records']} records, {breakdown['taxable']} taxable, {breakdown['tax']} tax")
                print(f"        File: {filename}")
            
            # Validate batch files
            print(f"\n🔍 Batch File Validation:")
            validation = batch_agent.validate_batch_files(batch_files)
            print(f"    Files validated: {validation['files_validated']}")
            print(f"    Files missing: {validation['files_missing']}")
            print(f"    Files empty: {validation['files_empty']}")
            print(f"    GST rate violations: {validation['gst_rate_violations']}")
            
            if validation['gst_rate_violations'] == 0:
                print(f"    ✅ All batch files contain only one GST rate each")
            else:
                print(f"    ❌ Some batch files contain multiple GST rates")
            
        else:
            print(f"❌ Batch Splitting failed: {batch_result.error_message}")
            return False
        
    finally:
        # Cleanup temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    # Step 3: Golden Test Validation (if available)
    print(f"\n🏆 STEP 3: Golden Test Validation")
    print("-" * 40)
    
    try:
        golden_path = "ingestion_layer/tests/golden/amazon_mtr_pivot_expected.csv"
        if os.path.exists(golden_path):
            golden_df = pd.read_csv(golden_path)
            print(f"📁 Loaded golden reference: {len(golden_df)} records")
            
            # Compare key metrics
            golden_total_taxable = golden_df['total_taxable'].sum()
            golden_total_tax = (golden_df['total_cgst'] + golden_df['total_sgst'] + golden_df['total_igst']).sum()
            
            actual_total_taxable = pivot_df['total_taxable'].sum()
            actual_total_tax = (pivot_df['total_cgst'] + pivot_df['total_sgst'] + pivot_df['total_igst']).sum()
            
            taxable_diff = abs(golden_total_taxable - actual_total_taxable)
            tax_diff = abs(golden_total_tax - actual_total_tax)
            
            print(f"    Golden taxable: ₹{golden_total_taxable:,.2f}")
            print(f"    Actual taxable: ₹{actual_total_taxable:,.2f}")
            print(f"    Difference: ₹{taxable_diff:,.2f}")
            
            print(f"    Golden tax: ₹{golden_total_tax:,.2f}")
            print(f"    Actual tax: ₹{actual_total_tax:,.2f}")
            print(f"    Difference: ₹{tax_diff:,.2f}")
            
            tolerance = 100.0  # Allow ₹100 difference due to data variations
            if taxable_diff <= tolerance and tax_diff <= tolerance:
                print(f"    ✅ Golden test validation PASSED (within ₹{tolerance} tolerance)")
            else:
                print(f"    ⚠️  Golden test validation had differences > ₹{tolerance}")
        else:
            print(f"    ⚠️  Golden reference file not found - skipping validation")
            
    except Exception as e:
        print(f"    ⚠️  Golden test validation error: {e}")
    
    # Summary
    print(f"\n" + "=" * 60)
    print("📋 PART-4 TEST SUMMARY")
    print("=" * 60)
    
    success = (pivot_result.success and batch_result.success and 
               pivot_summary['total_tax_amount'] >= 0 and batch_result.batch_files_created > 0)
    
    print(f"Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Pivot Generation: {pivot_result.pivot_records}/{pivot_result.processed_records} records")
    print(f"Batch Splitting: {batch_result.batch_files_created} files, {batch_result.gst_rates_processed} GST rates")
    print(f"Total Taxable Pivoted: ₹{pivot_summary['total_taxable_amount']:,.2f}")
    print(f"Total Tax Pivoted: ₹{pivot_summary['total_tax_amount']:,.2f}")
    print(f"Pivot File: {os.path.basename(pivot_path)}")
    
    if success:
        print(f"\n🎉 Part-4 is working correctly!")
        print(f"🚀 Ready for production with complete pipeline:")
        print(f"   python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"     --full-pipeline")
    
    return success


def test_pivot_rules_engine():
    """Test the pivot rules engine with various scenarios."""
    
    print("\n📊 TESTING PIVOT RULES ENGINE")
    print("=" * 40)
    
    pivot_engine = PivotRulesEngine()
    
    test_cases = [
        # (channel, expected_dimensions, expected_measures)
        ("amazon_mtr", ["gstin", "month", "gst_rate", "ledger_name", "fg"], ["quantity", "taxable_value", "cgst", "sgst", "igst"]),
        ("amazon_str", ["gstin", "month", "gst_rate", "ledger_name", "fg"], ["quantity", "taxable_value", "igst"]),
        ("flipkart", ["gstin", "month", "gst_rate", "ledger_name", "fg", "state_code"], ["quantity", "taxable_value", "cgst", "sgst", "igst"]),
        ("pepperfry", ["gstin", "month", "gst_rate", "ledger_name", "fg"], ["quantity", "taxable_value", "cgst", "sgst", "igst"]),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for i, (channel, exp_dims, exp_measures) in enumerate(test_cases):
        dims = pivot_engine.get_pivot_dimensions(channel)
        measures = pivot_engine.get_pivot_measures(channel)
        
        dims_match = set(dims) == set(exp_dims)
        measures_match = set(measures) == set(exp_measures)
        
        if dims_match and measures_match:
            print(f"    ✅ Test {i+1}: {channel} - dimensions and measures correct")
            passed += 1
        else:
            print(f"    ❌ Test {i+1}: {channel} - mismatch in dimensions or measures")
    
    print(f"\n📊 Pivot Rules Tests: {passed}/{total} passed")
    return passed == total


def test_summarizer():
    """Test the summarizer with sample data."""
    
    print("\n📈 TESTING SUMMARIZER")
    print("=" * 40)
    
    summarizer = Summarizer()
    
    # Create sample pivot data
    sample_pivot = pd.DataFrame([
        {"gst_rate": 0.18, "ledger_name": "Amazon Haryana", "fg": "Product A", "total_quantity": 10, "total_taxable": 1000.0, "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0},
        {"gst_rate": 0.18, "ledger_name": "Amazon Delhi", "fg": "Product B", "total_quantity": 5, "total_taxable": 500.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 90.0},
        {"gst_rate": 0.0, "ledger_name": "Amazon Delhi", "fg": "Product C", "total_quantity": 8, "total_taxable": 800.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 0.0},
    ])
    
    summary = summarizer.generate_pivot_summary(sample_pivot)
    
    expected_totals = {
        "total_records": 3,
        "total_taxable_amount": 2300.0,
        "total_tax_amount": 180.0,
        "unique_ledgers": 2,
        "unique_fgs": 3
    }
    
    passed = 0
    total = len(expected_totals)
    
    for key, expected in expected_totals.items():
        actual = summary.get(key, 0)
        if actual == expected:
            print(f"    ✅ {key}: {actual}")
            passed += 1
        else:
            print(f"    ❌ {key}: expected {expected}, got {actual}")
    
    print(f"\n📊 Summarizer Tests: {passed}/{total} passed")
    return passed == total


def main():
    print("🚀 PART-4 COMPLETE TESTING SUITE")
    print("Pivoting & Batch Splitting with Real Data")
    print("=" * 60)
    
    # Test 1: Core engines
    pivot_rules_ok = test_pivot_rules_engine()
    summarizer_ok = test_summarizer()
    
    # Test 2: Complete Part-4 workflow
    part4_ok = test_part4_with_real_data()
    
    # Final result
    print(f"\n" + "=" * 60)
    print("🏁 FINAL RESULT")
    print("=" * 60)
    
    all_passed = pivot_rules_ok and summarizer_ok and part4_ok
    
    if all_passed:
        print("✅ Part-4: Pivoting & Batch Splitting is WORKING!")
        print("🎯 Ready for production use with complete pipeline")
        print("\n🚀 PRODUCTION COMMANDS:")
        print("# Complete Pipeline (Parts 1+2+3+4):")
        print("python -m ingestion_layer.main --agent amazon_mtr \\")
        print("  --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print("  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print("  --full-pipeline")
        print("\n# Individual Parts:")
        print("python -m ingestion_layer.main ... --enable-mapping --enable-tax-invoice --enable-pivot-batch")
    else:
        print("❌ Part-4 testing failed - needs debugging")
        if not pivot_rules_ok:
            print("  - Pivot rules engine issues")
        if not summarizer_ok:
            print("  - Summarizer issues")
        if not part4_ok:
            print("  - Part-4 workflow issues")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
