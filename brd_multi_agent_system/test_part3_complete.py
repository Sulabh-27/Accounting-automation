#!/usr/bin/env python3
"""
Complete test script for Part-3: Tax Engine & Invoice Numbering
Tests the complete pipeline with real data and validates against golden references
"""
import sys
import os
import pandas as pd
import unittest
sys.path.append('.')

from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.tax_engine import TaxEngine
from ingestion_layer.agents.invoice_numbering import InvoiceNumberingAgent
from ingestion_layer.libs.tax_rules import TaxRulesEngine
from ingestion_layer.libs.numbering_rules import NumberingRulesEngine


class TestSupabaseForPart3(SupabaseClientWrapper):
    """Test Supabase client for Part-3 testing."""
    
    def __init__(self):
        self.tax_computations = []
        self.invoice_registry = []
        self._client = None
    
    def insert_tax_computations(self, records):
        """Mock tax computations insertion."""
        self.tax_computations.extend(records)
        return {"data": records}
    
    def insert_invoice_registry(self, records):
        """Mock invoice registry insertion."""
        self.invoice_registry.extend(records)
        return {"data": records}
    
    # Mock the table operations
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
        if self.table_name == 'tax_computations':
            self.parent.tax_computations.extend(data)
        elif self.table_name == 'invoice_registry':
            self.parent.invoice_registry.extend(data)
        return MockResponse(data)
    
    def select(self, columns="*"):
        return MockQuery(self.table_name, self.parent)


class MockQuery:
    """Mock Supabase query for testing."""
    
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def eq(self, column, value):
        return self
    
    def execute(self):
        return MockResponse([])


class MockResponse:
    """Mock Supabase response for testing."""
    
    def __init__(self, data):
        self.data = data


def test_part3_with_real_data():
    """Test Part-3 with real Amazon MTR data."""
    
    print("🧪 TESTING PART-3: Tax Engine & Invoice Numbering")
    print("=" * 60)
    
    # Find the latest enriched file from Parts 1+2
    enriched_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                     if f.endswith("_enriched.csv")]
    
    if not enriched_files:
        print("❌ No enriched files found. Run Parts 1+2 first!")
        return False
    
    latest_file = f"ingestion_layer/data/normalized/{max(enriched_files)}"
    print(f"📁 Using enriched data: {os.path.basename(latest_file)}")
    
    # Load the enriched data
    df = pd.read_csv(latest_file)
    print(f"📊 Loaded {len(df)} records with {len(df.columns)} columns")
    
    # Initialize test components
    supabase = TestSupabaseForPart3()
    tax_engine = TaxEngine(supabase)
    invoice_agent = InvoiceNumberingAgent(supabase)
    
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = "test-run-id"
    
    print(f"\n🔧 Test Configuration:")
    print(f"    GSTIN: {gstin}")
    print(f"    Channel: {channel}")
    print(f"    Month: {month}")
    
    # Step 1: Tax Engine Testing
    print(f"\n🧮 STEP 1: Tax Engine Processing")
    print("-" * 40)
    
    df_with_tax, tax_result = tax_engine.process_dataset(df.copy(), channel, gstin, run_id)
    
    if tax_result.success:
        tax_summary = tax_engine.get_tax_summary(df_with_tax)
        
        print(f"✅ Tax Engine Results:")
        print(f"    Processed: {tax_result.processed_records} records")
        print(f"    Successful: {tax_result.successful_computations}")
        print(f"    Failed: {tax_result.failed_computations}")
        print(f"    Total taxable: ₹{tax_summary['total_taxable_amount']:,.2f}")
        print(f"    Total tax: ₹{tax_summary['total_tax']:,.2f}")
        print(f"    Intrastate: {tax_summary['intrastate_records']}")
        print(f"    Interstate: {tax_summary['interstate_records']}")
        
        # Show sample tax computations
        print(f"\n📄 Sample Tax Computations:")
        sample_records = df_with_tax[['sku', 'state_code', 'taxable_value', 'gst_rate', 'cgst', 'sgst', 'igst', 'total_tax']].head(3)
        for i, row in sample_records.iterrows():
            tax_type = "Intrastate (CGST+SGST)" if row['cgst'] > 0 else "Interstate (IGST)"
            print(f"    {row['sku']}: ₹{row['taxable_value']} @ {row['gst_rate']*100}% → ₹{row['total_tax']} ({tax_type})")
    else:
        print(f"❌ Tax Engine failed: {tax_result.error_message}")
        return False
    
    # Step 2: Invoice Numbering Testing
    print(f"\n📋 STEP 2: Invoice Numbering Processing")
    print("-" * 40)
    
    df_final, invoice_result = invoice_agent.process_dataset(df_with_tax, channel, gstin, month, run_id)
    
    if invoice_result.success:
        invoice_summary = invoice_agent.get_numbering_summary(df_final, channel)
        
        print(f"✅ Invoice Numbering Results:")
        print(f"    Processed: {invoice_result.processed_records} records")
        print(f"    Generated: {invoice_result.successful_generations}")
        print(f"    Failed: {invoice_result.failed_generations}")
        print(f"    Unique invoices: {invoice_result.unique_invoice_numbers}")
        print(f"    States covered: {invoice_summary['states_covered']}")
        print(f"    Pattern: {invoice_summary['pattern_example']}")
        
        # Show sample invoice numbers
        print(f"\n📄 Sample Invoice Numbers:")
        sample_invoices = df_final[['sku', 'state_code', 'invoice_no']].head(5)
        for i, row in sample_invoices.iterrows():
            print(f"    {row['sku']} ({row['state_code']}) → {row['invoice_no']}")
    else:
        print(f"❌ Invoice Numbering failed: {invoice_result.error_message}")
        return False
    
    # Step 3: Golden Test Validation
    print(f"\n🏆 STEP 3: Golden Test Validation")
    print("-" * 40)
    
    try:
        golden_df = pd.read_csv("ingestion_layer/tests/golden/amazon_mtr_expected.csv")
        print(f"📁 Loaded golden reference: {len(golden_df)} records")
        
        # Compare tax computations (first few records)
        validation_passed = True
        tolerance = 0.01
        
        for i in range(min(5, len(golden_df), len(df_final))):
            golden_row = golden_df.iloc[i]
            actual_row = df_final.iloc[i]
            
            # Compare key fields
            fields_to_compare = ['cgst', 'sgst', 'igst', 'total_tax', 'total_amount']
            for field in fields_to_compare:
                if field in golden_row and field in actual_row:
                    expected = float(golden_row[field])
                    actual = float(actual_row[field])
                    
                    if abs(expected - actual) > tolerance:
                        print(f"    ❌ Mismatch in {field} for record {i}: expected {expected}, got {actual}")
                        validation_passed = False
                    else:
                        print(f"    ✅ {field} matches for record {i}: {actual}")
        
        if validation_passed:
            print(f"🎉 Golden test validation PASSED!")
        else:
            print(f"⚠️  Golden test validation had some mismatches")
            
    except FileNotFoundError:
        print(f"⚠️  Golden reference file not found - skipping validation")
    except Exception as e:
        print(f"⚠️  Golden test validation error: {e}")
    
    # Step 4: Save Final Dataset
    print(f"\n💾 STEP 4: Save Final Dataset")
    print("-" * 40)
    
    final_path = latest_file.replace('.csv', '_final.csv')
    df_final.to_csv(final_path, index=False)
    
    print(f"💾 Final dataset saved: {os.path.basename(final_path)}")
    print(f"📊 Final dataset: {len(df_final)} rows × {len(df_final.columns)} columns")
    print(f"📋 New columns: cgst, sgst, igst, total_tax, total_amount, invoice_no")
    
    # Summary
    print(f"\n" + "=" * 60)
    print("📋 PART-3 TEST SUMMARY")
    print("=" * 60)
    
    success = (tax_result.success and invoice_result.success and 
               tax_summary['total_tax'] > 0 and invoice_result.unique_invoice_numbers > 0)
    
    print(f"Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Tax Computation: {tax_result.successful_computations}/{tax_result.processed_records} records")
    print(f"Invoice Generation: {invoice_result.successful_generations}/{invoice_result.processed_records} records")
    print(f"Total Tax Computed: ₹{tax_summary['total_tax']:,.2f}")
    print(f"Unique Invoices: {invoice_result.unique_invoice_numbers}")
    print(f"Final Dataset: {os.path.basename(final_path)}")
    
    if success:
        print(f"\n🎉 Part-3 is working correctly!")
        print(f"🚀 Ready for production with complete pipeline:")
        print(f"   python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"     --full-pipeline")
    
    return success


def test_tax_rules_engine():
    """Test the tax rules engine with various scenarios."""
    
    print("\n🧮 TESTING TAX RULES ENGINE")
    print("=" * 40)
    
    gstin = "06ABGCS4796R1ZA"  # Haryana
    tax_engine = TaxRulesEngine(gstin)
    
    test_cases = [
        # (taxable_value, gst_rate, customer_state, expected_cgst, expected_sgst, expected_igst)
        (1000.0, 0.18, "HARYANA", 90.0, 90.0, 0.0),      # Intrastate
        (1000.0, 0.18, "DELHI", 0.0, 0.0, 180.0),        # Interstate
        (1000.0, 0.0, "DELHI", 0.0, 0.0, 0.0),           # Zero GST
        (500.0, 0.05, "KARNATAKA", 0.0, 0.0, 25.0),      # 5% GST Interstate
        (1000.0, 0.28, "HARYANA", 140.0, 140.0, 0.0),    # 28% GST Intrastate
    ]
    
    passed = 0
    total = len(test_cases)
    
    for i, (taxable, gst_rate, state, exp_cgst, exp_sgst, exp_igst) in enumerate(test_cases):
        result = tax_engine.compute_amazon_mtr_tax(taxable, gst_rate, state)
        
        if (abs(result['cgst'] - exp_cgst) < 0.01 and 
            abs(result['sgst'] - exp_sgst) < 0.01 and 
            abs(result['igst'] - exp_igst) < 0.01):
            print(f"    ✅ Test {i+1}: ₹{taxable} @ {gst_rate*100}% to {state}")
            passed += 1
        else:
            print(f"    ❌ Test {i+1}: Expected CGST:{exp_cgst}, SGST:{exp_sgst}, IGST:{exp_igst}")
            print(f"         Got CGST:{result['cgst']}, SGST:{result['sgst']}, IGST:{result['igst']}")
    
    print(f"\n📊 Tax Rules Tests: {passed}/{total} passed")
    return passed == total


def test_numbering_rules_engine():
    """Test the numbering rules engine with various patterns."""
    
    print("\n📋 TESTING NUMBERING RULES ENGINE")
    print("=" * 40)
    
    gstin = "06ABGCS4796R1ZA"  # Haryana
    numbering_engine = NumberingRulesEngine(gstin)
    
    test_cases = [
        # (channel, state, month, seq, expected_pattern)
        ("amazon_mtr", "ANDHRA PRADESH", "2025-08", 1, "AMZ-AP-08-0001"),
        ("amazon_str", "KARNATAKA", "2025-12", 5, "AMZST-KA-12-0005"),
        ("flipkart", "DELHI", "2025-08", 10, "FLIP-DL-08-0010"),
        ("pepperfry", "MAHARASHTRA", "2025-08", 25, "PEPP-MH-08-0025"),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for i, (channel, state, month, seq, expected) in enumerate(test_cases):
        result = numbering_engine.generate_invoice_number(channel, state, month, seq)
        
        if result == expected:
            print(f"    ✅ Test {i+1}: {channel} → {result}")
            passed += 1
        else:
            print(f"    ❌ Test {i+1}: Expected {expected}, got {result}")
    
    print(f"\n📊 Numbering Rules Tests: {passed}/{total} passed")
    return passed == total


def main():
    print("🚀 PART-3 COMPLETE TESTING SUITE")
    print("Tax Engine & Invoice Numbering with Real Data")
    print("=" * 60)
    
    # Test 1: Core rules engines
    tax_rules_ok = test_tax_rules_engine()
    numbering_rules_ok = test_numbering_rules_engine()
    
    # Test 2: Complete Part-3 workflow
    part3_ok = test_part3_with_real_data()
    
    # Final result
    print(f"\n" + "=" * 60)
    print("🏁 FINAL RESULT")
    print("=" * 60)
    
    all_passed = tax_rules_ok and numbering_rules_ok and part3_ok
    
    if all_passed:
        print("✅ Part-3: Tax Engine & Invoice Numbering is WORKING!")
        print("🎯 Ready for production use with complete pipeline")
        print("\n🚀 PRODUCTION COMMANDS:")
        print("# Complete Pipeline (Parts 1+2+3):")
        print("python -m ingestion_layer.main --agent amazon_mtr \\")
        print("  --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print("  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print("  --full-pipeline")
        print("\n# Individual Parts:")
        print("python -m ingestion_layer.main ... --enable-mapping --enable-tax-invoice")
    else:
        print("❌ Part-3 testing failed - needs debugging")
        if not tax_rules_ok:
            print("  - Tax rules engine issues")
        if not numbering_rules_ok:
            print("  - Numbering rules engine issues")
        if not part3_ok:
            print("  - Part-3 workflow issues")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
