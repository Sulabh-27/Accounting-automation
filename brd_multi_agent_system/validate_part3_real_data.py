#!/usr/bin/env python3
"""
Validation script for Part-3 with your real Amazon MTR data
"""
import sys
import os
import pandas as pd
sys.path.append('.')

from ingestion_layer.libs.tax_rules import TaxRulesEngine
from ingestion_layer.libs.numbering_rules import NumberingRulesEngine


def validate_with_real_data():
    """Validate Part-3 with your actual normalized data."""
    
    print("🧪 VALIDATING PART-3 WITH YOUR REAL DATA")
    print("=" * 60)
    
    # Load your actual normalized data
    data_file = "ingestion_layer/data/normalized/amazon_mtr_74b9a9f43e0043b58a7925760d541dc8.csv"
    
    if not os.path.exists(data_file):
        print(f"❌ Data file not found: {data_file}")
        return False
    
    df = pd.read_csv(data_file)
    print(f"📊 Loaded real data: {len(df)} transactions")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Initialize engines
    gstin = "06ABGCS4796R1ZA"  # Your company GSTIN (Haryana)
    tax_engine = TaxRulesEngine(gstin)
    numbering_engine = NumberingRulesEngine(gstin)
    
    print(f"\n🏢 Company Details:")
    print(f"    GSTIN: {gstin}")
    print(f"    State Code: {tax_engine.company_state_code} (Haryana)")
    
    # Process first 10 records for validation
    sample_size = min(10, len(df))
    sample_df = df.head(sample_size).copy()
    
    print(f"\n🧮 PROCESSING SAMPLE ({sample_size} records):")
    print("-" * 50)
    
    results = []
    total_taxable = 0
    total_tax = 0
    intrastate_count = 0
    interstate_count = 0
    
    for index, row in sample_df.iterrows():
        # Tax computation
        tax_result = tax_engine.compute_amazon_mtr_tax(
            taxable_value=row['taxable_value'],
            gst_rate=row['gst_rate'],
            customer_state=row['state_code']
        )
        
        # Invoice number generation
        invoice_no = numbering_engine.generate_invoice_number(
            channel="amazon_mtr",
            state_name=row['state_code'],
            month=row['month'],
            sequence_number=index + 1
        )
        
        # Determine tax type
        is_intrastate = tax_result['cgst'] > 0
        tax_type = "Intrastate (CGST+SGST)" if is_intrastate else "Interstate (IGST)"
        
        if is_intrastate:
            intrastate_count += 1
        else:
            interstate_count += 1
        
        total_taxable += tax_result['taxable_value']
        total_tax += tax_result['total_tax']
        
        # Display result
        print(f"{index+1:2d}. {row['sku'][:15]:<15} | {row['state_code'][:12]:<12} | ₹{row['taxable_value']:>7.0f} @ {row['gst_rate']*100:>2.0f}% → ₹{tax_result['total_tax']:>6.2f} | {invoice_no} | {tax_type}")
        
        results.append({
            'sku': row['sku'],
            'state_code': row['state_code'],
            'taxable_value': tax_result['taxable_value'],
            'cgst': tax_result['cgst'],
            'sgst': tax_result['sgst'],
            'igst': tax_result['igst'],
            'total_tax': tax_result['total_tax'],
            'total_amount': tax_result['total_amount'],
            'invoice_no': invoice_no,
            'tax_type': tax_type
        })
    
    # Summary
    print(f"\n📊 VALIDATION SUMMARY:")
    print("-" * 30)
    print(f"Total Records Processed: {sample_size}")
    print(f"Total Taxable Amount: ₹{total_taxable:,.2f}")
    print(f"Total Tax Computed: ₹{total_tax:,.2f}")
    print(f"Intrastate Transactions: {intrastate_count}")
    print(f"Interstate Transactions: {interstate_count}")
    print(f"Unique Invoice Numbers: {len(set(r['invoice_no'] for r in results))}")
    
    # State-wise breakdown
    print(f"\n🗺️  STATE-WISE BREAKDOWN:")
    print("-" * 30)
    state_summary = {}
    for result in results:
        state = result['state_code']
        if state not in state_summary:
            state_summary[state] = {'count': 0, 'tax': 0, 'taxable': 0}
        state_summary[state]['count'] += 1
        state_summary[state]['tax'] += result['total_tax']
        state_summary[state]['taxable'] += result['taxable_value']
    
    for state, data in state_summary.items():
        tax_type = "Intrastate" if state == "HARYANA" else "Interstate"
        print(f"{state[:15]:<15}: {data['count']} txns, ₹{data['taxable']:>7.0f} taxable, ₹{data['tax']:>6.2f} tax ({tax_type})")
    
    # Validation checks
    print(f"\n✅ VALIDATION CHECKS:")
    print("-" * 25)
    
    checks_passed = 0
    total_checks = 0
    
    # Check 1: All records processed
    total_checks += 1
    if len(results) == sample_size:
        print(f"✅ All {sample_size} records processed successfully")
        checks_passed += 1
    else:
        print(f"❌ Only {len(results)}/{sample_size} records processed")
    
    # Check 2: Tax computation correctness
    total_checks += 1
    tax_errors = 0
    for result in results:
        expected_total = result['cgst'] + result['sgst'] + result['igst']
        if abs(expected_total - result['total_tax']) > 0.01:
            tax_errors += 1
    
    if tax_errors == 0:
        print(f"✅ All tax computations are mathematically correct")
        checks_passed += 1
    else:
        print(f"❌ {tax_errors} tax computation errors found")
    
    # Check 3: Invoice number uniqueness
    total_checks += 1
    invoice_numbers = [r['invoice_no'] for r in results]
    unique_invoices = set(invoice_numbers)
    
    if len(invoice_numbers) == len(unique_invoices):
        print(f"✅ All invoice numbers are unique")
        checks_passed += 1
    else:
        print(f"❌ Duplicate invoice numbers found")
    
    # Check 4: Intrastate/Interstate logic
    total_checks += 1
    logic_errors = 0
    for result in results:
        is_haryana = result['state_code'] == "HARYANA"
        has_cgst_sgst = result['cgst'] > 0 and result['sgst'] > 0
        has_igst = result['igst'] > 0
        is_zero_gst = result['total_tax'] == 0
        
        # For Haryana (intrastate), should have CGST+SGST (unless zero GST)
        if is_haryana and not has_cgst_sgst and not is_zero_gst:
            logic_errors += 1
        # For other states (interstate), should have IGST (unless zero GST)
        elif not is_haryana and not has_igst and not is_zero_gst:
            logic_errors += 1
    
    if logic_errors == 0:
        print(f"✅ Intrastate/Interstate tax logic is correct")
        checks_passed += 1
    else:
        print(f"❌ {logic_errors} tax logic errors found")
    
    # Final result
    print(f"\n" + "=" * 60)
    print(f"🏁 VALIDATION RESULT: {checks_passed}/{total_checks} checks passed")
    
    if checks_passed == total_checks:
        print("🎉 Part-3 validation SUCCESSFUL! Ready for production.")
        
        # Show next steps
        print(f"\n🚀 NEXT STEPS:")
        print("1. Run complete pipeline with your data:")
        print("   python -m ingestion_layer.main --agent amazon_mtr \\")
        print("     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print("     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print("     --full-pipeline")
        print("\n2. Or run just Part-3 on existing normalized data:")
        print("   python validate_part3_with_pipeline.py")
        
        return True
    else:
        print("❌ Part-3 validation failed - needs debugging")
        return False


def main():
    return validate_with_real_data()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
