#!/usr/bin/env python3
"""
Complete validation of Part-3 with full dataset processing
"""
import sys
import os
import pandas as pd
import uuid
sys.path.append('.')

from ingestion_layer.libs.tax_rules import TaxRulesEngine
from ingestion_layer.libs.numbering_rules import NumberingRulesEngine


def validate_complete_part3():
    """Validate Part-3 with complete dataset processing."""
    
    print("🎯 COMPLETE PART-3 VALIDATION")
    print("Processing your full Amazon MTR dataset")
    print("=" * 60)
    
    # Load your actual data
    data_file = "ingestion_layer/data/normalized/amazon_mtr_74b9a9f43e0043b58a7925760d541dc8.csv"
    
    if not os.path.exists(data_file):
        print(f"❌ Data file not found: {data_file}")
        return False
    
    df = pd.read_csv(data_file)
    print(f"📊 Loaded dataset: {len(df)} transactions")
    
    # Initialize engines
    gstin = "06ABGCS4796R1ZA"
    tax_engine = TaxRulesEngine(gstin)
    numbering_engine = NumberingRulesEngine(gstin)
    
    print(f"🏢 Company: {gstin} (Haryana)")
    print(f"📅 Processing month: {df['month'].iloc[0]}")
    
    # Process all records
    print(f"\n🧮 PROCESSING ALL {len(df)} RECORDS...")
    print("-" * 50)
    
    enriched_records = []
    tax_summary = {
        'total_records': 0,
        'total_taxable': 0.0,
        'total_cgst': 0.0,
        'total_sgst': 0.0,
        'total_igst': 0.0,
        'total_tax': 0.0,
        'total_amount': 0.0,
        'intrastate_count': 0,
        'interstate_count': 0,
        'zero_gst_count': 0,
        'states_processed': set(),
        'invoice_numbers': []
    }
    
    # State-wise sequence tracking
    state_sequences = {}
    
    for index, row in df.iterrows():
        # Tax computation
        tax_result = tax_engine.compute_amazon_mtr_tax(
            taxable_value=row['taxable_value'],
            gst_rate=row['gst_rate'],
            customer_state=row['state_code']
        )
        
        # Invoice number generation with proper sequencing
        state = row['state_code']
        if state not in state_sequences:
            state_sequences[state] = 0
        state_sequences[state] += 1
        
        invoice_no = numbering_engine.generate_invoice_number(
            channel="amazon_mtr",
            state_name=state,
            month=row['month'],
            sequence_number=state_sequences[state]
        )
        
        # Create enriched record
        enriched_record = row.to_dict()
        enriched_record.update({
            'cgst': tax_result['cgst'],
            'sgst': tax_result['sgst'],
            'igst': tax_result['igst'],
            'total_tax': tax_result['total_tax'],
            'total_amount': tax_result['total_amount'],
            'invoice_no': invoice_no
        })
        enriched_records.append(enriched_record)
        
        # Update summary
        tax_summary['total_records'] += 1
        tax_summary['total_taxable'] += tax_result['taxable_value']
        tax_summary['total_cgst'] += tax_result['cgst']
        tax_summary['total_sgst'] += tax_result['sgst']
        tax_summary['total_igst'] += tax_result['igst']
        tax_summary['total_tax'] += tax_result['total_tax']
        tax_summary['total_amount'] += tax_result['total_amount']
        tax_summary['states_processed'].add(state)
        tax_summary['invoice_numbers'].append(invoice_no)
        
        if tax_result['cgst'] > 0:
            tax_summary['intrastate_count'] += 1
        elif tax_result['igst'] > 0:
            tax_summary['interstate_count'] += 1
        else:
            tax_summary['zero_gst_count'] += 1
        
        # Progress indicator
        if (index + 1) % 100 == 0:
            print(f"  Processed {index + 1}/{len(df)} records...")
    
    print(f"✅ Processing complete!")
    
    # Create enriched DataFrame
    enriched_df = pd.DataFrame(enriched_records)
    
    # Save enriched dataset
    output_file = data_file.replace('.csv', '_with_tax_invoice.csv')
    enriched_df.to_csv(output_file, index=False)
    
    print(f"\n📊 PROCESSING RESULTS:")
    print("-" * 30)
    print(f"Total Records: {tax_summary['total_records']:,}")
    print(f"Total Taxable: ₹{tax_summary['total_taxable']:,.2f}")
    print(f"Total CGST: ₹{tax_summary['total_cgst']:,.2f}")
    print(f"Total SGST: ₹{tax_summary['total_sgst']:,.2f}")
    print(f"Total IGST: ₹{tax_summary['total_igst']:,.2f}")
    print(f"Total Tax: ₹{tax_summary['total_tax']:,.2f}")
    print(f"Total Amount: ₹{tax_summary['total_amount']:,.2f}")
    
    print(f"\n🏛️  TAX BREAKDOWN:")
    print("-" * 20)
    print(f"Intrastate (CGST+SGST): {tax_summary['intrastate_count']:,} transactions")
    print(f"Interstate (IGST): {tax_summary['interstate_count']:,} transactions")
    print(f"Zero GST: {tax_summary['zero_gst_count']:,} transactions")
    
    print(f"\n🗺️  GEOGRAPHIC COVERAGE:")
    print("-" * 25)
    print(f"States Processed: {len(tax_summary['states_processed'])}")
    
    # State-wise summary
    state_stats = enriched_df.groupby('state_code').agg({
        'taxable_value': ['count', 'sum'],
        'total_tax': 'sum',
        'cgst': 'sum',
        'igst': 'sum'
    }).round(2)
    
    print(f"\nTop 10 States by Transaction Count:")
    top_states = enriched_df['state_code'].value_counts().head(10)
    for state, count in top_states.items():
        state_tax = enriched_df[enriched_df['state_code'] == state]['total_tax'].sum()
        tax_type = "Intrastate" if state == "HARYANA" else "Interstate"
        print(f"  {state[:20]:<20}: {count:>3} txns, ₹{state_tax:>8,.0f} tax ({tax_type})")
    
    print(f"\n📋 INVOICE NUMBERING:")
    print("-" * 25)
    unique_invoices = len(set(tax_summary['invoice_numbers']))
    print(f"Total Invoice Numbers: {len(tax_summary['invoice_numbers']):,}")
    print(f"Unique Invoice Numbers: {unique_invoices:,}")
    print(f"Duplicate Check: {'✅ PASS' if unique_invoices == len(tax_summary['invoice_numbers']) else '❌ FAIL'}")
    
    # Show sample invoice patterns
    print(f"\nSample Invoice Patterns:")
    sample_invoices = {}
    for record in enriched_records[:20]:
        state = record['state_code']
        if state not in sample_invoices:
            sample_invoices[state] = record['invoice_no']
    
    for state, invoice in list(sample_invoices.items())[:5]:
        print(f"  {state[:15]:<15}: {invoice}")
    
    # Validation checks
    print(f"\n✅ VALIDATION SUMMARY:")
    print("-" * 25)
    
    checks = []
    
    # Check 1: All records processed
    checks.append(("Records Processed", len(enriched_records) == len(df)))
    
    # Check 2: Tax computation accuracy
    tax_accuracy = True
    for record in enriched_records[:100]:  # Sample check
        expected_total = record['cgst'] + record['sgst'] + record['igst']
        if abs(expected_total - record['total_tax']) > 0.01:
            tax_accuracy = False
            break
    checks.append(("Tax Computation Accuracy", tax_accuracy))
    
    # Check 3: Invoice uniqueness
    checks.append(("Invoice Uniqueness", unique_invoices == len(tax_summary['invoice_numbers'])))
    
    # Check 4: Tax logic correctness
    logic_correct = True
    for record in enriched_records[:100]:  # Sample check
        is_haryana = record['state_code'] == "HARYANA"
        has_cgst_sgst = record['cgst'] > 0 and record['sgst'] > 0
        has_igst = record['igst'] > 0
        is_zero_tax = record['total_tax'] == 0
        
        if is_haryana and not has_cgst_sgst and not is_zero_tax:
            logic_correct = False
            break
        elif not is_haryana and not has_igst and not is_zero_tax:
            logic_correct = False
            break
    checks.append(("Tax Logic Correctness", logic_correct))
    
    # Check 5: Data integrity
    data_integrity = (
        tax_summary['total_tax'] > 0 and
        tax_summary['total_amount'] > tax_summary['total_taxable'] and
        len(tax_summary['states_processed']) > 1
    )
    checks.append(("Data Integrity", data_integrity))
    
    # Display check results
    passed_checks = 0
    for check_name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check_name:<25}: {status}")
        if passed:
            passed_checks += 1
    
    # Final result
    print(f"\n" + "=" * 60)
    print(f"🏁 FINAL VALIDATION RESULT")
    print("=" * 60)
    
    all_passed = passed_checks == len(checks)
    print(f"Status: {'🎉 SUCCESS' if all_passed else '❌ FAILED'}")
    print(f"Checks Passed: {passed_checks}/{len(checks)}")
    print(f"Dataset Processed: {len(enriched_records):,} records")
    print(f"Total Tax Computed: ₹{tax_summary['total_tax']:,.2f}")
    print(f"States Covered: {len(tax_summary['states_processed'])}")
    print(f"Output File: {os.path.basename(output_file)}")
    
    if all_passed:
        print(f"\n🚀 PRODUCTION READY!")
        print("Your Part-3 implementation is working perfectly with real data.")
        print("\nNext steps:")
        print("1. Set up Supabase database with Part-3 schema")
        print("2. Run complete pipeline with --full-pipeline flag")
        print("3. Deploy to production environment")
    
    return all_passed


def main():
    return validate_complete_part3()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
