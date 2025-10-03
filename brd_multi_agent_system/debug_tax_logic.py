#!/usr/bin/env python3
"""
Debug tax logic issues
"""
import sys
import pandas as pd
sys.path.append('.')

from ingestion_layer.libs.tax_rules import TaxRulesEngine

def debug_tax_logic():
    """Debug the tax logic issues."""
    
    print("🔍 DEBUGGING TAX LOGIC ISSUES")
    print("=" * 40)
    
    # Load data
    data_file = "ingestion_layer/data/normalized/amazon_mtr_74b9a9f43e0043b58a7925760d541dc8.csv"
    df = pd.read_csv(data_file)
    
    # Initialize tax engine
    gstin = "06ABGCS4796R1ZA"  # Haryana (state code 06)
    tax_engine = TaxRulesEngine(gstin)
    
    print(f"Company GSTIN: {gstin}")
    print(f"Company State Code: {tax_engine.company_state_code}")
    
    # Check first 10 records
    sample_df = df.head(10)
    
    print(f"\nDETAILED TAX LOGIC ANALYSIS:")
    print("-" * 60)
    
    for index, row in sample_df.iterrows():
        state = row['state_code']
        taxable = row['taxable_value']
        gst_rate = row['gst_rate']
        
        # Check if intrastate
        is_intrastate = tax_engine._is_intrastate(state)
        
        # Compute tax
        tax_result = tax_engine.compute_amazon_mtr_tax(taxable, gst_rate, state)
        
        # Expected logic
        if is_intrastate:
            expected_cgst = round(taxable * gst_rate / 2, 2)
            expected_sgst = round(taxable * gst_rate / 2, 2)
            expected_igst = 0.0
        else:
            expected_cgst = 0.0
            expected_sgst = 0.0
            expected_igst = round(taxable * gst_rate, 2)
        
        # Check for errors
        cgst_ok = abs(tax_result['cgst'] - expected_cgst) < 0.01
        sgst_ok = abs(tax_result['sgst'] - expected_sgst) < 0.01
        igst_ok = abs(tax_result['igst'] - expected_igst) < 0.01
        
        status = "✅" if (cgst_ok and sgst_ok and igst_ok) else "❌"
        
        print(f"{index+1:2d}. {state[:15]:<15} | Intrastate: {is_intrastate} | {status}")
        print(f"    Expected: CGST={expected_cgst}, SGST={expected_sgst}, IGST={expected_igst}")
        print(f"    Actual:   CGST={tax_result['cgst']}, SGST={tax_result['sgst']}, IGST={tax_result['igst']}")
        
        if not (cgst_ok and sgst_ok and igst_ok):
            print(f"    🔍 Issue: Tax computation doesn't match expected logic")
        
        print()

if __name__ == "__main__":
    debug_tax_logic()
