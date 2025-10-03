#!/usr/bin/env python3
"""
Simple test for Part-5 Tally Export functionality
"""
import sys
import os
import pandas as pd
sys.path.append('.')

from ingestion_layer.libs.x2beta_writer import X2BetaWriter


def test_part5_components():
    """Test Part-5 components with existing batch data."""
    
    print("🏭 TESTING PART-5: Tally Export Components")
    print("=" * 50)
    
    # Check if batch files exist
    batch_dir = "ingestion_layer/data/batches"
    if not os.path.exists(batch_dir):
        print("❌ Batch directory not found. Run Part-4 first!")
        return False
    
    batch_files = [f for f in os.listdir(batch_dir) if f.endswith('_batch.csv')]
    if not batch_files:
        print("❌ No batch files found. Run Part-4 first!")
        return False
    
    print(f"📁 Found {len(batch_files)} batch files:")
    for batch_file in batch_files:
        print(f"    - {batch_file}")
    
    # Test X2Beta Writer
    print(f"\n🔧 Testing X2Beta Writer Components:")
    
    writer = X2BetaWriter()
    
    # Test 1: Template validation
    template_path = "ingestion_layer/templates/X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx"
    
    if os.path.exists(template_path):
        template_info = writer.get_template_info(template_path)
        print(f"✅ Template exists: {template_info['exists']}")
        print(f"    Sheet: {template_info.get('sheet_name', 'N/A')}")
        print(f"    Headers: {len(template_info.get('headers', []))}")
    else:
        print(f"❌ Template not found: {template_path}")
        return False
    
    # Test 2: Load and validate batch data
    sample_batch_file = os.path.join(batch_dir, batch_files[0])
    print(f"\n📄 Testing with batch file: {os.path.basename(sample_batch_file)}")
    
    try:
        df = pd.read_csv(sample_batch_file)
        print(f"✅ Loaded batch data: {len(df)} records")
        print(f"    Columns: {list(df.columns)}")
        
        # Validate batch data
        validation = writer.validate_batch_data(df)
        print(f"✅ Batch validation: {validation['valid']}")
        
        if validation['valid']:
            print(f"    GST Rate: {validation['gst_rate']}")
            print(f"    Records: {validation['record_count']}")
        else:
            print(f"    Errors: {validation['errors']}")
            return False
        
        # Test X2Beta mapping
        x2beta_df = writer.map_batch_to_x2beta(df, '06ABGCS4796R1ZA', '2025-08')
        print(f"✅ X2Beta mapping: {len(x2beta_df)} records converted")
        
        if len(x2beta_df) > 0:
            sample_record = x2beta_df.iloc[0]
            print(f"    Sample voucher: {sample_record.get('Voucher No.', 'N/A')}")
            print(f"    Party ledger: {sample_record.get('Party Ledger', 'N/A')}")
            print(f"    Taxable amount: ₹{sample_record.get('Taxable Amount', 0):,.2f}")
            
            # Show tax details
            cgst = sample_record.get('CGST Amount', 0)
            sgst = sample_record.get('SGST Amount', 0) 
            igst = sample_record.get('IGST Amount', 0)
            
            if cgst > 0 or sgst > 0:
                print(f"    Tax type: Intrastate (CGST: ₹{cgst:.2f}, SGST: ₹{sgst:.2f})")
            elif igst > 0:
                print(f"    Tax type: Interstate (IGST: ₹{igst:.2f})")
            else:
                print(f"    Tax type: Zero GST")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing batch file: {e}")
        return False


def main():
    print("🚀 PART-5 SIMPLE TEST")
    print("Testing Tally Export Components")
    print("=" * 40)
    
    success = test_part5_components()
    
    if success:
        print(f"\n✅ Part-5 components are working!")
        print(f"🚀 Ready to run complete pipeline:")
        print(f"   python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"     --full-pipeline")
        print(f"\n📁 Expected outputs:")
        print(f"   - X2Beta Excel files: ingestion_layer/exports/")
        print(f"   - Database records: tally_exports table")
    else:
        print(f"\n❌ Part-5 test failed - check the setup")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
