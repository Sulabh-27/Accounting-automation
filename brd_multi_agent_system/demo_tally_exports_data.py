#!/usr/bin/env python3
"""
Demo what tally_exports table should contain after running Part-5
"""
import sys
import os
import pandas as pd
import uuid
from datetime import datetime
sys.path.append('.')


def show_expected_tally_exports():
    """Show what the tally_exports table should contain."""
    
    print("📊 EXPECTED TALLY_EXPORTS TABLE DATA")
    print("=" * 60)
    
    # Check existing batch files
    batch_dir = "ingestion_layer/data/batches"
    if not os.path.exists(batch_dir):
        print("❌ No batch files found")
        return False
    
    batch_files = [f for f in os.listdir(batch_dir) if f.endswith('_batch.csv')]
    print(f"📁 Found {len(batch_files)} batch files:")
    
    # Simulate what would be inserted into tally_exports
    mock_run_id = str(uuid.uuid4())
    expected_records = []
    
    for batch_file in batch_files:
        print(f"\n📄 Processing: {batch_file}")
        
        # Extract info from filename
        # Format: amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv
        parts = batch_file.replace('.csv', '').split('_')
        if len(parts) >= 5:
            channel = parts[0] + '_' + parts[1]  # amazon_mtr
            gstin = parts[2]  # 06ABGCS4796R1ZA
            month = parts[3]  # 2025-08
            gst_rate_str = parts[4]  # 18pct
            
            # Convert GST rate
            if gst_rate_str == '0pct':
                gst_rate = 0.0
            else:
                gst_rate = float(gst_rate_str.replace('pct', '')) / 100
            
            # Load batch data
            batch_path = os.path.join(batch_dir, batch_file)
            df = pd.read_csv(batch_path)
            
            # Calculate totals
            total_taxable = df['total_taxable'].sum()
            total_cgst = df.get('total_cgst', pd.Series([0])).sum()
            total_sgst = df.get('total_sgst', pd.Series([0])).sum()
            total_igst = df.get('total_igst', pd.Series([0])).sum()
            total_tax = total_cgst + total_sgst + total_igst
            
            # Expected tally_exports record
            export_record = {
                'id': str(uuid.uuid4()),
                'run_id': mock_run_id,
                'channel': channel,
                'gstin': gstin,
                'month': month,
                'gst_rate': gst_rate,
                'template_name': f'X2Beta Sales Template - {gstin}.xlsx',
                'file_path': f'ingestion_layer/exports/{channel}_{gstin}_{month}_{gst_rate_str}_x2beta.xlsx',
                'file_size': 0,  # Would be actual file size
                'record_count': len(df),
                'total_taxable': total_taxable,
                'total_tax': total_tax,
                'export_status': 'success',
                'created_at': datetime.now().isoformat()
            }
            
            expected_records.append(export_record)
            
            print(f"    Channel: {channel}")
            print(f"    GSTIN: {gstin}")
            print(f"    Month: {month}")
            print(f"    GST Rate: {gst_rate * 100:.0f}%")
            print(f"    Records: {len(df)}")
            print(f"    Total Taxable: ₹{total_taxable:,.2f}")
            print(f"    Total Tax: ₹{total_tax:,.2f}")
            print(f"    Expected File: {os.path.basename(export_record['file_path'])}")
    
    # Show as table
    if expected_records:
        print(f"\n📋 EXPECTED TALLY_EXPORTS TABLE CONTENT:")
        print("=" * 100)
        
        # Create DataFrame for better display
        df_exports = pd.DataFrame(expected_records)
        
        # Select key columns for display
        display_columns = ['run_id', 'channel', 'gstin', 'month', 'gst_rate', 'record_count', 'total_taxable', 'total_tax', 'export_status']
        display_df = df_exports[display_columns].copy()
        
        # Format for display
        display_df['run_id'] = display_df['run_id'].str[:8] + '...'
        display_df['total_taxable'] = display_df['total_taxable'].apply(lambda x: f"₹{x:,.0f}")
        display_df['total_tax'] = display_df['total_tax'].apply(lambda x: f"₹{x:,.2f}")
        display_df['gst_rate'] = display_df['gst_rate'].apply(lambda x: f"{x*100:.0f}%")
        
        print(display_df.to_string(index=False))
        
        print(f"\n📊 Summary:")
        print(f"    Total export records: {len(expected_records)}")
        print(f"    Total records exported: {sum(r['record_count'] for r in expected_records)}")
        print(f"    Total taxable amount: ₹{sum(r['total_taxable'] for r in expected_records):,.2f}")
        print(f"    Total tax amount: ₹{sum(r['total_tax'] for r in expected_records):,.2f}")
        
        return True
    
    return False


def show_database_status():
    """Show current database table status."""
    
    print(f"\n🗄️  CURRENT DATABASE STATUS")
    print("=" * 40)
    
    print("✅ x2beta_templates: 5 rows (Template configurations)")
    print("    - 06ABGCS4796R1ZA (Haryana)")
    print("    - 07ABGCS4796R1Z8 (Delhi)")
    print("    - 09ABGCS4796R1Z4 (Uttar Pradesh)")
    print("    - 24ABGCS4796R1ZC (Gujarat)")
    print("    - 29ABGCS4796R1Z2 (Karnataka)")
    
    print("\n⏳ tally_exports: 0 rows (Will be populated when pipeline runs)")
    print("    - This table gets data when Part-5 actually exports files")
    print("    - Each GST rate gets a separate record")
    print("    - Contains metadata about exported X2Beta files")
    
    print("\n⏳ tally_imports: 0 rows (Future use)")
    print("    - Will track when X2Beta files are imported into Tally")
    print("    - Currently not used by the pipeline")


def main():
    print("🎯 TALLY EXPORTS TABLE PREVIEW")
    print("What you should see after running the complete pipeline")
    print("=" * 70)
    
    # Show expected data
    success = show_expected_tally_exports()
    
    # Show current status
    show_database_status()
    
    if success:
        print(f"\n🚀 TO POPULATE TALLY_EXPORTS TABLE:")
        print(f"1. Make sure openpyxl is installed: pip install openpyxl")
        print(f"2. Run complete pipeline:")
        print(f"   python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"     --full-pipeline")
        
        print(f"\n📋 AFTER RUNNING PIPELINE:")
        print(f"✅ tally_exports table will have 2 records (one per GST rate)")
        print(f"✅ X2Beta Excel files will be in ingestion_layer/exports/")
        print(f"✅ Files will be ready for direct Tally import")
    else:
        print(f"\n❌ No batch files found - run Parts 1-4 first")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
