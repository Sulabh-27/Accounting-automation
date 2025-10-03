#!/usr/bin/env python3
"""
Run complete pipeline with Part-5 to populate tally_exports table
"""
import sys
import os
import subprocess
sys.path.append('.')


def run_complete_pipeline():
    """Run the complete pipeline including Part-5."""
    
    print("🚀 RUNNING COMPLETE PIPELINE (Parts 1+2+3+4+5)")
    print("=" * 60)
    
    # Check if input file exists
    input_file = "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx"
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return False
    
    print(f"📁 Input file: {os.path.basename(input_file)} ({os.path.getsize(input_file):,} bytes)")
    
    # Pipeline configuration
    config = {
        'agent': 'amazon_mtr',
        'input': input_file,
        'channel': 'amazon',
        'gstin': '06ABGCS4796R1ZA',
        'month': '2025-08'
    }
    
    print(f"🔧 Configuration:")
    for key, value in config.items():
        print(f"    {key}: {value}")
    
    # Build command
    cmd = [
        'python', '-m', 'ingestion_layer.main',
        '--agent', config['agent'],
        '--input', config['input'],
        '--channel', config['channel'],
        '--gstin', config['gstin'],
        '--month', config['month'],
        '--full-pipeline'  # This enables all parts including Part-5
    ]
    
    print(f"\n🚀 Running pipeline...")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        # Run the pipeline
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        print(f"\n📋 Pipeline Results:")
        print(f"Exit code: {result.returncode}")
        
        if result.stdout:
            print(f"\n📤 Output:")
            print(result.stdout)
        
        if result.stderr:
            print(f"\n⚠️  Errors:")
            print(result.stderr)
        
        # Check what files were created
        print(f"\n📁 Checking output files:")
        
        # Check exports directory
        exports_dir = "ingestion_layer/exports"
        if os.path.exists(exports_dir):
            export_files = [f for f in os.listdir(exports_dir) if f.endswith('.xlsx')]
            print(f"✅ X2Beta exports: {len(export_files)} files")
            for export_file in export_files:
                file_path = os.path.join(exports_dir, export_file)
                file_size = os.path.getsize(file_path)
                print(f"    - {export_file} ({file_size:,} bytes)")
        else:
            print(f"❌ Exports directory not found")
        
        # Check batches directory
        batches_dir = "ingestion_layer/data/batches"
        if os.path.exists(batches_dir):
            batch_files = [f for f in os.listdir(batches_dir) if f.endswith('_batch.csv')]
            print(f"✅ Batch files: {len(batch_files)} files")
            for batch_file in batch_files:
                print(f"    - {batch_file}")
        else:
            print(f"❌ Batches directory not found")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return False


def main():
    print("🎯 COMPLETE PIPELINE EXECUTION")
    print("This will populate the tally_exports table")
    print("=" * 50)
    
    success = run_complete_pipeline()
    
    if success:
        print(f"\n🎉 Pipeline completed successfully!")
        print(f"\n📊 Expected Database Changes:")
        print(f"✅ tally_exports table should now have records")
        print(f"✅ Each GST rate will have a separate export record")
        print(f"✅ Files created in ingestion_layer/exports/")
        
        print(f"\n🔍 Check your Supabase tally_exports table now!")
        print(f"You should see records with:")
        print(f"  - run_id: UUID of this pipeline run")
        print(f"  - channel: amazon_mtr")
        print(f"  - gstin: 06ABGCS4796R1ZA")
        print(f"  - month: 2025-08")
        print(f"  - gst_rate: 0.0 and 0.18 (separate records)")
        print(f"  - record_count: Number of records in each export")
        print(f"  - total_taxable: Total taxable amount")
        print(f"  - total_tax: Total tax amount")
    else:
        print(f"\n❌ Pipeline failed - check the output above")
        print(f"Common issues:")
        print(f"  - Supabase credentials not configured")
        print(f"  - Database schema not set up")
        print(f"  - Input file path issues")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
