#!/usr/bin/env python3
"""
Demo Part-5 with existing batch data
"""
import sys
import os
import pandas as pd
import uuid
sys.path.append('.')

from ingestion_layer.agents.tally_exporter import TallyExporterAgent
from ingestion_layer.libs.x2beta_writer import X2BetaWriter


class MockSupabase:
    """Mock Supabase client for demo."""
    
    def __init__(self):
        self.tally_exports = []
    
    @property
    def client(self):
        return MockClient(self)


class MockClient:
    """Mock Supabase client."""
    
    def __init__(self, parent):
        self.parent = parent
    
    def table(self, table_name):
        return MockTable(table_name, self.parent)


class MockTable:
    """Mock Supabase table."""
    
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def insert(self, data):
        if self.table_name == 'tally_exports':
            if isinstance(data, list):
                self.parent.tally_exports.extend(data)
            else:
                self.parent.tally_exports.append(data)
        return MockInsert()


class MockInsert:
    """Mock insert operation."""
    
    def execute(self):
        return MockResponse([])


class MockResponse:
    """Mock response."""
    
    def __init__(self, data):
        self.data = data


def demo_part5_export():
    """Demo Part-5 Tally Export with existing batch data."""
    
    print("🏭 PART-5 DEMO: Tally Export (X2Beta Templates)")
    print("=" * 60)
    
    # Check batch files
    batch_dir = "ingestion_layer/data/batches"
    if not os.path.exists(batch_dir):
        print("❌ No batch directory found. Run Parts 1-4 first!")
        return False
    
    batch_files = [f for f in os.listdir(batch_dir) if f.endswith('_batch.csv')]
    if not batch_files:
        print("❌ No batch files found. Run Parts 1-4 first!")
        return False
    
    print(f"📁 Found {len(batch_files)} batch files:")
    for batch_file in batch_files:
        file_path = os.path.join(batch_dir, batch_file)
        file_size = os.path.getsize(file_path)
        df = pd.read_csv(file_path)
        print(f"    - {batch_file} ({len(df)} records, {file_size:,} bytes)")
    
    # Initialize components
    supabase = MockSupabase()
    tally_exporter = TallyExporterAgent(supabase)
    
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    print(f"\n🔧 Configuration:")
    print(f"    GSTIN: {gstin}")
    print(f"    Channel: {channel}")
    print(f"    Month: {month}")
    print(f"    Run ID: {str(run_id)[:8]}...")
    
    # Step 1: Template Validation
    print(f"\n📋 STEP 1: Template Validation")
    print("-" * 30)
    
    template_validation = tally_exporter.validate_template_availability(gstin)
    
    if template_validation['available']:
        print(f"✅ Template Available:")
        print(f"    Name: {template_validation['template_name']}")
        print(f"    Company: {template_validation['company_name']}")
        print(f"    State: {template_validation['state_name']}")
        print(f"    Path: {template_validation['template_path']}")
    else:
        print(f"❌ Template validation failed")
        if 'error' in template_validation:
            print(f"    Error: {template_validation['error']}")
        return False
    
    # Step 2: Process Batch Files
    print(f"\n📄 STEP 2: Processing Batch Files")
    print("-" * 30)
    
    # Create exports directory
    export_dir = "ingestion_layer/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    try:
        export_result = tally_exporter.process_batch_files(
            batch_dir, gstin, channel, month, run_id, export_dir
        )
        
        if export_result.success:
            print(f"✅ Export Results:")
            print(f"    Processed files: {export_result.processed_files}")
            print(f"    Exported files: {export_result.exported_files}")
            print(f"    Total records: {export_result.total_records}")
            print(f"    Total taxable: ₹{export_result.total_taxable:,.2f}")
            print(f"    Total tax: ₹{export_result.total_tax:,.2f}")
            print(f"    GST rates: {export_result.gst_rates_processed}")
            
            print(f"\n📄 X2Beta Files Created:")
            for i, export_path in enumerate(export_result.export_paths):
                if os.path.exists(export_path):
                    filename = os.path.basename(export_path)
                    file_size = os.path.getsize(export_path)
                    print(f"    {i+1}. {filename} ({file_size:,} bytes)")
                    
                    # Quick validation of Excel file
                    try:
                        df_excel = pd.read_excel(export_path, skiprows=4)
                        total_taxable = df_excel['Taxable Amount'].sum() if 'Taxable Amount' in df_excel.columns else 0
                        print(f"        Records: {len(df_excel)}, Taxable: ₹{total_taxable:,.2f}")
                    except Exception as e:
                        print(f"        Could not read Excel: {str(e)[:50]}...")
                else:
                    print(f"    {i+1}. {os.path.basename(export_path)} (file not found)")
            
            # Step 3: Database Records
            print(f"\n💾 STEP 3: Database Integration")
            print("-" * 30)
            
            saved_exports = len(supabase.tally_exports)
            print(f"✅ Export Records Saved: {saved_exports}")
            
            if saved_exports > 0:
                for i, export_record in enumerate(supabase.tally_exports):
                    print(f"    {i+1}. GSTIN: {export_record.get('gstin')}, GST Rate: {export_record.get('gst_rate')}%, Records: {export_record.get('record_count')}")
            
            return True
            
        else:
            print(f"❌ Export failed: {export_result.error_message}")
            return False
            
    except Exception as e:
        print(f"❌ Error during export: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("🚀 PART-5 TALLY EXPORT DEMO")
    print("Converting batch files to X2Beta Excel templates")
    print("=" * 60)
    
    success = demo_part5_export()
    
    if success:
        print(f"\n🎉 Part-5 Demo Successful!")
        print(f"✅ X2Beta Excel files created in: ingestion_layer/exports/")
        print(f"✅ Files are ready for Tally import")
        print(f"\n🚀 To run complete pipeline:")
        print(f"python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"  --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"  --full-pipeline")
    else:
        print(f"\n❌ Part-5 demo failed")
        print(f"Make sure:")
        print(f"  1. X2Beta templates exist in ingestion_layer/templates/")
        print(f"  2. Batch files exist in ingestion_layer/data/batches/")
        print(f"  3. Database schema is set up correctly")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
