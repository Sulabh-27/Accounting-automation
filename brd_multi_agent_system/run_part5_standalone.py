#!/usr/bin/env python3
"""
Standalone Part-5 runner that bypasses Supabase path issues
"""
import sys
import os
import pandas as pd
import uuid
sys.path.append('.')

from ingestion_layer.agents.tally_exporter import TallyExporterAgent


class MockSupabase:
    """Mock Supabase that avoids path issues."""
    
    def __init__(self):
        self.tally_exports = []
    
    @property
    def client(self):
        return MockClient(self)


class MockClient:
    def __init__(self, parent):
        self.parent = parent
    
    def table(self, table_name):
        return MockTable(table_name, self.parent)


class MockTable:
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
    def execute(self):
        return MockResponse([])


class MockResponse:
    def __init__(self, data):
        self.data = data


def main():
    print("🏭 PART-5 STANDALONE: Tally Export")
    print("Bypassing Supabase path issues")
    print("=" * 50)
    
    # Configuration
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    print(f"🔧 Configuration:")
    print(f"    GSTIN: {gstin}")
    print(f"    Channel: {channel}")
    print(f"    Month: {month}")
    
    # Check batch files
    batch_dir = "ingestion_layer/data/batches"
    if not os.path.exists(batch_dir):
        print("❌ No batch files found. Run Parts 1-4 first!")
        return 1
    
    batch_files = [f for f in os.listdir(batch_dir) if f.endswith('_batch.csv')]
    if not batch_files:
        print("❌ No batch files found. Run Parts 1-4 first!")
        return 1
    
    print(f"📁 Found {len(batch_files)} batch files:")
    for batch_file in batch_files:
        file_path = os.path.join(batch_dir, batch_file)
        df = pd.read_csv(file_path)
        print(f"    - {batch_file} ({len(df)} records)")
    
    # Initialize components
    mock_supa = MockSupabase()
    tally_exporter = TallyExporterAgent(mock_supa)
    
    # Create exports directory
    export_dir = "ingestion_layer/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    try:
        # Process batch files
        export_result = tally_exporter.process_batch_files(
            batch_dir, gstin, channel, month, run_id, export_dir
        )
        
        if export_result.success:
            print(f"\n✅ PART-5 SUCCESS!")
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
                    print(f"    {i+1}. ✅ {filename} ({file_size:,} bytes)")
                    
                    # Quick validation
                    try:
                        df_check = pd.read_excel(export_path, skiprows=4)
                        taxable_sum = df_check['Taxable Amount'].sum() if 'Taxable Amount' in df_check.columns else 0
                        print(f"        Records: {len(df_check)}, Taxable: ₹{taxable_sum:,.2f}")
                    except Exception as e:
                        print(f"        Validation: {str(e)[:50]}...")
                else:
                    print(f"    {i+1}. ❌ {os.path.basename(export_path)} (not created)")
            
            print(f"\n💾 Mock Database Records: {len(mock_supa.tally_exports)}")
            for i, record in enumerate(mock_supa.tally_exports):
                print(f"    {i+1}. GST Rate: {record.get('gst_rate')}%, Records: {record.get('record_count')}, Taxable: ₹{record.get('total_taxable', 0):,.2f}")
            
            print(f"\n🎉 PART-5 COMPLETE!")
            print(f"📁 X2Beta files ready for Tally import in: {export_dir}")
            print(f"🗄️  In production, these records would be in tally_exports table")
            
            return 0
        else:
            print(f"\n❌ PART-5 FAILED: {export_result.error_message}")
            return 1
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
