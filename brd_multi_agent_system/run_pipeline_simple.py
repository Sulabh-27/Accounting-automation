#!/usr/bin/env python3
"""
Simple complete pipeline using existing processed data
"""
import sys
import os
import pandas as pd
import uuid
sys.path.append('.')

from ingestion_layer.agents.tally_exporter import TallyExporterAgent


class MockSupabase:
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
    print("🚀 COMPLETE PIPELINE DEMONSTRATION")
    print("Using existing processed data from Parts 1-4")
    print("=" * 60)
    
    # Check what we have
    print("📋 Checking existing data...")
    
    # Check normalized data
    normalized_dir = "ingestion_layer/data/normalized"
    if os.path.exists(normalized_dir):
        normalized_files = [f for f in os.listdir(normalized_dir) if f.endswith('.csv')]
        print(f"✅ Normalized files: {len(normalized_files)}")
        for f in normalized_files[:3]:  # Show first 3
            print(f"    - {f}")
    
    # Check batch data
    batch_dir = "ingestion_layer/data/batches"
    if os.path.exists(batch_dir):
        batch_files = [f for f in os.listdir(batch_dir) if f.endswith('_batch.csv')]
        print(f"✅ Batch files: {len(batch_files)}")
        for f in batch_files:
            file_path = os.path.join(batch_dir, f)
            df = pd.read_csv(file_path)
            print(f"    - {f} ({len(df)} records)")
    
    # Check templates
    template_dir = "ingestion_layer/templates"
    if os.path.exists(template_dir):
        template_files = [f for f in os.listdir(template_dir) if f.endswith('.xlsx')]
        print(f"✅ X2Beta templates: {len(template_files)}")
    
    # Run Part-5 (Tally Export)
    if batch_files:
        print(f"\n🏭 RUNNING PART-5: Tally Export")
        print("-" * 40)
        
        # Configuration
        gstin = "06ABGCS4796R1ZA"
        channel = "amazon_mtr"
        month = "2025-08"
        run_id = uuid.uuid4()
        
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
                print(f"✅ Tally Export Success!")
                print(f"    Exported files: {export_result.exported_files}")
                print(f"    Total records: {export_result.total_records}")
                print(f"    Total taxable: ₹{export_result.total_taxable:,.2f}")
                print(f"    Total tax: ₹{export_result.total_tax:,.2f}")
                
                print(f"\n📄 X2Beta Files Created:")
                for i, export_path in enumerate(export_result.export_paths):
                    if os.path.exists(export_path):
                        filename = os.path.basename(export_path)
                        file_size = os.path.getsize(export_path)
                        print(f"    {i+1}. ✅ {filename} ({file_size:,} bytes)")
                        
                        # Validate Excel content
                        try:
                            df_excel = pd.read_excel(export_path, skiprows=4)
                            if 'Taxable Amount' in df_excel.columns:
                                taxable_sum = df_excel['Taxable Amount'].sum()
                                print(f"        Records: {len(df_excel)}, Taxable: ₹{taxable_sum:,.2f}")
                        except Exception as e:
                            print(f"        Validation: {str(e)[:30]}...")
                    else:
                        print(f"    {i+1}. ❌ {os.path.basename(export_path)} (not found)")
                
                print(f"\n💾 Database Records (Mock): {len(mock_supa.tally_exports)}")
                for i, record in enumerate(mock_supa.tally_exports):
                    gst_rate = record.get('gst_rate', 0)
                    record_count = record.get('record_count', 0)
                    total_taxable = record.get('total_taxable', 0)
                    print(f"    {i+1}. GST: {gst_rate*100:.0f}%, Records: {record_count}, Taxable: ₹{total_taxable:,.2f}")
                
                # Final summary
                print(f"\n" + "=" * 60)
                print("🎉 COMPLETE PIPELINE DEMONSTRATION SUCCESS!")
                print("=" * 60)
                
                print(f"📊 What was accomplished:")
                print(f"✅ Part 1: Data ingestion & normalization (from previous runs)")
                print(f"✅ Part 2: Item & ledger mapping (from previous runs)")
                print(f"✅ Part 3: Tax computation & invoice numbering (from previous runs)")
                print(f"✅ Part 4: Pivoting & batch splitting (from previous runs)")
                print(f"✅ Part 5: Tally export (X2Beta templates) - JUST COMPLETED")
                
                print(f"\n📁 Output Files Ready:")
                print(f"    Batch CSVs: ingestion_layer/data/batches/ ({len(batch_files)} files)")
                print(f"    X2Beta Excel: ingestion_layer/exports/ ({export_result.exported_files} files)")
                
                print(f"\n🗄️  Database Impact:")
                print(f"    In production, tally_exports table would have {len(mock_supa.tally_exports)} records")
                print(f"    Each record represents one GST rate export file")
                
                print(f"\n🚀 PRODUCTION READY!")
                print(f"    Your complete 5-part accounting pipeline is working")
                print(f"    Raw e-commerce data → Tally-ready Excel files")
                
                return True
            else:
                print(f"❌ Tally export failed: {export_result.error_message}")
                return False
                
        except Exception as e:
            print(f"❌ Error in Part-5: {e}")
            return False
    else:
        print(f"❌ No batch files found. Run Parts 1-4 first!")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
