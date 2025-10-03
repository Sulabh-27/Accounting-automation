#!/usr/bin/env python3
"""
Fix for Supabase path issue - create a mock mode that works without Supabase
"""
import sys
import os
sys.path.append('.')

from ingestion_layer.libs.supabase_client import SupabaseClientWrapper


def create_mock_supabase_wrapper():
    """Create a mock Supabase wrapper that works without actual Supabase connection."""
    
    class MockSupabaseWrapper(SupabaseClientWrapper):
        """Mock Supabase wrapper that handles file operations locally."""
        
        def __init__(self):
            # Initialize without connecting to Supabase
            self.url = None
            self.key = None
            self.bucket = "mock-bucket"
            self.client = None
            
            # Create local directories for mock storage
            self.local_storage_dir = "ingestion_layer/data/mock_storage"
            os.makedirs(self.local_storage_dir, exist_ok=True)
        
        def upload_file(self, local_path: str, dest_path: str = None) -> str:
            """Mock upload - just copy to local storage and return local path."""
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")
            
            # Create a mock storage path
            filename = os.path.basename(local_path)
            mock_storage_path = os.path.join(self.local_storage_dir, filename)
            
            # Copy file to mock storage
            import shutil
            shutil.copy2(local_path, mock_storage_path)
            
            # Return the local path (not a Supabase path)
            return mock_storage_path
        
        def insert_report_metadata(self, run_id, report_type: str, file_path: str):
            """Mock metadata insertion."""
            print(f"Mock: Inserted report metadata for {report_type}: {os.path.basename(file_path)}")
            return {
                "id": str(run_id),
                "run_id": str(run_id),
                "report_type": report_type,
                "file_path": file_path
            }
        
        def list_reports(self, run_id):
            """Mock report listing."""
            return []
        
        def insert_run_start(self, run_id, **kwargs):
            """Mock run start."""
            print(f"Mock: Started run {str(run_id)[:8]}... with {kwargs}")
        
        def update_run_finish(self, run_id, **kwargs):
            """Mock run finish."""
            print(f"Mock: Finished run {str(run_id)[:8]}... with {kwargs}")
    
    return MockSupabaseWrapper()


def test_mock_supabase():
    """Test the mock Supabase wrapper."""
    
    print("🔧 TESTING MOCK SUPABASE WRAPPER")
    print("=" * 40)
    
    # Create mock wrapper
    mock_supa = create_mock_supabase_wrapper()
    
    # Test file upload
    test_file = "ingestion_layer/data/batches/amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv"
    
    if os.path.exists(test_file):
        print(f"📁 Testing with file: {os.path.basename(test_file)}")
        
        try:
            uploaded_path = mock_supa.upload_file(test_file)
            print(f"✅ Mock upload successful: {uploaded_path}")
            
            # Verify the uploaded file exists
            if os.path.exists(uploaded_path):
                print(f"✅ Mock storage file exists: {os.path.getsize(uploaded_path)} bytes")
                return True
            else:
                print(f"❌ Mock storage file not found")
                return False
                
        except Exception as e:
            print(f"❌ Mock upload failed: {e}")
            return False
    else:
        print(f"❌ Test file not found: {test_file}")
        return False


def create_pipeline_with_mock_supabase():
    """Create a version of the pipeline that uses mock Supabase."""
    
    print(f"\n🚀 CREATING PIPELINE WITH MOCK SUPABASE")
    print("=" * 50)
    
    # Create a simple pipeline runner that bypasses Supabase issues
    pipeline_code = '''#!/usr/bin/env python3
"""
Pipeline runner with mock Supabase to avoid path issues
"""
import sys
import os
import uuid
sys.path.append('.')

from ingestion_layer.agents.amazon_mtr_agent import AmazonMTRAgent
from ingestion_layer.agents.tally_exporter import TallyExporterAgent
from ingestion_layer.libs.contracts import IngestionRequest


class MockSupabase:
    """Simple mock Supabase for testing."""
    
    def __init__(self):
        self.storage_dir = "ingestion_layer/data/mock_storage"
        os.makedirs(self.storage_dir, exist_ok=True)
        self.tally_exports = []
    
    def upload_file(self, local_path: str, dest_path: str = None) -> str:
        """Mock upload - return local path."""
        return local_path  # Just return the original path
    
    def insert_report_metadata(self, run_id, report_type: str, file_path: str):
        print(f"Mock: Report metadata - {report_type}: {os.path.basename(file_path)}")
        return {"id": str(run_id), "file_path": file_path}
    
    def insert_run_start(self, run_id, **kwargs):
        print(f"Mock: Run started - {str(run_id)[:8]}...")
    
    def update_run_finish(self, run_id, **kwargs):
        print(f"Mock: Run finished - {str(run_id)[:8]}...")
    
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


def run_part5_only():
    """Run only Part-5 with existing batch files."""
    
    print("🏭 RUNNING PART-5 WITH MOCK SUPABASE")
    print("=" * 50)
    
    # Configuration
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    # Initialize mock Supabase
    mock_supa = MockSupabase()
    
    # Initialize Tally Exporter
    tally_exporter = TallyExporterAgent(mock_supa)
    
    # Check batch files
    batch_dir = "ingestion_layer/data/batches"
    if not os.path.exists(batch_dir):
        print("❌ No batch files found")
        return False
    
    # Process batch files
    try:
        export_result = tally_exporter.process_batch_files(
            batch_dir, gstin, channel, month, run_id, "ingestion_layer/exports"
        )
        
        if export_result.success:
            print(f"✅ Part-5 Success!")
            print(f"    Exported files: {export_result.exported_files}")
            print(f"    Total records: {export_result.total_records}")
            print(f"    Total taxable: ₹{export_result.total_taxable:,.2f}")
            print(f"    Total tax: ₹{export_result.total_tax:,.2f}")
            
            print(f"\\n📄 Files created:")
            for export_path in export_result.export_paths:
                if os.path.exists(export_path):
                    filename = os.path.basename(export_path)
                    file_size = os.path.getsize(export_path)
                    print(f"    ✅ {filename} ({file_size:,} bytes)")
                else:
                    print(f"    ❌ {os.path.basename(export_path)} (not found)")
            
            print(f"\\n💾 Database records: {len(mock_supa.tally_exports)}")
            
            return True
        else:
            print(f"❌ Part-5 failed: {export_result.error_message}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_part5_only()
    print(f"\\n{'✅ SUCCESS' if success else '❌ FAILED'}")
    sys.exit(0 if success else 1)
'''
    
    # Write the pipeline runner
    runner_path = "run_part5_mock.py"
    with open(runner_path, 'w') as f:
        f.write(pipeline_code)
    
    print(f"✅ Created mock pipeline runner: {runner_path}")
    return runner_path


def main():
    print("🔧 SUPABASE PATH ISSUE FIX")
    print("=" * 40)
    
    # Test mock Supabase
    mock_test = test_mock_supabase()
    
    # Create mock pipeline
    runner_path = create_pipeline_with_mock_supabase()
    
    if mock_test and runner_path:
        print(f"\\n🎉 SOLUTION READY!")
        print(f"\\n📋 To run Part-5 without Supabase issues:")
        print(f"   python {runner_path}")
        print(f"\\n📋 This will:")
        print(f"   ✅ Use existing batch files from Parts 1-4")
        print(f"   ✅ Convert them to X2Beta Excel files")
        print(f"   ✅ Save files to ingestion_layer/exports/")
        print(f"   ✅ Avoid Supabase path issues")
        print(f"   ✅ Show what tally_exports table should contain")
    else:
        print(f"\\n❌ Solution setup failed")
    
    return 0 if (mock_test and runner_path) else 1


if __name__ == "__main__":
    sys.exit(main())
