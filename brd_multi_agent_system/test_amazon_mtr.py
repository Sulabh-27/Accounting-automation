#!/usr/bin/env python3
"""
Quick test script to process the Amazon MTR sample data
"""
import sys
import os
sys.path.append('.')

from ingestion_layer.libs.contracts import IngestionRequest
from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.amazon_mtr_agent import AmazonMTRAgent
import uuid

def main():
    # Test with your sample data
    file_path = "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    # Create test request
    request = IngestionRequest(
        run_id=uuid.uuid4(),
        channel="amazon",
        gstin="06ABGCS4796R1ZA",  # From your sample data
        month="2025-08",
        report_type="amazon_mtr",
        file_path=file_path
    )
    
    # Use a fake Supabase client for testing
    class FakeSupabase(SupabaseClientWrapper):
        def __init__(self):
            self.uploads = []
            self.reports = []
            
        def upload_file(self, local_path, dest_path=None):
            print(f"Would upload: {local_path}")
            fake_path = f"raw-reports/{os.path.basename(local_path)}"
            self.uploads.append(fake_path)
            return fake_path
            
        def insert_report_metadata(self, run_id, report_type, file_path):
            print(f"Would insert metadata: {report_type} -> {file_path}")
            self.reports.append({"run_id": run_id, "report_type": report_type, "file_path": file_path})
            return {"id": str(uuid.uuid4())}
    
    supabase = FakeSupabase()
    agent = AmazonMTRAgent()
    
    try:
        result_path = agent.process(request, supabase)
        print(f"✅ Success! Processed file and would upload to: {result_path}")
        print(f"📊 Reports created: {len(supabase.reports)}")
        print(f"📁 Files uploaded: {len(supabase.uploads)}")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
