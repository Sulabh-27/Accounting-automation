#!/usr/bin/env python3
"""
Test all agents with the actual data files
"""
import sys
import os
import uuid
import pandas as pd
sys.path.append('.')

from ingestion_layer.libs.contracts import IngestionRequest
from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.amazon_mtr_agent import AmazonMTRAgent

class FakeSupabase(SupabaseClientWrapper):
    def __init__(self):
        self.uploads = []
        self.reports = []
        
    def upload_file(self, local_path, dest_path=None):
        print(f"  📤 Would upload: {os.path.basename(local_path)}")
        fake_path = f"raw-reports/{os.path.basename(local_path)}"
        self.uploads.append(fake_path)
        return fake_path
        
    def insert_report_metadata(self, run_id, report_type, file_path):
        print(f"  📊 Would insert metadata: {report_type}")
        self.reports.append({"run_id": run_id, "report_type": report_type, "file_path": file_path})
        return {"id": str(uuid.uuid4())}

def test_amazon_mtr():
    print("🧪 Testing Amazon MTR Agent...")
    file_path = "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    request = IngestionRequest(
        run_id=uuid.uuid4(),
        channel="amazon",
        gstin="06ABGCS4796R1ZA",
        month="2025-08",
        report_type="amazon_mtr",
        file_path=file_path
    )
    
    supabase = FakeSupabase()
    agent = AmazonMTRAgent()
    
    try:
        result_path = agent.process(request, supabase)
        print(f"  ✅ Success! Normalized and ready for upload")
        
        # Check the output
        normalized_files = [f for f in os.listdir("ingestion_layer/data/normalized") if f.startswith("amazon_mtr_")]
        if normalized_files:
            latest_file = f"ingestion_layer/data/normalized/{normalized_files[-1]}"
            df = pd.read_csv(latest_file)
            print(f"  📈 Output: {len(df)} rows, {len(df.columns)} columns")
            print(f"  📋 Columns: {list(df.columns)}")
            
            # Show sample data
            print(f"  📄 Sample data:")
            for i, row in df.head(2).iterrows():
                print(f"    Row {i+1}: {row['invoice_date']} | {row['order_id']} | {row['sku']} | ₹{row['taxable_value']}")
        
        return True
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def analyze_data_files():
    print("🔍 Analyzing available data files...")
    data_dir = "ingestion_layer/data"
    
    files = [f for f in os.listdir(data_dir) if f.endswith(('.xlsx', '.xls'))]
    
    for file in files:
        print(f"\n📁 {file}")
        try:
            df = pd.read_excel(os.path.join(data_dir, file))
            print(f"  📊 Shape: {df.shape}")
            print(f"  📋 Columns: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
        except Exception as e:
            print(f"  ❌ Error reading: {e}")

def main():
    print("🚀 Testing Multi-Agent Ingestion System")
    print("=" * 50)
    
    # Analyze all data files first
    analyze_data_files()
    
    print("\n" + "=" * 50)
    
    # Test Amazon MTR
    success = test_amazon_mtr()
    
    print("\n" + "=" * 50)
    print("📋 Summary:")
    print(f"  Amazon MTR: {'✅ PASS' if success else '❌ FAIL'}")
    
    if success:
        print("\n🎉 Ready for production! Next steps:")
        print("  1. Set up Supabase credentials in .env file")
        print("  2. Create Supabase tables and storage bucket")
        print("  3. Run with real Supabase:")
        print("     python -m ingestion_layer.main --agent amazon_mtr --input ingestion_layer/data/Amazon\\ MTR\\ B2C\\ Report\\ -\\ Sample.xlsx --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08")

if __name__ == "__main__":
    main()
