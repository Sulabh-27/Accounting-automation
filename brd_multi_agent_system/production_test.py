#!/usr/bin/env python3
"""
Production test script for the multi-agent ingestion system
Tests with real data and provides production-ready commands
"""
import sys
import os
import uuid
import pandas as pd
from typing import Dict, Optional
sys.path.append('.')

from ingestion_layer.libs.contracts import IngestionRequest
from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.universal_agent import UniversalAgent

class TestSupabase(SupabaseClientWrapper):
    """Test Supabase client that simulates uploads but shows what would happen in production."""
    
    def __init__(self):
        self.uploads = []
        self.reports = []
        self.runs = {}
        
    def upload_file(self, local_path, dest_path=None):
        filename = os.path.basename(local_path)
        storage_path = f"raw-reports/{filename}"
        self.uploads.append(storage_path)
        print(f"    📤 Would upload to Supabase: {storage_path}")
        return storage_path
        
    def insert_report_metadata(self, run_id, report_type, file_path):
        metadata = {
            "id": str(uuid.uuid4()),
            "run_id": str(run_id),
            "report_type": report_type,
            "file_path": file_path,
            "created_at": "2025-08-28T12:00:00Z"
        }
        self.reports.append(metadata)
        print(f"    📊 Would insert metadata: {report_type}")
        return metadata
        
    def insert_run_start(self, run_id, channel, gstin, month):
        run_data = {
            "run_id": str(run_id),
            "channel": channel,
            "gstin": gstin,
            "month": month,
            "status": "running",
            "started_at": "2025-08-28T12:00:00Z"
        }
        self.runs[str(run_id)] = run_data
        print(f"    🚀 Would start run: {run_id}")
        return run_data
        
    def update_run_finish(self, run_id, status="success"):
        if str(run_id) in self.runs:
            self.runs[str(run_id)]["status"] = status
            self.runs[str(run_id)]["finished_at"] = "2025-08-28T12:05:00Z"
        print(f"    ✅ Would finish run: {run_id} ({status})")
        return self.runs.get(str(run_id), {})

def load_asin_sku_mapping() -> Dict[str, str]:
    """Load ASIN to SKU mapping from Item Master files."""
    mapping = {}
    
    for filename in ["Item Master - Sample.xlsx", "Item Master Sample.xlsx"]:
        filepath = f"ingestion_layer/data/{filename}"
        if os.path.exists(filepath):
            try:
                df = pd.read_excel(filepath)
                # Look for ASIN and SKU columns
                asin_col = None
                sku_col = None
                
                for col in df.columns:
                    col_lower = col.lower()
                    if 'asin' in col_lower:
                        asin_col = col
                    elif 'sku' in col_lower:
                        sku_col = col
                
                if asin_col and sku_col:
                    temp_mapping = dict(zip(df[asin_col].dropna(), df[sku_col].dropna()))
                    mapping.update(temp_mapping)
                    print(f"    📋 Loaded {len(temp_mapping)} ASIN→SKU mappings from {filename}")
                    
            except Exception as e:
                print(f"    ⚠️  Could not load {filename}: {e}")
    
    return mapping

def test_agent(report_type: str, file_path: str, channel: str, gstin: str, month: str) -> bool:
    """Test a specific agent with real data."""
    
    if not os.path.exists(file_path):
        print(f"    ❌ File not found: {file_path}")
        return False
    
    print(f"\n🧪 Testing {report_type.upper()} Agent")
    print(f"    📁 File: {os.path.basename(file_path)}")
    
    # Create request
    run_id = uuid.uuid4()
    request = IngestionRequest(
        run_id=run_id,
        channel=channel,
        gstin=gstin,
        month=month,
        report_type=report_type,
        file_path=file_path
    )
    
    # Initialize components
    supabase = TestSupabase()
    agent = UniversalAgent()
    
    # Load ASIN mapping for Amazon STR
    asin_mapping = None
    if report_type == "amazon_str":
        asin_mapping = load_asin_sku_mapping()
    
    try:
        # Start run
        supabase.insert_run_start(run_id, channel, gstin, month)
        
        # Process file
        result_path = agent.process(request, supabase, asin_to_sku=asin_mapping)
        
        # Check output
        normalized_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                          if f.startswith(f"{report_type}_")]
        
        if normalized_files:
            latest_file = f"ingestion_layer/data/normalized/{normalized_files[-1]}"
            df = pd.read_csv(latest_file)
            
            print(f"    ✅ Success! Processed {len(df)} rows")
            print(f"    📊 Output columns: {list(df.columns)}")
            print(f"    💰 Total taxable value: ₹{df['taxable_value'].sum():,.2f}")
            print(f"    📈 Sample data:")
            
            for i, row in df.head(2).iterrows():
                print(f"      Row {i+1}: {row['invoice_date']} | {row['order_id']} | {row['sku']} | ₹{row['taxable_value']}")
        
        # Finish run
        supabase.update_run_finish(run_id, "success")
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error: {e}")
        supabase.update_run_finish(run_id, "failed")
        import traceback
        traceback.print_exc()
        return False

def generate_production_commands(successes: Dict[str, bool]):
    """Generate production-ready commands for successful tests."""
    
    print("\n" + "="*60)
    print("🚀 PRODUCTION COMMANDS")
    print("="*60)
    
    if successes.get("amazon_mtr"):
        print("\n📦 Amazon MTR:")
        print('python -m ingestion_layer.main --agent amazon_mtr \\')
        print('  --input "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx" \\')
        print('  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08')
    
    print("\n📋 Prerequisites for production:")
    print("1. Copy ingestion_layer/.env.example to ingestion_layer/.env")
    print("2. Fill in your Supabase credentials in .env file")
    print("3. Create Supabase tables:")
    print("   - runs (run_id, channel, gstin, month, status, started_at, finished_at)")
    print("   - reports (id, run_id, report_type, file_path, hash, created_at)")
    print("4. Create Supabase storage bucket: 'multi-agent-system'")

def main():
    print("🚀 PRODUCTION TEST - Multi-Agent Ingestion System")
    print("="*60)
    
    # Test cases based on available data
    test_cases = [
        {
            "report_type": "amazon_mtr",
            "file_path": "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx",
            "channel": "amazon",
            "gstin": "06ABGCS4796R1ZA",
            "month": "2025-08"
        }
    ]
    
    successes = {}
    
    for test_case in test_cases:
        success = test_agent(**test_case)
        successes[test_case["report_type"]] = success
    
    # Summary
    print("\n" + "="*60)
    print("📋 TEST SUMMARY")
    print("="*60)
    
    total_tests = len(test_cases)
    passed_tests = sum(successes.values())
    
    for report_type, success in successes.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {report_type.upper()}: {status}")
    
    print(f"\nOverall: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests > 0:
        generate_production_commands(successes)
        print(f"\n🎉 System ready for production with {passed_tests} working agent(s)!")
    else:
        print("\n⚠️  No agents passed testing. Please check the data files and mappings.")

if __name__ == "__main__":
    main()
