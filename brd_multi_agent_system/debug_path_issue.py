#!/usr/bin/env python3
"""
Debug script to identify the path issue causing "daata" instead of "data"
"""
import os
import sys
sys.path.insert(0, '.')

from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.amazon_mtr_agent import AmazonMTRAgent
from ingestion_layer.libs.contracts import IngestionRequest
import uuid

print("🔍 DEBUGGING PATH ISSUE")
print("=" * 50)

# Test 1: Direct path construction
print("\n1. Testing direct path construction:")
file_path = "ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx"
print(f"   Original file_path: {repr(file_path)}")
print(f"   Dirname: {repr(os.path.dirname(file_path))}")
out_dir = os.path.join(os.path.dirname(file_path), "normalized")
print(f"   Output dir: {repr(out_dir)}")
out_path = os.path.join(out_dir, "test.csv")
print(f"   Output path: {repr(out_path)}")
            print(f"    Output: {result_path}")
            print(f"    File exists at output path: {os.path.exists(result_path)}")
        else:
            print(f"    ❌ Test file not found: {test_file}")
    except Exception as e:
        print(f"    ❌ Upload test failed: {e}")
    
    # Test 4: Check what's in the database
    print(f"\n🗄️  Database Records Check:")
    try:
        # Check recent runs
        print("    Recent runs in database:")
        # This would require actual database query - skipping for now
        print("    (Database query would go here)")
    except Exception as e:
        print(f"    ❌ Database check failed: {e}")
    
    # Test 5: Check data directory
    print(f"\n📁 Data Directory Check:")
    data_dir = "ingestion_layer/data"
    if os.path.exists(data_dir):
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        print(f"    CSV files in {data_dir}: {len(files)}")
        for f in files[:5]:  # Show first 5
            print(f"        - {f}")
        if len(files) > 5:
            print(f"        ... and {len(files) - 5} more")
    else:
        print(f"    ❌ Data directory not found: {data_dir}")
    
    print(f"\n" + "=" * 60)
    print("🎯 ANALYSIS:")
    print("The issue is that when Supabase client is None (no connection),")
    print("the upload_file method was returning fake Supabase paths like:")
    print("'Zaggle/[uuid]/filename.csv' instead of local paths.")
    print("")
    print("These fake paths get stored in the database, but the files")
    print("don't actually exist at those locations locally.")
    print("")
    print("✅ FIXED: Modified upload_file to return local paths when client is None")
    print("❌ REMAINING: Need to clean up old database records with fake paths")


def main():
    debug_supabase_paths()
    return 0


if __name__ == "__main__":
    sys.exit(main())
