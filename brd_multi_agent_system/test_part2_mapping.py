#!/usr/bin/env python3
"""
Test script for Part-2: Item & Ledger Master Mapping
Tests the complete mapping workflow with real data from Part-1
"""
import sys
import os
import pandas as pd
sys.path.append('.')

from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.item_master_resolver import ItemMasterResolver
from ingestion_layer.agents.ledger_mapper import LedgerMapper
from ingestion_layer.agents.approval_agent import ApprovalAgent


class TestSupabaseForPart2(SupabaseClientWrapper):
    """Test Supabase client that simulates database operations."""
    
    def __init__(self):
        self.item_master = {}
        self.ledger_master = {}
        self.approvals = []
        self.client = None
        
        # Load some sample data
        self._load_sample_data()
    
    def _load_sample_data(self):
        """Load sample item and ledger master data."""
        # Sample item master data from your actual data
        sample_items = [
            ("LLQ-LAV-3L-FBA", "B0CZXQMSR5", "Liquid Lavender 3L"),
            ("FABCON-5L-FBA", "B09MZ2LBXB", "Fabric Conditioner 5L"),
            ("DW-5L", "B09P7P7P32", "Dishwash Liquid 5L"),
            ("SD-620A-K20G", "B0CZX5LX3X", "Surface Disinfectant"),
            ("KOPAROFABCON", "B093HFKP28", "Koparo Fabric Conditioner"),
            ("TOILETCLEANER", "B0D4NGD87J", "Toilet Cleaner")
        ]
        
        for sku, asin, fg in sample_items:
            self.item_master[(sku, asin)] = {
                "id": f"item_{len(self.item_master)}",
                "sku": sku,
                "asin": asin,
                "item_code": sku,
                "fg": fg,
                "gst_rate": 0.18,
                "approved_by": "system"
            }
        
        # Sample ledger master data
        sample_ledgers = [
            ("amazon", "ANDHRA PRADESH", "Amazon Sales - AP"),
            ("amazon", "KARNATAKA", "Amazon Sales - KA"),
            ("amazon", "DELHI", "Amazon Sales - DL"),
            ("amazon", "MAHARASHTRA", "Amazon Sales - MH"),
            ("amazon", "TELANGANA", "Amazon Sales - TG"),
            ("amazon", "PUNJAB", "Amazon Sales - PB"),
            ("amazon", "JAMMU & KASHMIR", "Amazon Sales - JK")
        ]
        
        for channel, state, ledger in sample_ledgers:
            self.ledger_master[(channel, state)] = {
                "id": f"ledger_{len(self.ledger_master)}",
                "channel": channel,
                "state_code": state,
                "ledger_name": ledger,
                "approved_by": "system"
            }
    
    def get_item_master(self, sku=None, asin=None):
        for (stored_sku, stored_asin), record in self.item_master.items():
            if (sku and stored_sku == sku) or (asin and stored_asin == asin):
                return record
        return None
    
    def get_ledger_master(self, channel, state_code):
        return self.ledger_master.get((channel.lower(), state_code.upper()))
    
    def insert_item_master(self, sku, asin, item_code, fg, gst_rate, approved_by="system"):
        record = {
            "id": f"item_{len(self.item_master)}",
            "sku": sku,
            "asin": asin,
            "item_code": item_code,
            "fg": fg,
            "gst_rate": gst_rate,
            "approved_by": approved_by
        }
        self.item_master[(sku, asin)] = record
        return record
    
    def insert_ledger_master(self, channel, state_code, ledger_name, approved_by="system"):
        record = {
            "id": f"ledger_{len(self.ledger_master)}",
            "channel": channel,
            "state_code": state_code,
            "ledger_name": ledger_name,
            "approved_by": approved_by
        }
        self.ledger_master[(channel.lower(), state_code.upper())] = record
        return record
    
    def insert_approval_request(self, approval_type, payload):
        record = {
            "id": f"approval_{len(self.approvals)}",
            "type": approval_type,
            "payload": payload,
            "status": "pending"
        }
        self.approvals.append(record)
        print(f"    📝 Created {approval_type} approval request: {payload}")
        return record
    
    def get_pending_approvals(self, approval_type=None):
        approvals = [a for a in self.approvals if a["status"] == "pending"]
        if approval_type:
            approvals = [a for a in approvals if a["type"] == approval_type]
        return approvals
    
    def approve_request(self, approval_id, approver):
        for approval in self.approvals:
            if approval["id"] == approval_id:
                approval["status"] = "approved"
                approval["approver"] = approver
                return approval
        return {}


def test_part2_with_real_data():
    """Test Part-2 mapping with real normalized data from Part-1."""
    
    print("🧪 TESTING PART-2: Item & Ledger Master Mapping")
    print("=" * 60)
    
    # Find the latest normalized file from Part-1
    normalized_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                       if f.startswith("amazon_mtr_") and f.endswith(".csv")]
    
    if not normalized_files:
        print("❌ No normalized files found. Run Part-1 first!")
        return False
    
    latest_file = f"ingestion_layer/data/normalized/{max(normalized_files)}"
    print(f"📁 Using normalized data: {os.path.basename(latest_file)}")
    
    # Load the normalized data
    df = pd.read_csv(latest_file)
    print(f"📊 Loaded {len(df)} records with {len(df.columns)} columns")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Initialize test components
    supabase = TestSupabaseForPart2()
    item_resolver = ItemMasterResolver(supabase)
    ledger_mapper = LedgerMapper(supabase)
    approval_agent = ApprovalAgent(supabase)
    
    print(f"\n🔧 Initialized components:")
    print(f"    📦 Item Master: {len(supabase.item_master)} pre-loaded items")
    print(f"    📋 Ledger Master: {len(supabase.ledger_master)} pre-loaded ledgers")
    
    # Step 1: Process Item Mappings
    print(f"\n🔍 STEP 1: Item Master Resolution")
    print("-" * 40)
    
    df_with_items, item_result = item_resolver.process_dataset(df.copy())
    item_stats = item_resolver.get_mapping_stats(df_with_items)
    
    print(f"📦 Item Mapping Results:")
    print(f"    Total items: {item_stats['total_items']}")
    print(f"    Mapped items: {item_stats['mapped_items']}")
    print(f"    Unmapped items: {item_stats['unmapped_items']}")
    print(f"    Coverage: {item_stats['coverage_pct']}%")
    print(f"    Pending approvals: {item_result.pending_approvals}")
    
    # Show sample mapped items
    mapped_items = df_with_items[df_with_items['item_resolved'] == True]
    if not mapped_items.empty:
        print(f"\n📄 Sample mapped items:")
        for i, row in mapped_items.head(3).iterrows():
            print(f"    {row['sku']} ({row['asin']}) → {row['fg']}")
    
    # Step 2: Process Ledger Mappings
    print(f"\n🔍 STEP 2: Ledger Master Resolution")
    print("-" * 40)
    
    df_with_ledgers, ledger_result = ledger_mapper.process_dataset(df_with_items)
    ledger_stats = ledger_mapper.get_mapping_stats(df_with_ledgers)
    
    print(f"📋 Ledger Mapping Results:")
    print(f"    Total records: {ledger_stats['total_records']}")
    print(f"    Mapped records: {ledger_stats['mapped_records']}")
    print(f"    Unmapped records: {ledger_stats['unmapped_records']}")
    print(f"    Coverage: {ledger_stats['coverage_pct']}%")
    print(f"    Pending approvals: {ledger_result.pending_approvals}")
    
    # Show sample mapped ledgers
    mapped_ledgers = df_with_ledgers[df_with_ledgers['ledger_resolved'] == True]
    if not mapped_ledgers.empty:
        print(f"\n📄 Sample mapped ledgers:")
        unique_ledgers = mapped_ledgers[['channel', 'state_code', 'ledger_name']].drop_duplicates()
        for i, row in unique_ledgers.head(3).iterrows():
            print(f"    {row['channel']} + {row['state_code']} → {row['ledger_name']}")
    
    # Step 3: Handle Approvals
    print(f"\n🔍 STEP 3: Approval Management")
    print("-" * 40)
    
    item_approvals = approval_agent.get_pending_approvals("item")
    ledger_approvals = approval_agent.get_pending_approvals("ledger")
    
    print(f"⏳ Pending Approvals:")
    print(f"    Item approvals: {len(item_approvals)}")
    print(f"    Ledger approvals: {len(ledger_approvals)}")
    
    if item_approvals or ledger_approvals:
        print(f"\n🚀 Auto-approving with suggested values...")
        
        item_approved = approval_agent.bulk_approve_items(item_approvals, "test_auto")
        ledger_approved = approval_agent.bulk_approve_ledgers(ledger_approvals, "test_auto")
        
        print(f"    ✅ Auto-approved {item_approved} item mappings")
        print(f"    ✅ Auto-approved {ledger_approved} ledger mappings")
        
        # Re-process with new mappings
        print(f"\n🔄 Re-processing with new mappings...")
        df_final, item_result_2 = item_resolver.process_dataset(df.copy())
        df_final, ledger_result_2 = ledger_mapper.process_dataset(df_final)
        
        final_item_stats = item_resolver.get_mapping_stats(df_final)
        final_ledger_stats = ledger_mapper.get_mapping_stats(df_final)
        
        print(f"📊 Final Coverage:")
        print(f"    Item coverage: {final_item_stats['coverage_pct']}%")
        print(f"    Ledger coverage: {final_ledger_stats['coverage_pct']}%")
    
    # Step 4: Save Enriched Dataset
    print(f"\n💾 STEP 4: Save Enriched Dataset")
    print("-" * 40)
    
    enriched_path = latest_file.replace('.csv', '_enriched.csv')
    df_with_ledgers.to_csv(enriched_path, index=False)
    
    print(f"💾 Enriched dataset saved: {os.path.basename(enriched_path)}")
    print(f"📊 Final dataset: {len(df_with_ledgers)} rows, {len(df_with_ledgers.columns)} columns")
    print(f"📋 New columns added: fg, item_resolved, ledger_name, ledger_resolved")
    
    # Summary
    print(f"\n" + "=" * 60)
    print("📋 PART-2 TEST SUMMARY")
    print("=" * 60)
    
    success = (item_result.success and ledger_result.success and 
               item_stats['coverage_pct'] > 0 and ledger_stats['coverage_pct'] > 0)
    
    print(f"Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Item Mapping: {item_stats['coverage_pct']}% coverage")
    print(f"Ledger Mapping: {ledger_stats['coverage_pct']}% coverage")
    print(f"Enriched File: {os.path.basename(enriched_path)}")
    
    if success:
        print(f"\n🎉 Part-2 is working correctly!")
        print(f"🚀 Ready for production deployment with:")
        print(f"   python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"     --enable-mapping")
    
    return success


def load_item_master_from_excel():
    """Load item master data from your Excel files."""
    
    print("\n📥 LOADING ITEM MASTER DATA")
    print("=" * 40)
    
    supabase = TestSupabaseForPart2()
    item_resolver = ItemMasterResolver(supabase)
    
    item_files = [
        "ingestion_layer/data/Item Master - Sample.xlsx",
        "ingestion_layer/data/Item Master Sample.xlsx"
    ]
    
    total_loaded = 0
    
    for file_path in item_files:
        if os.path.exists(file_path):
            try:
                print(f"📁 Loading: {os.path.basename(file_path)}")
                count = item_resolver.load_item_master_from_excel(file_path, "excel_import")
                total_loaded += count
                print(f"    ✅ Loaded {count} item mappings")
            except Exception as e:
                print(f"    ❌ Error loading {file_path}: {e}")
        else:
            print(f"    ⚠️  File not found: {file_path}")
    
    print(f"\n📊 Total item mappings loaded: {total_loaded}")
    return total_loaded


def main():
    print("🚀 PART-2 TESTING SUITE")
    print("Testing Item & Ledger Master Mapping functionality")
    print("=" * 60)
    
    # Test 1: Load item master data
    load_item_master_from_excel()
    
    # Test 2: Test mapping with real data
    success = test_part2_with_real_data()
    
    print(f"\n" + "=" * 60)
    print("🏁 FINAL RESULT")
    print("=" * 60)
    
    if success:
        print("✅ Part-2: Item & Ledger Master Mapping is WORKING!")
        print("🎯 Ready for production use")
    else:
        print("❌ Part-2 testing failed - needs debugging")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
