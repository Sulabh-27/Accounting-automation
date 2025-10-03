#!/usr/bin/env python3
"""
Complete demonstration of Part-2: Item & Ledger Master Mapping
Shows the full workflow from Part-1 normalized data to enriched output
"""
import sys
import os
import pandas as pd
sys.path.append('.')

from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.item_master_resolver import ItemMasterResolver
from ingestion_layer.agents.ledger_mapper import LedgerMapper
from ingestion_layer.agents.approval_agent import ApprovalAgent


class DemoSupabase(SupabaseClientWrapper):
    """Demo Supabase client with realistic data."""
    
    def __init__(self):
        self.item_master = {}
        self.ledger_master = {}
        self.approvals = []
        self.client = None
        
        # Load realistic sample data based on your actual data
        self._load_realistic_data()
    
    def _load_realistic_data(self):
        """Load realistic item and ledger data."""
        
        # Real SKUs from your Amazon MTR data
        real_items = [
            ("LLQ-LAV-3L-FBA", "B0CZXQMSR5", "Liquid Lavender 3L", 0.18),
            ("FABCON-5L-FBA", "B09MZ2LBXB", "Fabric Conditioner 5L", 0.18),
            ("90-X8YV-Q3DM", "B0CZXQMSR5", "Liquid Lavender 3L", 0.00),  # 0% GST item
            ("DW-5L", "B09P7P7P32", "Dishwash Liquid 5L", 0.18),
            ("SD-620A-K20G", "B0CZX5LX3X", "Surface Disinfectant", 0.18),
            ("FABCON-5L", "B09MZ2LBXB", "Fabric Conditioner 5L", 0.00),  # 0% GST variant
            ("KOPAROFABCON", "B093HFKP28", "Koparo Fabric Conditioner", 0.18),
            ("TOILETCLEANER", "B0D4NGD87J", "Toilet Cleaner", 0.18)
        ]
        
        for sku, asin, fg, gst_rate in real_items:
            self.item_master[(sku, asin)] = {
                "id": f"item_{len(self.item_master)}",
                "sku": sku,
                "asin": asin,
                "item_code": sku,
                "fg": fg,
                "gst_rate": gst_rate,
                "approved_by": "system"
            }
        
        # Real states from your Amazon MTR data
        real_ledgers = [
            ("amazon", "ANDHRA PRADESH", "Amazon Sales - AP"),
            ("amazon", "KARNATAKA", "Amazon Sales - KA"),
            ("amazon", "DELHI", "Amazon Sales - DL"),
            ("amazon", "JAMMU & KASHMIR", "Amazon Sales - JK"),
            ("amazon", "TELANGANA", "Amazon Sales - TG"),
            ("amazon", "MAHARASHTRA", "Amazon Sales - MH"),
            ("amazon", "PUNJAB", "Amazon Sales - PB"),
            ("amazon", "WEST BENGAL", "Amazon Sales - WB"),
            ("amazon", "UTTAR PRADESH", "Amazon Sales - UP"),
            ("amazon", "RAJASTHAN", "Amazon Sales - RJ")
        ]
        
        for channel, state, ledger in real_ledgers:
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
        print(f"    ✅ Added item mapping: {sku} → {fg}")
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
        print(f"    ✅ Added ledger mapping: {channel} + {state_code} → {ledger_name}")
        return record
    
    def insert_approval_request(self, approval_type, payload):
        record = {
            "id": f"approval_{len(self.approvals)}",
            "type": approval_type,
            "payload": payload,
            "status": "pending"
        }
        self.approvals.append(record)
        
        if approval_type == "item":
            print(f"    📝 Item approval needed: {payload.get('sku', '')} → {payload.get('suggested_fg', '')}")
        else:
            print(f"    📝 Ledger approval needed: {payload.get('channel', '')} + {payload.get('state_code', '')} → {payload.get('suggested_ledger_name', '')}")
        
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


def main():
    print("🎯 PART-2 COMPLETE DEMONSTRATION")
    print("Item & Ledger Master Mapping with Real Data")
    print("=" * 60)
    
    # Find the latest normalized file
    normalized_files = [f for f in os.listdir("ingestion_layer/data/normalized") 
                       if f.startswith("amazon_mtr_") and f.endswith(".csv")]
    
    if not normalized_files:
        print("❌ No normalized files found. Please run Part-1 first:")
        print("   python final_production_test.py")
        return 1
    
    latest_file = f"ingestion_layer/data/normalized/{max(normalized_files)}"
    print(f"📁 Input: {os.path.basename(latest_file)}")
    
    # Load normalized data
    df = pd.read_csv(latest_file)
    print(f"📊 Loaded: {len(df)} transactions")
    print(f"📦 Unique SKUs: {df['sku'].nunique()}")
    print(f"🗺️  Unique States: {df['state_code'].nunique()}")
    
    # Initialize components
    supabase = DemoSupabase()
    item_resolver = ItemMasterResolver(supabase)
    ledger_mapper = LedgerMapper(supabase)
    approval_agent = ApprovalAgent(supabase)
    
    print(f"\n🔧 Master Data Loaded:")
    print(f"    📦 Item Master: {len(supabase.item_master)} mappings")
    print(f"    📋 Ledger Master: {len(supabase.ledger_master)} mappings")
    
    # STEP 1: Item Master Resolution
    print(f"\n🔍 STEP 1: Item Master Resolution")
    print("-" * 40)
    
    df_items, item_result = item_resolver.process_dataset(df.copy())
    item_stats = item_resolver.get_mapping_stats(df_items)
    
    print(f"📦 Results:")
    print(f"    ✅ Mapped: {item_stats['mapped_items']}/{item_stats['total_items']} ({item_stats['coverage_pct']}%)")
    print(f"    ⏳ Pending: {item_result.pending_approvals} approvals")
    
    # Show some mapped items
    mapped_sample = df_items[df_items['item_resolved'] == True][['sku', 'asin', 'fg']].drop_duplicates().head(3)
    if not mapped_sample.empty:
        print(f"    📄 Sample mappings:")
        for _, row in mapped_sample.iterrows():
            print(f"       {row['sku']} → {row['fg']}")
    
    # STEP 2: Ledger Master Resolution
    print(f"\n🔍 STEP 2: Ledger Master Resolution")
    print("-" * 40)
    
    df_ledgers, ledger_result = ledger_mapper.process_dataset(df_items)
    ledger_stats = ledger_mapper.get_mapping_stats(df_ledgers)
    
    print(f"📋 Results:")
    print(f"    ✅ Mapped: {ledger_stats['mapped_records']}/{ledger_stats['total_records']} ({ledger_stats['coverage_pct']}%)")
    print(f"    ⏳ Pending: {ledger_result.pending_approvals} approvals")
    
    # Show some mapped ledgers
    ledger_sample = df_ledgers[df_ledgers['ledger_resolved'] == True][['channel', 'state_code', 'ledger_name']].drop_duplicates().head(3)
    if not ledger_sample.empty:
        print(f"    📄 Sample mappings:")
        for _, row in ledger_sample.iterrows():
            print(f"       {row['channel']} + {row['state_code']} → {row['ledger_name']}")
    
    # STEP 3: Handle Approvals
    total_pending = item_result.pending_approvals + ledger_result.pending_approvals
    
    if total_pending > 0:
        print(f"\n🔍 STEP 3: Approval Processing")
        print("-" * 40)
        
        print(f"⏳ Found {total_pending} pending approvals")
        
        # Auto-approve for demo
        item_approvals = approval_agent.get_pending_approvals("item")
        ledger_approvals = approval_agent.get_pending_approvals("ledger")
        
        item_approved = approval_agent.bulk_approve_items(item_approvals, "demo_auto")
        ledger_approved = approval_agent.bulk_approve_ledgers(ledger_approvals, "demo_auto")
        
        print(f"🚀 Auto-approved {item_approved + ledger_approved} mappings")
        
        # Re-process with new mappings
        print(f"🔄 Re-processing with approved mappings...")
        df_final, _ = item_resolver.process_dataset(df.copy())
        df_final, _ = ledger_mapper.process_dataset(df_final)
        
        final_item_stats = item_resolver.get_mapping_stats(df_final)
        final_ledger_stats = ledger_mapper.get_mapping_stats(df_final)
        
        print(f"📊 Final coverage:")
        print(f"    📦 Items: {final_item_stats['coverage_pct']}%")
        print(f"    📋 Ledgers: {final_ledger_stats['coverage_pct']}%")
        
        df_ledgers = df_final  # Use the final enriched dataset
    
    # STEP 4: Save Enriched Dataset
    print(f"\n💾 STEP 4: Save Enriched Dataset")
    print("-" * 40)
    
    enriched_path = latest_file.replace('.csv', '_enriched.csv')
    df_ledgers.to_csv(enriched_path, index=False)
    
    print(f"💾 Saved: {os.path.basename(enriched_path)}")
    print(f"📊 Final dataset: {len(df_ledgers)} rows × {len(df_ledgers.columns)} columns")
    
    # Show final enriched sample
    print(f"\n📄 Sample Enriched Data:")
    sample_cols = ['invoice_date', 'sku', 'fg', 'state_code', 'ledger_name', 'taxable_value']
    available_cols = [col for col in sample_cols if col in df_ledgers.columns]
    sample_data = df_ledgers[available_cols].head(3)
    
    for i, row in sample_data.iterrows():
        print(f"    Row {i+1}: {row['invoice_date'][:10]} | {row['sku']} → {row.get('fg', 'N/A')} | {row['state_code']} → {row.get('ledger_name', 'N/A')} | ₹{row['taxable_value']}")
    
    # FINAL SUMMARY
    print(f"\n" + "=" * 60)
    print("🎉 PART-2 DEMONSTRATION COMPLETE!")
    print("=" * 60)
    
    final_item_stats = item_resolver.get_mapping_stats(df_ledgers)
    final_ledger_stats = ledger_mapper.get_mapping_stats(df_ledgers)
    
    print(f"✅ Status: SUCCESS")
    print(f"📦 Item Coverage: {final_item_stats['coverage_pct']}%")
    print(f"📋 Ledger Coverage: {final_ledger_stats['coverage_pct']}%")
    print(f"💾 Output: {os.path.basename(enriched_path)}")
    print(f"📊 Total Value Processed: ₹{df_ledgers['taxable_value'].sum():,.2f}")
    
    print(f"\n🚀 PRODUCTION READY COMMANDS:")
    print(f"# Run with mapping enabled:")
    print(f"python -m ingestion_layer.main \\")
    print(f"  --agent amazon_mtr \\")
    print(f"  --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
    print(f"  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
    print(f"  --enable-mapping")
    
    print(f"\n# Manage approvals:")
    print(f"python -m ingestion_layer.approval_cli --interactive")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
