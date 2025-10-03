import os
import tempfile
import unittest
import uuid
import pandas as pd
from unittest.mock import MagicMock

from ingestion_layer.libs.contracts import ItemMappingRequest, LedgerMappingRequest, MappingResult
from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.item_master_resolver import ItemMasterResolver
from ingestion_layer.agents.ledger_mapper import LedgerMapper
from ingestion_layer.agents.approval_agent import ApprovalAgent


class FakeSupabaseForMapping(SupabaseClientWrapper):
    """Fake Supabase client for testing mapping functionality."""
    
    def __init__(self):
        self.item_master = {
            # Pre-existing mappings
            ("LLQ-LAV-3L-FBA", "B0CZXQMSR5"): {
                "id": str(uuid.uuid4()),
                "sku": "LLQ-LAV-3L-FBA",
                "asin": "B0CZXQMSR5",
                "item_code": "LLQ001",
                "fg": "Liquid Lavender 3L",
                "gst_rate": 0.18,
                "approved_by": "system"
            },
            ("FABCON-5L-FBA", "B09MZ2LBXB"): {
                "id": str(uuid.uuid4()),
                "sku": "FABCON-5L-FBA",
                "asin": "B09MZ2LBXB",
                "item_code": "FAB001",
                "fg": "Fabric Conditioner 5L",
                "gst_rate": 0.18,
                "approved_by": "system"
            }
        }
        
        self.ledger_master = {
            # Pre-existing mappings
            ("amazon", "ANDHRA PRADESH"): {
                "id": str(uuid.uuid4()),
                "channel": "amazon",
                "state_code": "ANDHRA PRADESH",
                "ledger_name": "Amazon Sales - AP",
                "approved_by": "system"
            },
            ("amazon", "KARNATAKA"): {
                "id": str(uuid.uuid4()),
                "channel": "amazon",
                "state_code": "KARNATAKA",
                "ledger_name": "Amazon Sales - KA",
                "approved_by": "system"
            }
        }
        
        self.approvals = []
        self.client = None
    
    def get_item_master(self, sku=None, asin=None):
        """Get item master record by SKU or ASIN."""
        for (stored_sku, stored_asin), record in self.item_master.items():
            if (sku and stored_sku == sku) or (asin and stored_asin == asin):
                return record
        return None
    
    def get_ledger_master(self, channel, state_code):
        """Get ledger master record by channel and state."""
        key = (channel.lower(), state_code.upper())
        return self.ledger_master.get(key)
    
    def insert_item_master(self, sku, asin, item_code, fg, gst_rate, approved_by="system"):
        """Insert new item master record."""
        record = {
            "id": str(uuid.uuid4()),
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
        """Insert new ledger master record."""
        record = {
            "id": str(uuid.uuid4()),
            "channel": channel,
            "state_code": state_code,
            "ledger_name": ledger_name,
            "approved_by": approved_by
        }
        self.ledger_master[(channel.lower(), state_code.upper())] = record
        return record
    
    def insert_approval_request(self, approval_type, payload):
        """Insert approval request."""
        record = {
            "id": str(uuid.uuid4()),
            "type": approval_type,
            "payload": payload,
            "status": "pending"
        }
        self.approvals.append(record)
        return record
    
    def get_pending_approvals(self, approval_type=None):
        """Get pending approval requests."""
        approvals = [a for a in self.approvals if a["status"] == "pending"]
        if approval_type:
            approvals = [a for a in approvals if a["type"] == approval_type]
        return approvals
    
    def approve_request(self, approval_id, approver):
        """Approve a request."""
        for approval in self.approvals:
            if approval["id"] == approval_id:
                approval["status"] = "approved"
                approval["approver"] = approver
                return approval
        return {}


class TestItemMasterResolver(unittest.TestCase):
    def setUp(self):
        self.supabase = FakeSupabaseForMapping()
        self.resolver = ItemMasterResolver(self.supabase)

    def test_resolve_known_item_by_sku(self):
        """Test resolving a known item by SKU."""
        fg_name, is_resolved = self.resolver.resolve_item_mapping("LLQ-LAV-3L-FBA")
        self.assertTrue(is_resolved)
        self.assertEqual(fg_name, "Liquid Lavender 3L")

    def test_resolve_known_item_by_asin(self):
        """Test resolving a known item by ASIN."""
        fg_name, is_resolved = self.resolver.resolve_item_mapping("", "B09MZ2LBXB")
        self.assertTrue(is_resolved)
        self.assertEqual(fg_name, "Fabric Conditioner 5L")

    def test_resolve_unknown_item(self):
        """Test resolving an unknown item."""
        fg_name, is_resolved = self.resolver.resolve_item_mapping("UNKNOWN-SKU", "UNKNOWN-ASIN")
        self.assertFalse(is_resolved)
        self.assertEqual(fg_name, "")

    def test_process_dataset_with_known_items(self):
        """Test processing dataset with known items."""
        df = pd.DataFrame({
            "sku": ["LLQ-LAV-3L-FBA", "FABCON-5L-FBA"],
            "asin": ["B0CZXQMSR5", "B09MZ2LBXB"],
            "quantity": [1, 2],
            "taxable_value": [449.0, 1059.0]
        })
        
        enriched_df, result = self.resolver.process_dataset(df)
        
        self.assertTrue(result.success)
        self.assertEqual(result.mapped_count, 2)
        self.assertEqual(result.pending_approvals, 0)
        
        # Check enriched data
        self.assertIn("fg", enriched_df.columns)
        self.assertIn("item_resolved", enriched_df.columns)
        self.assertEqual(enriched_df.loc[0, "fg"], "Liquid Lavender 3L")
        self.assertEqual(enriched_df.loc[1, "fg"], "Fabric Conditioner 5L")
        self.assertTrue(all(enriched_df["item_resolved"]))

    def test_process_dataset_with_unknown_items(self):
        """Test processing dataset with unknown items."""
        df = pd.DataFrame({
            "sku": ["UNKNOWN-SKU1", "UNKNOWN-SKU2"],
            "asin": ["UNKNOWN-ASIN1", "UNKNOWN-ASIN2"],
            "quantity": [1, 1],
            "taxable_value": [100.0, 200.0]
        })
        
        enriched_df, result = self.resolver.process_dataset(df)
        
        self.assertTrue(result.success)
        self.assertEqual(result.mapped_count, 0)
        self.assertEqual(result.pending_approvals, 2)
        
        # Check that approval requests were created
        approvals = self.supabase.get_pending_approvals("item")
        self.assertEqual(len(approvals), 2)

    def test_get_mapping_stats(self):
        """Test getting mapping statistics."""
        df = pd.DataFrame({
            "item_resolved": [True, True, False, False],
            "fg": ["Item1", "Item2", "", ""]
        })
        
        stats = self.resolver.get_mapping_stats(df)
        
        self.assertEqual(stats["total_items"], 4)
        self.assertEqual(stats["mapped_items"], 2)
        self.assertEqual(stats["unmapped_items"], 2)
        self.assertEqual(stats["coverage_pct"], 50)


class TestLedgerMapper(unittest.TestCase):
    def setUp(self):
        self.supabase = FakeSupabaseForMapping()
        self.mapper = LedgerMapper(self.supabase)

    def test_resolve_known_ledger(self):
        """Test resolving a known ledger mapping."""
        ledger_name, is_resolved = self.mapper.resolve_ledger_mapping("amazon", "ANDHRA PRADESH")
        self.assertTrue(is_resolved)
        self.assertEqual(ledger_name, "Amazon Sales - AP")

    def test_resolve_unknown_ledger(self):
        """Test resolving an unknown ledger mapping."""
        ledger_name, is_resolved = self.mapper.resolve_ledger_mapping("flipkart", "DELHI")
        self.assertFalse(is_resolved)
        self.assertEqual(ledger_name, "")

    def test_process_dataset_with_known_ledgers(self):
        """Test processing dataset with known ledger mappings."""
        df = pd.DataFrame({
            "channel": ["amazon", "amazon"],
            "state_code": ["ANDHRA PRADESH", "KARNATAKA"],
            "taxable_value": [449.0, 1059.0]
        })
        
        enriched_df, result = self.mapper.process_dataset(df)
        
        self.assertTrue(result.success)
        self.assertEqual(result.mapped_count, 2)
        self.assertEqual(result.pending_approvals, 0)
        
        # Check enriched data
        self.assertIn("ledger_name", enriched_df.columns)
        self.assertIn("ledger_resolved", enriched_df.columns)
        self.assertEqual(enriched_df.loc[0, "ledger_name"], "Amazon Sales - AP")
        self.assertEqual(enriched_df.loc[1, "ledger_name"], "Amazon Sales - KA")
        self.assertTrue(all(enriched_df["ledger_resolved"]))

    def test_process_dataset_with_unknown_ledgers(self):
        """Test processing dataset with unknown ledger mappings."""
        df = pd.DataFrame({
            "channel": ["flipkart", "pepperfry"],
            "state_code": ["DELHI", "MUMBAI"],
            "taxable_value": [100.0, 200.0]
        })
        
        enriched_df, result = self.mapper.process_dataset(df)
        
        self.assertTrue(result.success)
        self.assertEqual(result.mapped_count, 0)
        self.assertEqual(result.pending_approvals, 2)
        
        # Check that approval requests were created
        approvals = self.supabase.get_pending_approvals("ledger")
        self.assertEqual(len(approvals), 2)

    def test_state_abbreviation_mapping(self):
        """Test state abbreviation mapping."""
        self.assertEqual(self.mapper._get_state_abbreviation("ANDHRA PRADESH"), "AP")
        self.assertEqual(self.mapper._get_state_abbreviation("KARNATAKA"), "KA")
        self.assertEqual(self.mapper._get_state_abbreviation("UNKNOWN STATE"), "UN")

    def test_generate_default_ledgers(self):
        """Test generating default ledger mappings."""
        channels = ["flipkart", "pepperfry"]
        states = ["DELHI", "MUMBAI"]
        
        created_count = self.mapper.generate_default_ledgers(channels, states, "test")
        
        self.assertEqual(created_count, 4)  # 2 channels × 2 states
        
        # Verify mappings were created
        self.assertIsNotNone(self.supabase.get_ledger_master("flipkart", "DELHI"))
        self.assertIsNotNone(self.supabase.get_ledger_master("pepperfry", "MUMBAI"))


class TestApprovalAgent(unittest.TestCase):
    def setUp(self):
        self.supabase = FakeSupabaseForMapping()
        self.agent = ApprovalAgent(self.supabase)
        
        # Add some test approval requests
        self.supabase.insert_approval_request("item", {
            "sku": "TEST-SKU",
            "asin": "TEST-ASIN",
            "suggested_fg": "Test Product",
            "gst_rate": 0.18
        })
        
        self.supabase.insert_approval_request("ledger", {
            "channel": "test",
            "state_code": "TEST STATE",
            "suggested_ledger_name": "Test Sales - TS"
        })

    def test_get_pending_approvals(self):
        """Test getting pending approvals."""
        all_approvals = self.agent.get_pending_approvals()
        self.assertEqual(len(all_approvals), 2)
        
        item_approvals = self.agent.get_pending_approvals("item")
        self.assertEqual(len(item_approvals), 1)
        
        ledger_approvals = self.agent.get_pending_approvals("ledger")
        self.assertEqual(len(ledger_approvals), 1)

    def test_approve_item_mapping(self):
        """Test approving an item mapping."""
        approvals = self.agent.get_pending_approvals("item")
        approval_id = approvals[0]["id"]
        
        success = self.agent.approve_item_mapping(
            approval_id=approval_id,
            fg_name="Approved Test Product",
            gst_rate=0.18,
            approver="test"
        )
        
        self.assertTrue(success)
        
        # Check that item master record was created
        item_record = self.supabase.get_item_master(sku="TEST-SKU")
        self.assertIsNotNone(item_record)
        self.assertEqual(item_record["fg"], "Approved Test Product")

    def test_approve_ledger_mapping(self):
        """Test approving a ledger mapping."""
        approvals = self.agent.get_pending_approvals("ledger")
        approval_id = approvals[0]["id"]
        
        success = self.agent.approve_ledger_mapping(
            approval_id=approval_id,
            ledger_name="Approved Test Ledger",
            approver="test"
        )
        
        self.assertTrue(success)
        
        # Check that ledger master record was created
        ledger_record = self.supabase.get_ledger_master("test", "TEST STATE")
        self.assertIsNotNone(ledger_record)
        self.assertEqual(ledger_record["ledger_name"], "Approved Test Ledger")

    def test_bulk_approve_items(self):
        """Test bulk approving item mappings."""
        approvals = self.agent.get_pending_approvals("item")
        approved_count = self.agent.bulk_approve_items(approvals, "bulk_test")
        
        self.assertEqual(approved_count, 1)
        
        # Check that item was approved with suggested value
        item_record = self.supabase.get_item_master(sku="TEST-SKU")
        self.assertIsNotNone(item_record)
        self.assertEqual(item_record["fg"], "Test Product")

    def test_bulk_approve_ledgers(self):
        """Test bulk approving ledger mappings."""
        approvals = self.agent.get_pending_approvals("ledger")
        approved_count = self.agent.bulk_approve_ledgers(approvals, "bulk_test")
        
        self.assertEqual(approved_count, 1)
        
        # Check that ledger was approved with suggested value
        ledger_record = self.supabase.get_ledger_master("test", "TEST STATE")
        self.assertIsNotNone(ledger_record)
        self.assertEqual(ledger_record["ledger_name"], "Test Sales - TS")


class TestIntegratedMappingWorkflow(unittest.TestCase):
    """Test the complete mapping workflow end-to-end."""
    
    def setUp(self):
        self.supabase = FakeSupabaseForMapping()
        self.item_resolver = ItemMasterResolver(self.supabase)
        self.ledger_mapper = LedgerMapper(self.supabase)
        self.approval_agent = ApprovalAgent(self.supabase)

    def test_complete_mapping_workflow(self):
        """Test complete workflow: ingestion → item mapping → ledger mapping → approval."""
        
        # Step 1: Create sample normalized data (mix of known and unknown items/ledgers)
        df = pd.DataFrame({
            "invoice_date": ["2025-08-01", "2025-08-02", "2025-08-03"],
            "sku": ["LLQ-LAV-3L-FBA", "UNKNOWN-SKU", "FABCON-5L-FBA"],
            "asin": ["B0CZXQMSR5", "UNKNOWN-ASIN", "B09MZ2LBXB"],
            "channel": ["amazon", "flipkart", "amazon"],
            "state_code": ["ANDHRA PRADESH", "DELHI", "UNKNOWN STATE"],
            "quantity": [1, 2, 1],
            "taxable_value": [449.0, 200.0, 1059.0]
        })
        
        # Step 2: Process item mappings
        df, item_result = self.item_resolver.process_dataset(df)
        
        # Should resolve 2 known items, 1 unknown
        self.assertEqual(item_result.mapped_count, 2)
        self.assertEqual(item_result.pending_approvals, 1)
        
        # Step 3: Process ledger mappings
        df, ledger_result = self.ledger_mapper.process_dataset(df)
        
        # Should resolve 1 known ledger, 2 unknown
        self.assertEqual(ledger_result.mapped_count, 1)
        self.assertEqual(ledger_result.pending_approvals, 2)
        
        # Step 4: Check that approval requests were created
        item_approvals = self.approval_agent.get_pending_approvals("item")
        ledger_approvals = self.approval_agent.get_pending_approvals("ledger")
        
        self.assertEqual(len(item_approvals), 1)
        self.assertEqual(len(ledger_approvals), 2)
        
        # Step 5: Bulk approve all pending requests
        item_approved = self.approval_agent.bulk_approve_items(item_approvals, "workflow_test")
        ledger_approved = self.approval_agent.bulk_approve_ledgers(ledger_approvals, "workflow_test")
        
        self.assertEqual(item_approved, 1)
        self.assertEqual(ledger_approved, 2)
        
        # Step 6: Re-process dataset - should now have 100% mapping coverage
        df_reprocessed = df.copy()
        df_reprocessed, item_result_2 = self.item_resolver.process_dataset(df_reprocessed)
        df_reprocessed, ledger_result_2 = self.ledger_mapper.process_dataset(df_reprocessed)
        
        # Should now resolve all items and ledgers
        item_stats = self.item_resolver.get_mapping_stats(df_reprocessed)
        ledger_stats = self.ledger_mapper.get_mapping_stats(df_reprocessed)
        
        self.assertEqual(item_stats["coverage_pct"], 100)
        self.assertEqual(ledger_stats["coverage_pct"], 100)
        self.assertEqual(item_result_2.pending_approvals, 0)
        self.assertEqual(ledger_result_2.pending_approvals, 0)


if __name__ == "__main__":
    unittest.main()
