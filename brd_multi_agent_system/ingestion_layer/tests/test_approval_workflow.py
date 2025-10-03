"""
Tests for Approval Workflow Agent (Part 7)
"""
import unittest
import uuid
import json
from datetime import datetime
from unittest.mock import Mock, patch

from ..agents.approval_workflow import ApprovalWorkflowAgent, ApprovalRequest, ApprovalResult


class TestApprovalWorkflowAgent(unittest.TestCase):
    """Test cases for Approval Workflow Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = Mock()
        self.agent = ApprovalWorkflowAgent(self.mock_supabase)
        self.run_id = uuid.uuid4()
    
    def test_create_approval_request(self):
        """Test creating an approval request."""
        payload = {
            'sku': 'ABC123',
            'asin': 'B08N5WRWNW',
            'channel': 'amazon',
            'final_goods_name': None
        }
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        request_id = self.agent.create_approval_request(
            request_type='item_mapping',
            payload=payload,
            run_id=self.run_id,
            priority='medium'
        )
        
        self.assertIsInstance(request_id, str)
        self.mock_supabase.client.table.assert_called_with('approval_queue')
    
    def test_auto_approval_item_mapping(self):
        """Test auto-approval for item mapping with similar patterns."""
        payload = {
            'sku': 'ABC123',  # Should match ABC pattern
            'asin': 'B08N5WRWNW',
            'channel': 'amazon',
            'estimated_value': 1000
        }
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        request_id = self.agent.create_approval_request(
            request_type='item_mapping',
            payload=payload,
            run_id=self.run_id
        )
        
        # Should be auto-approved due to ABC pattern
        self.assertIsInstance(request_id, str)
    
    def test_auto_approval_ledger_mapping(self):
        """Test auto-approval for standard ledger mapping."""
        payload = {
            'channel': 'amazon',
            'state_code': 'HR'
        }
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        request_id = self.agent.create_approval_request(
            request_type='ledger_mapping',
            payload=payload,
            run_id=self.run_id
        )
        
        # Should be auto-approved for standard channel and state
        self.assertIsInstance(request_id, str)
    
    def test_manual_approval_required_gst_rate(self):
        """Test that GST rate overrides require manual approval."""
        payload = {
            'current_rate': 0.18,
            'proposed_rate': 0.12,
            'reason': 'Product category change'
        }
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        with patch('ingestion_layer.libs.notification_utils.notify_approval_request') as mock_notify:
            request_id = self.agent.create_approval_request(
                request_type='gst_rate_override',
                payload=payload,
                run_id=self.run_id,
                priority='high'
            )
            
            # Should require manual approval
            self.assertIsInstance(request_id, str)
            mock_notify.assert_called_once()
    
    def test_process_approval_decision_approved(self):
        """Test processing an approved decision."""
        request_id = str(uuid.uuid4())
        
        # Mock database update
        mock_request_data = {
            'id': request_id,
            'request_type': 'item_mapping',
            'payload': {
                'sku': 'ABC123',
                'final_goods_name': 'Test Product',
                'channel': 'amazon'
            }
        }
        
        self.mock_supabase.client.table.return_value.update.return_value.eq.return_value.execute.return_value.data = [mock_request_data]
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'new_mapping'}]
        
        with patch('ingestion_layer.libs.notification_utils.notify_approval_completion') as mock_notify:
            result = self.agent.process_approval_request(
                request_id=request_id,
                decision='approved',
                approver='test_user',
                notes='Looks good'
            )
            
            self.assertTrue(result)
            mock_notify.assert_called_once()
    
    def test_process_approval_decision_rejected(self):
        """Test processing a rejected decision."""
        request_id = str(uuid.uuid4())
        
        # Mock database update
        mock_request_data = {
            'id': request_id,
            'request_type': 'item_mapping',
            'payload': {
                'sku': 'ABC123',
                'final_goods_name': 'Test Product',
                'channel': 'amazon'
            }
        }
        
        self.mock_supabase.client.table.return_value.update.return_value.eq.return_value.execute.return_value.data = [mock_request_data]
        
        with patch('ingestion_layer.libs.notification_utils.notify_approval_completion') as mock_notify:
            result = self.agent.process_approval_request(
                request_id=request_id,
                decision='rejected',
                approver='test_user',
                notes='Insufficient information'
            )
            
            self.assertTrue(result)
            mock_notify.assert_called_once()
    
    def test_invalid_decision(self):
        """Test handling of invalid approval decisions."""
        request_id = str(uuid.uuid4())
        
        with self.assertRaises(ValueError):
            self.agent.process_approval_request(
                request_id=request_id,
                decision='invalid_decision',
                approver='test_user'
            )
    
    def test_apply_item_mapping_approval(self):
        """Test applying approved item mapping to database."""
        payload = {
            'sku': 'ABC123',
            'asin': 'B08N5WRWNW',
            'final_goods_name': 'Test Product',
            'channel': 'amazon',
            'approver': 'test_user'
        }
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'new_mapping'}]
        
        result = self.agent._apply_item_mapping_approval(payload)
        
        self.assertTrue(result)
        self.mock_supabase.client.table.assert_called_with('item_master')
    
    def test_apply_ledger_mapping_approval(self):
        """Test applying approved ledger mapping to database."""
        payload = {
            'channel': 'amazon',
            'state_code': 'HR',
            'ledger_name': 'Amazon Haryana',
            'approver': 'test_user'
        }
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'new_mapping'}]
        
        result = self.agent._apply_ledger_mapping_approval(payload)
        
        self.assertTrue(result)
        self.mock_supabase.client.table.assert_called_with('ledger_master')
    
    def test_get_pending_approvals(self):
        """Test retrieving pending approval requests."""
        mock_approvals = [
            {
                'id': str(uuid.uuid4()),
                'run_id': str(self.run_id),
                'request_type': 'item_mapping',
                'payload': {'sku': 'ABC123'},
                'context_data': {},
                'priority': 'medium',
                'status': 'pending',
                'approver': None,
                'approval_notes': None,
                'created_at': datetime.now().isoformat(),
                'decided_at': None
            }
        ]
        
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = mock_approvals
        
        approvals = self.agent.get_pending_approvals(run_id=self.run_id)
        
        self.assertEqual(len(approvals), 1)
        self.assertIsInstance(approvals[0], ApprovalRequest)
        self.assertEqual(approvals[0].status, 'pending')
    
    def test_get_approval_summary(self):
        """Test getting approval summary statistics."""
        mock_requests = [
            {
                'id': str(uuid.uuid4()),
                'run_id': str(self.run_id),
                'request_type': 'item_mapping',
                'status': 'pending',
                'approver': None
            },
            {
                'id': str(uuid.uuid4()),
                'run_id': str(self.run_id),
                'request_type': 'ledger_mapping',
                'status': 'approved',
                'approver': 'system_auto'
            },
            {
                'id': str(uuid.uuid4()),
                'run_id': str(self.run_id),
                'request_type': 'gst_rate_override',
                'status': 'rejected',
                'approver': 'test_user'
            }
        ]
        
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = mock_requests
        
        summary = self.agent.get_approval_summary(run_id=self.run_id)
        
        self.assertIsInstance(summary, ApprovalResult)
        self.assertEqual(summary.total_requests, 3)
        self.assertEqual(summary.pending_requests, 1)
        self.assertEqual(summary.approved_requests, 1)
        self.assertEqual(summary.rejected_requests, 1)
        self.assertEqual(summary.auto_approved_requests, 1)
        self.assertFalse(summary.processing_successful)  # Has pending requests
    
    def test_create_item_mapping_request_convenience(self):
        """Test convenience method for creating item mapping requests."""
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        request_id = self.agent.create_item_mapping_request(
            sku='ABC123',
            asin='B08N5WRWNW',
            channel='amazon',
            run_id=self.run_id,
            suggested_fg_name='Test Product',
            estimated_value=1500
        )
        
        self.assertIsInstance(request_id, str)
    
    def test_create_ledger_mapping_request_convenience(self):
        """Test convenience method for creating ledger mapping requests."""
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        request_id = self.agent.create_ledger_mapping_request(
            channel='amazon',
            state_code='DL',
            run_id=self.run_id,
            suggested_ledger_name='Amazon Delhi'
        )
        
        self.assertIsInstance(request_id, str)
    
    def test_auto_approval_rules_loading(self):
        """Test loading of approval rules."""
        # Test that default rules are loaded
        self.assertIn('item_mapping', self.agent.approval_rules)
        self.assertIn('ledger_mapping', self.agent.approval_rules)
        self.assertIn('gst_rate_override', self.agent.approval_rules)
        self.assertIn('invoice_override', self.agent.approval_rules)
    
    def test_check_item_mapping_auto_approval_high_value(self):
        """Test that high-value items are not auto-approved."""
        payload = {
            'sku': 'ABC123',
            'estimated_value': 10000  # Above threshold
        }
        
        rules = self.agent.approval_rules['item_mapping']
        result = self.agent._check_item_mapping_auto_approval(payload, rules)
        
        self.assertFalse(result['can_auto_approve'])
        self.assertIn('exceeds threshold', result['reason'])
    
    def test_check_ledger_mapping_auto_approval_unknown_state(self):
        """Test that unknown states require manual approval."""
        payload = {
            'channel': 'amazon',
            'state_code': 'XX'  # Unknown state
        }
        
        rules = self.agent.approval_rules['ledger_mapping']
        result = self.agent._check_ledger_mapping_auto_approval(payload, rules)
        
        self.assertFalse(result['can_auto_approve'])
    
    def test_check_gst_rate_auto_approval_disabled(self):
        """Test that GST rate overrides are not auto-approved by default."""
        payload = {
            'proposed_gst_rate': 0.18
        }
        
        rules = self.agent.approval_rules['gst_rate_override']
        result = self.agent._check_gst_rate_auto_approval(payload, rules)
        
        self.assertFalse(result['can_auto_approve'])
        self.assertIn('manual approval', result['reason'])
    
    def test_check_invoice_auto_approval_format_fix(self):
        """Test auto-approval for invoice format fixes."""
        payload = {
            'override_type': 'format_fix'
        }
        
        rules = self.agent.approval_rules['invoice_override']
        result = self.agent._check_invoice_auto_approval(payload, rules)
        
        self.assertTrue(result['can_auto_approve'])
        self.assertIn('format correction', result['reason'])
    
    def test_save_approval_request(self):
        """Test saving approval request to database."""
        request = ApprovalRequest(
            id=str(uuid.uuid4()),
            run_id=str(self.run_id),
            request_type='item_mapping',
            payload={'sku': 'ABC123'},
            priority='medium',
            status='pending'
        )
        
        # Mock database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        result = self.agent._save_approval_request(request)
        
        self.assertTrue(result)
        self.mock_supabase.client.table.assert_called_with('approval_queue')


if __name__ == '__main__':
    unittest.main()
