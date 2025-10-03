"""
Unit tests for Expense Mapper Agent
Tests expense mapping to ledger accounts and GST computation
"""
import unittest
import os
import uuid
from datetime import datetime, date
from unittest.mock import Mock, MagicMock

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from ingestion_layer.agents.expense_mapper import ExpenseMapperAgent, MappedExpense
from ingestion_layer.libs.expense_rules import ExpenseRulesEngine, ExpenseRule


class TestExpenseMapper(unittest.TestCase):
    """Test cases for Expense Mapper Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = Mock()
        self.mapper_agent = ExpenseMapperAgent(self.mock_supabase)
        self.test_run_id = uuid.uuid4()
        self.test_gstin = '06ABGCS4796R1ZA'
    
    def test_expense_mapper_initialization(self):
        """Test expense mapper initialization."""
        self.assertIsNotNone(self.mapper_agent)
        self.assertIsNotNone(self.mapper_agent.expense_rules)
        self.assertIsInstance(self.mapper_agent.expense_rules, ExpenseRulesEngine)
    
    def test_get_mock_parsed_invoices(self):
        """Test getting mock parsed invoices."""
        invoices = self.mapper_agent._get_mock_parsed_invoices()
        
        self.assertGreater(len(invoices), 0)
        
        for invoice in invoices:
            self.assertIn('invoice_no', invoice)
            self.assertIn('expense_type', invoice)
            self.assertIn('taxable_value', invoice)
            self.assertIn('total_value', invoice)
            self.assertIn('gst_rate', invoice)
    
    def test_map_invoice_to_ledger_with_rule(self):
        """Test mapping invoice to ledger with existing rule."""
        invoice = {
            'id': str(uuid.uuid4()),
            'run_id': str(self.test_run_id),
            'channel': 'amazon',
            'gstin': self.test_gstin,
            'invoice_no': 'AMZ-FEE-001',
            'invoice_date': date(2025, 8, 20),
            'expense_type': 'Closing Fee',
            'taxable_value': 1000.0,
            'gst_rate': 0.18,
            'cgst': 90.0,
            'sgst': 90.0,
            'igst': 0.0,
            'total_value': 1180.0
        }
        
        mapped_expense = self.mapper_agent._map_invoice_to_ledger(invoice, self.test_gstin)
        
        self.assertIsInstance(mapped_expense, MappedExpense)
        self.assertEqual(mapped_expense.expense_type, 'Closing Fee')
        self.assertEqual(mapped_expense.ledger_name, 'Amazon Closing Fee')
        self.assertEqual(mapped_expense.taxable_value, 1000.0)
        self.assertEqual(mapped_expense.voucher_type, 'Purchase')
        self.assertIsNotNone(mapped_expense.voucher_no)
    
    def test_map_invoice_to_ledger_without_rule(self):
        """Test mapping invoice to ledger without existing rule."""
        invoice = {
            'id': str(uuid.uuid4()),
            'run_id': str(self.test_run_id),
            'channel': 'unknown_channel',
            'gstin': self.test_gstin,
            'invoice_no': 'UNK-FEE-001',
            'invoice_date': date(2025, 8, 20),
            'expense_type': 'Unknown Fee',
            'taxable_value': 500.0,
            'gst_rate': 0.18,
            'cgst': 45.0,
            'sgst': 45.0,
            'igst': 0.0,
            'total_value': 590.0
        }
        
        mapped_expense = self.mapper_agent._map_invoice_to_ledger(invoice, self.test_gstin)
        
        self.assertIsInstance(mapped_expense, MappedExpense)
        self.assertEqual(mapped_expense.expense_type, 'Unknown Fee')
        self.assertEqual(mapped_expense.ledger_name, 'Unknown_channel Unknown Fee')
        self.assertEqual(mapped_expense.taxable_value, 500.0)
    
    def test_generate_voucher_number(self):
        """Test voucher number generation."""
        invoice = {
            'invoice_no': 'AMZ-FEE-001',
            'invoice_date': date(2025, 8, 20)
        }
        
        voucher_no = self.mapper_agent._generate_voucher_number(invoice, self.test_gstin)
        
        # Should be in format: EXP{state_code}{year}{month}{sequence}
        self.assertTrue(voucher_no.startswith('EXP06'))  # State code 06 for Haryana
        self.assertIn('2508', voucher_no)  # Year 25, Month 08
        self.assertEqual(len(voucher_no), 11)  # EXP + 2 + 2 + 2 + 4
    
    def test_process_parsed_invoices_success(self):
        """Test successful processing of parsed invoices."""
        # Mock database response
        mock_result = Mock()
        mock_result.data = [
            {
                'id': str(uuid.uuid4()),
                'run_id': str(self.test_run_id),
                'channel': 'amazon',
                'gstin': self.test_gstin,
                'invoice_no': 'AMZ-FEE-001',
                'invoice_date': date(2025, 8, 20),
                'expense_type': 'Closing Fee',
                'taxable_value': 1000.0,
                'gst_rate': 0.18,
                'cgst': 90.0,
                'sgst': 90.0,
                'igst': 0.0,
                'total_value': 1180.0,
                'processing_status': 'processed'
            }
        ]
        
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result
        
        result = self.mapper_agent.process_parsed_invoices(self.test_run_id, self.test_gstin)
        
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 1)
        self.assertIn('summary', result.metadata)
    
    def test_process_parsed_invoices_no_data(self):
        """Test processing when no parsed invoices found."""
        # Mock empty database response
        mock_result = Mock()
        mock_result.data = []
        
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result
        
        result = self.mapper_agent.process_parsed_invoices(self.test_run_id, self.test_gstin)
        
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 0)
        self.assertIn('No parsed invoices found', result.metadata['message'])
    
    def test_generate_mapping_summary(self):
        """Test generation of mapping summary."""
        mapped_expenses = [
            MappedExpense(
                id=str(uuid.uuid4()),
                run_id=str(self.test_run_id),
                channel='amazon',
                gstin=self.test_gstin,
                invoice_no='AMZ-FEE-001',
                invoice_date=datetime(2025, 8, 20),
                expense_type='Closing Fee',
                ledger_name='Amazon Closing Fee',
                taxable_value=1000.0,
                gst_rate=0.18,
                cgst_amount=90.0,
                sgst_amount=90.0,
                igst_amount=0.0,
                total_value=1180.0,
                voucher_no='EXP0625080001'
            ),
            MappedExpense(
                id=str(uuid.uuid4()),
                run_id=str(self.test_run_id),
                channel='amazon',
                gstin=self.test_gstin,
                invoice_no='AMZ-FEE-002',
                invoice_date=datetime(2025, 8, 21),
                expense_type='Shipping Fee',
                ledger_name='Amazon Shipping Fee',
                taxable_value=2000.0,
                gst_rate=0.18,
                cgst_amount=0.0,
                sgst_amount=0.0,
                igst_amount=360.0,
                total_value=2360.0,
                voucher_no='EXP0625080002'
            )
        ]
        
        summary = self.mapper_agent._generate_mapping_summary(mapped_expenses)
        
        self.assertEqual(summary['total_expenses'], 2)
        self.assertEqual(summary['total_amount'], 3540.0)
        self.assertIn('Closing Fee', summary['expense_types'])
        self.assertIn('Shipping Fee', summary['expense_types'])
        self.assertIn('Amazon Closing Fee', summary['ledger_mapping'])
        self.assertIn('Amazon Shipping Fee', summary['ledger_mapping'])
        
        # GST summary
        gst_summary = summary['gst_summary']
        self.assertEqual(gst_summary['total_cgst'], 90.0)
        self.assertEqual(gst_summary['total_sgst'], 90.0)
        self.assertEqual(gst_summary['total_igst'], 360.0)
        self.assertEqual(gst_summary['intrastate_transactions'], 1)
        self.assertEqual(gst_summary['interstate_transactions'], 1)
    
    def test_validate_expense_mapping_success(self):
        """Test successful expense mapping validation."""
        # Mock successful mapping data
        with unittest.mock.patch.object(self.mapper_agent, 'get_mapped_expenses_for_export') as mock_get:
            mock_get.return_value = [
                MappedExpense(
                    id=str(uuid.uuid4()),
                    run_id=str(self.test_run_id),
                    channel='amazon',
                    gstin=self.test_gstin,
                    invoice_no='AMZ-FEE-001',
                    invoice_date=datetime(2025, 8, 20),
                    expense_type='Closing Fee',
                    ledger_name='Amazon Closing Fee',
                    taxable_value=1000.0,
                    gst_rate=0.18,
                    cgst_amount=90.0,
                    sgst_amount=90.0,
                    igst_amount=0.0,
                    total_value=1180.0,
                    voucher_no='EXP0625080001'
                )
            ]
            
            is_valid, errors = self.mapper_agent.validate_expense_mapping(self.test_run_id, self.test_gstin)
            
            self.assertTrue(is_valid)
            self.assertEqual(len(errors), 0)
    
    def test_validate_expense_mapping_failure(self):
        """Test expense mapping validation with errors."""
        # Mock invalid mapping data
        with unittest.mock.patch.object(self.mapper_agent, 'get_mapped_expenses_for_export') as mock_get:
            mock_get.return_value = [
                MappedExpense(
                    id=str(uuid.uuid4()),
                    run_id=str(self.test_run_id),
                    channel='amazon',
                    gstin=self.test_gstin,
                    invoice_no='AMZ-FEE-001',
                    invoice_date=datetime(2025, 8, 20),
                    expense_type='Closing Fee',
                    ledger_name='',  # Missing ledger name
                    taxable_value=-100.0,  # Invalid negative value
                    gst_rate=0.18,
                    cgst_amount=90.0,
                    sgst_amount=90.0,
                    igst_amount=0.0,
                    total_value=1180.0,
                    voucher_no=''  # Missing voucher number
                )
            ]
            
            is_valid, errors = self.mapper_agent.validate_expense_mapping(self.test_run_id, self.test_gstin)
            
            self.assertFalse(is_valid)
            self.assertGreater(len(errors), 0)
    
    def test_add_custom_expense_rule(self):
        """Test adding custom expense rule."""
        success = self.mapper_agent.add_custom_expense_rule(
            'custom_channel', 'Custom Fee', 'Custom Ledger', 0.12
        )
        
        self.assertTrue(success)
        
        # Verify rule was added
        rule = self.mapper_agent.expense_rules.get_expense_rule('custom_channel', 'Custom Fee')
        self.assertIsNotNone(rule)
        self.assertEqual(rule.ledger_name, 'Custom Ledger')
        self.assertEqual(rule.gst_rate, 0.12)


class TestExpenseRulesEngine(unittest.TestCase):
    """Test cases for Expense Rules Engine."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.rules_engine = ExpenseRulesEngine()
    
    def test_get_expense_rule_exact_match(self):
        """Test getting expense rule with exact match."""
        rule = self.rules_engine.get_expense_rule('amazon', 'Closing Fee')
        
        self.assertIsNotNone(rule)
        self.assertEqual(rule.channel, 'amazon')
        self.assertEqual(rule.expense_type, 'Closing Fee')
        self.assertEqual(rule.ledger_name, 'Amazon Closing Fee')
        self.assertEqual(rule.gst_rate, 0.18)
    
    def test_get_expense_rule_partial_match(self):
        """Test getting expense rule with partial match."""
        rule = self.rules_engine.get_expense_rule('amazon', 'closing charges')
        
        self.assertIsNotNone(rule)
        self.assertEqual(rule.expense_type, 'Closing Fee')
    
    def test_get_expense_rule_fallback(self):
        """Test getting expense rule fallback to 'Other Fee'."""
        rule = self.rules_engine.get_expense_rule('amazon', 'Unknown Expense')
        
        self.assertIsNotNone(rule)
        self.assertEqual(rule.expense_type, 'Other Fee')
    
    def test_get_expense_rule_no_match(self):
        """Test getting expense rule with no match."""
        rule = self.rules_engine.get_expense_rule('unknown_channel', 'Unknown Fee')
        
        self.assertIsNone(rule)
    
    def test_compute_gst_split_intrastate(self):
        """Test GST split computation for intrastate transaction."""
        gst_split = self.rules_engine.compute_gst_split(
            1000.0, 0.18, '06ABGCS4796R1ZA', '06SOMEOTHER1Z1A'  # Same state code
        )
        
        self.assertEqual(gst_split['cgst_rate'], 0.09)
        self.assertEqual(gst_split['sgst_rate'], 0.09)
        self.assertEqual(gst_split['igst_rate'], 0.0)
        self.assertEqual(gst_split['cgst_amount'], 90.0)
        self.assertEqual(gst_split['sgst_amount'], 90.0)
        self.assertEqual(gst_split['igst_amount'], 0.0)
    
    def test_compute_gst_split_interstate(self):
        """Test GST split computation for interstate transaction."""
        gst_split = self.rules_engine.compute_gst_split(
            1000.0, 0.18, '06ABGCS4796R1ZA', '07SOMEOTHER1Z1A'  # Different state codes
        )
        
        self.assertEqual(gst_split['cgst_rate'], 0.0)
        self.assertEqual(gst_split['sgst_rate'], 0.0)
        self.assertEqual(gst_split['igst_rate'], 0.18)
        self.assertEqual(gst_split['cgst_amount'], 0.0)
        self.assertEqual(gst_split['sgst_amount'], 0.0)
        self.assertEqual(gst_split['igst_amount'], 180.0)
    
    def test_compute_gst_split_no_vendor_gstin(self):
        """Test GST split computation without vendor GSTIN (assumes interstate)."""
        gst_split = self.rules_engine.compute_gst_split(
            1000.0, 0.18, '06ABGCS4796R1ZA', None
        )
        
        # Should default to interstate (IGST)
        self.assertEqual(gst_split['igst_rate'], 0.18)
        self.assertEqual(gst_split['igst_amount'], 180.0)
    
    def test_get_gst_ledger_names_input(self):
        """Test getting input GST ledger names."""
        gst_split = {
            'cgst_rate': 0.09,
            'sgst_rate': 0.09,
            'igst_rate': 0.0,
            'cgst_amount': 90.0,
            'sgst_amount': 90.0,
            'igst_amount': 0.0
        }
        
        ledgers = self.rules_engine.get_gst_ledger_names(gst_split, is_input_gst=True)
        
        self.assertEqual(ledgers['cgst_ledger'], 'Input CGST @ 9%')
        self.assertEqual(ledgers['sgst_ledger'], 'Input SGST @ 9%')
        self.assertNotIn('igst_ledger', ledgers)
    
    def test_normalize_expense_type(self):
        """Test expense type normalization."""
        test_cases = [
            ('closing fee', 'Closing Fee'),
            ('SHIPPING FEE', 'Shipping Fee'),
            ('delivery charges', 'Shipping Fee'),
            ('referral fee', 'Commission'),
            ('fba charges', 'Fulfillment Fee'),
            ('warehouse fee', 'Storage Fee'),
            ('ads fee', 'Advertising Fee'),
            ('random expense', 'Random Expense')
        ]
        
        for input_type, expected_type in test_cases:
            result = self.rules_engine.normalize_expense_type(input_type)
            self.assertEqual(result, expected_type)
    
    def test_validate_expense_data_success(self):
        """Test successful expense data validation."""
        expense_data = {
            'expense_type': 'Closing Fee',
            'taxable_value': 1000.0,
            'gst_rate': 0.18,
            'total_value': 1180.0
        }
        
        is_valid, errors = self.rules_engine.validate_expense_data(expense_data)
        
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_validate_expense_data_failure(self):
        """Test expense data validation with errors."""
        expense_data = {
            'expense_type': '',  # Missing
            'taxable_value': -100.0,  # Negative
            'gst_rate': 0.25,  # Invalid rate
            'total_value': 50.0  # Less than taxable
        }
        
        is_valid, errors = self.rules_engine.validate_expense_data(expense_data)
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)


if __name__ == '__main__':
    unittest.main()
