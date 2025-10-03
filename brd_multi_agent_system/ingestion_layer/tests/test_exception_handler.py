"""
Tests for Exception Handler Agent (Part 7)
"""
import unittest
import pandas as pd
import uuid
from datetime import datetime
from unittest.mock import Mock, patch

from ..agents.exception_handler import ExceptionHandler, ExceptionResult
from ..libs.error_codes import ErrorCodes


class TestExceptionHandler(unittest.TestCase):
    """Test cases for Exception Handler Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = Mock()
        self.handler = ExceptionHandler(self.mock_supabase)
        self.run_id = uuid.uuid4()
    
    def test_detect_missing_sku_mapping(self):
        """Test detection of missing SKU mappings."""
        # Create test data with missing SKU mapping
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'asin': ['B08N5WRWNW', 'B07M8K9XYZ'],
            'final_goods_name': ['Product A', None],  # Missing mapping for XYZ789
            'channel': ['amazon', 'amazon'],
            'state_code': ['HR', 'DL']
        })
        
        result = self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        
        self.assertIsInstance(result, ExceptionResult)
        self.assertEqual(result.total_records, 2)
        self.assertEqual(result.exceptions_detected, 1)
        self.assertTrue(result.processing_successful)
        
        # Check that exception was recorded
        self.assertEqual(len(self.handler.exceptions), 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'MAP-001')
        self.assertEqual(exception['record_id'], 'XYZ789')
    
    def test_detect_missing_asin_mapping(self):
        """Test detection of missing ASIN mappings."""
        df = pd.DataFrame({
            'sku': [None, 'ABC123'],
            'asin': ['B08N5WRWNW', 'B07M8K9XYZ'],
            'final_goods_name': [None, 'Product B'],  # Missing mapping for B08N5WRWNW
            'channel': ['amazon', 'amazon'],
            'state_code': ['HR', 'DL']
        })
        
        result = self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'MAP-002')
        self.assertEqual(exception['record_id'], 'B08N5WRWNW')
    
    def test_detect_missing_ledger_mapping(self):
        """Test detection of missing ledger mappings."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'channel': ['amazon', 'flipkart'],
            'state_code': ['HR', 'DL'],
            'ledger_name': ['Amazon Haryana', None]  # Missing ledger for flipkart DL
        })
        
        result = self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'LED-001')
        self.assertEqual(exception['record_id'], 'flipkart_DL')
    
    def test_detect_invalid_state_codes(self):
        """Test detection of invalid state codes."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'channel': ['amazon', 'amazon'],
            'state_code': ['HR', 'XX'],  # XX is invalid state code
            'ledger_name': ['Amazon Haryana', 'Amazon Unknown']
        })
        
        result = self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'LED-002')
        self.assertEqual(exception['record_id'], 'XX')
    
    def test_detect_invalid_gst_rates(self):
        """Test detection of invalid GST rates."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'gst_rate': [0.18, 0.15],  # 0.15 is invalid GST rate
            'taxable_value': [1000, 2000],
            'final_goods_name': ['Product A', 'Product B']
        })
        
        result = self.handler.detect_gst_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'GST-001')
        self.assertIn('0.15', exception['record_id'])
    
    def test_detect_missing_gst_rates(self):
        """Test detection of missing GST rates on taxable transactions."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'gst_rate': [0.18, None],  # Missing GST rate for taxable transaction
            'taxable_value': [1000, 2000],
            'final_goods_name': ['Product A', 'Product B']
        })
        
        result = self.handler.detect_gst_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'GST-003')
    
    def test_detect_gst_calculation_mismatch(self):
        """Test detection of GST calculation mismatches."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'taxable_value': [1000, 2000],
            'gst_rate': [0.18, 0.18],
            'total_tax': [180, 300]  # Should be 360 for second row
        })
        
        result = self.handler.detect_gst_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'GST-002')
    
    def test_detect_duplicate_invoices(self):
        """Test detection of duplicate invoice numbers."""
        df = pd.DataFrame({
            'invoice_no': ['AMZHR202508001', 'AMZHR202508001', 'AMZHR202508002'],
            'sku': ['ABC123', 'XYZ789', 'DEF456']
        })
        
        result = self.handler.detect_invoice_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 2)  # Both duplicate records
        for exception in self.handler.exceptions:
            self.assertEqual(exception['error_code'], 'INV-001')
            self.assertEqual(exception['record_id'], 'AMZHR202508001')
    
    def test_detect_invalid_invoice_format(self):
        """Test detection of invalid invoice number formats."""
        df = pd.DataFrame({
            'invoice_no': ['AMZHR202508001', 'INVALID123'],
            'channel': ['amazon', 'amazon'],
            'sku': ['ABC123', 'XYZ789']
        })
        
        result = self.handler.detect_invoice_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'INV-002')
        self.assertEqual(exception['record_id'], 'INVALID123')
    
    def test_detect_invalid_invoice_dates(self):
        """Test detection of invalid invoice dates."""
        future_date = datetime.now().date().replace(year=2030)
        
        df = pd.DataFrame({
            'invoice_date': [datetime.now().date(), future_date],
            'sku': ['ABC123', 'XYZ789']
        })
        
        result = self.handler.detect_invoice_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'INV-003')
    
    def test_detect_negative_amounts(self):
        """Test detection of negative amounts."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'taxable_value': [1000, -500],  # Negative amount
            'total_tax': [180, 90],
            'total_amount': [1180, 590]
        })
        
        result = self.handler.detect_data_quality_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'DAT-001')
    
    def test_detect_zero_quantities(self):
        """Test detection of zero or negative quantities."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'quantity': [5, 0]  # Zero quantity
        })
        
        result = self.handler.detect_data_quality_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'DAT-002')
    
    def test_detect_missing_required_data(self):
        """Test detection of missing required data."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'invoice_date': [datetime.now().date(), None],  # Missing date
            'taxable_value': [1000, 2000],
            'channel': ['amazon', 'amazon'],
            'gstin': ['06ABGCS4796R1ZA', '06ABGCS4796R1ZA']
        })
        
        result = self.handler.detect_data_quality_exceptions(df, self.run_id, "sales")
        
        self.assertEqual(result.exceptions_detected, 1)
        exception = self.handler.exceptions[0]
        self.assertEqual(exception['error_code'], 'DAT-003')
    
    def test_detect_schema_exceptions(self):
        """Test detection of schema-related exceptions."""
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'taxable_value': [1000, 'invalid']  # Invalid data type
        })
        
        expected_columns = ['sku', 'taxable_value', 'gst_rate', 'missing_column']
        
        result = self.handler.detect_schema_exceptions(df, self.run_id, expected_columns, "sales")
        
        # Should detect missing column and invalid data type
        self.assertGreaterEqual(result.exceptions_detected, 2)
        
        error_codes = [e['error_code'] for e in self.handler.exceptions]
        self.assertIn('SCH-001', error_codes)  # Missing column
        self.assertIn('SCH-002', error_codes)  # Invalid data type
    
    def test_exception_summary(self):
        """Test exception summary generation."""
        # Create some test exceptions
        df = pd.DataFrame({
            'sku': ['ABC123', 'XYZ789'],
            'final_goods_name': [None, None],  # Missing mappings
            'gst_rate': [0.18, 0.15],  # Invalid GST rate
            'channel': ['amazon', 'amazon']
        })
        
        self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        self.handler.detect_gst_exceptions(df, self.run_id, "sales")
        
        summary = self.handler.get_exception_summary()
        
        self.assertGreater(summary['total_exceptions'], 0)
        self.assertIn('by_severity', summary)
        self.assertIn('by_category', summary)
        self.assertIn('by_error_code', summary)
    
    @patch('ingestion_layer.agents.exception_handler.notify_exception')
    def test_notification_for_critical_exceptions(self, mock_notify):
        """Test that notifications are sent for critical exceptions."""
        # Create exception with critical severity
        df = pd.DataFrame({
            'sku': ['ABC123'],
            'taxable_value': ['invalid_type']  # This should trigger schema error
        })
        
        expected_columns = ['sku', 'taxable_value']
        result = self.handler.detect_schema_exceptions(df, self.run_id, expected_columns, "sales")
        
        # Should have called notification for error severity
        self.assertTrue(mock_notify.called)
    
    def test_save_exceptions_to_database(self):
        """Test saving exceptions to database."""
        # Create some test exceptions
        df = pd.DataFrame({
            'sku': ['ABC123'],
            'final_goods_name': [None]
        })
        
        self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        
        # Mock successful database insertion
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value.data = [{'id': 'test'}]
        
        result = self.handler.save_exceptions_to_database()
        
        self.assertTrue(result)
        self.mock_supabase.client.table.assert_called_with('exceptions')
    
    def test_clear_exceptions(self):
        """Test clearing stored exceptions."""
        # Create some test exceptions
        df = pd.DataFrame({
            'sku': ['ABC123'],
            'final_goods_name': [None]
        })
        
        self.handler.detect_mapping_exceptions(df, self.run_id, "sales")
        self.assertGreater(len(self.handler.exceptions), 0)
        
        self.handler.clear_exceptions()
        self.assertEqual(len(self.handler.exceptions), 0)


if __name__ == '__main__':
    unittest.main()
