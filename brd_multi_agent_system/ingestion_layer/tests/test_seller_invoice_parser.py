"""
Unit tests for Seller Invoice Parser Agent
Tests PDF/Excel parsing and data extraction functionality
"""
import unittest
import os
import tempfile
import uuid
import pandas as pd
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from ingestion_layer.agents.seller_invoice_parser import SellerInvoiceParserAgent, SellerInvoiceData
from ingestion_layer.libs.pdf_utils import PDFParser, ExcelInvoiceParser


class TestSellerInvoiceParser(unittest.TestCase):
    """Test cases for Seller Invoice Parser Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = Mock()
        self.parser_agent = SellerInvoiceParserAgent(self.mock_supabase)
        self.test_run_id = uuid.uuid4()
    
    def test_pdf_parser_initialization(self):
        """Test PDF parser initialization."""
        parser = PDFParser()
        self.assertIsNotNone(parser)
        self.assertIsNotNone(parser.logger)
    
    def test_excel_parser_initialization(self):
        """Test Excel parser initialization."""
        parser = ExcelInvoiceParser()
        self.assertIsNotNone(parser)
        self.assertIsNotNone(parser.logger)
    
    def test_parse_amazon_fee_invoice_text(self):
        """Test parsing Amazon fee invoice from text."""
        parser = PDFParser()
        
        # Sample invoice text
        invoice_text = """
        Amazon Services LLC
        Invoice Number: AMZ-FEE-001
        Invoice Date: 20-08-2025
        GSTIN: 06ABGCS4796R1ZA
        
        Description                Amount      Total
        Closing Fee               1000.00     1180.00
        Shipping Fee              2000.00     2360.00
        Commission                5000.00     5900.00
        """
        
        result = parser.parse_amazon_fee_invoice(invoice_text)
        
        self.assertEqual(result['invoice_no'], 'AMZ-FEE-001')
        self.assertEqual(result['gstin'], '06ABGCS4796R1ZA')
        self.assertIsNotNone(result['invoice_date'])
        self.assertGreater(len(result['line_items']), 0)
    
    def test_expense_type_classification(self):
        """Test expense type classification."""
        parser = PDFParser()
        
        test_cases = [
            ('Closing Fee charges', 'Closing Fee'),
            ('Shipping and delivery', 'Shipping Fee'),
            ('Marketplace commission', 'Commission'),
            ('FBA fulfillment charges', 'Fulfillment Fee'),
            ('Storage warehouse fee', 'Storage Fee'),
            ('Advertising promotion', 'Advertising Fee'),
            ('Unknown expense type', 'Other Fee')
        ]
        
        for description, expected_type in test_cases:
            result = parser._classify_expense_type(description)
            self.assertEqual(result, expected_type)
    
    def test_create_mock_excel_invoice(self):
        """Test creating and parsing mock Excel invoice."""
        # Create temporary Excel file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            # Create mock Excel data
            data = {
                'Invoice': ['AMZ-FEE-001'],
                'Date': [datetime(2025, 8, 20)],
                'GSTIN': ['06ABGCS4796R1ZA'],
                'Description': ['Closing Fee'],
                'Taxable Amount': [1000.0],
                'Total Amount': [1180.0]
            }
            
            df = pd.DataFrame(data)
            df.to_excel(tmp_file.name, index=False)
            
            # Test parsing
            parser = ExcelInvoiceParser()
            result = parser.parse_excel_invoice(tmp_file.name)
            
            self.assertEqual(result['invoice_no'], 'AMZ-FEE-001')
            self.assertEqual(result['gstin'], '06ABGCS4796R1ZA')
            self.assertGreater(len(result['line_items']), 0)
            
            # Clean up
            os.unlink(tmp_file.name)
    
    def test_validate_parsed_data_success(self):
        """Test validation of valid parsed data."""
        valid_data = {
            'invoice_no': 'AMZ-FEE-001',
            'invoice_date': date(2025, 8, 20),
            'gstin': '06ABGCS4796R1ZA',
            'line_items': [
                {
                    'expense_type': 'Closing Fee',
                    'taxable_value': 1000.0,
                    'total_value': 1180.0,
                    'tax_amount': 180.0
                }
            ]
        }
        
        is_valid, errors = self.parser_agent._validate_parsed_data(valid_data)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_validate_parsed_data_failure(self):
        """Test validation of invalid parsed data."""
        invalid_data = {
            'invoice_no': '',  # Missing invoice number
            'invoice_date': None,  # Missing date
            'line_items': [
                {
                    'expense_type': '',  # Missing expense type
                    'taxable_value': -100.0,  # Invalid negative value
                    'total_value': 'invalid'  # Invalid type
                }
            ]
        }
        
        is_valid, errors = self.parser_agent._validate_parsed_data(invalid_data)
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
    
    def test_create_invoice_data(self):
        """Test creation of structured invoice data."""
        parsed_data = {
            'invoice_no': 'AMZ-FEE-001',
            'invoice_date': '2025-08-20',
            'gstin': '06ABGCS4796R1ZA',
            'line_items': [
                {
                    'expense_type': 'Closing Fee',
                    'taxable_value': 1000.0,
                    'total_value': 1180.0,
                    'tax_amount': 180.0
                }
            ]
        }
        
        invoice_data = self.parser_agent._create_invoice_data(
            parsed_data, 'amazon', '/path/to/file.pdf'
        )
        
        self.assertIsInstance(invoice_data, SellerInvoiceData)
        self.assertEqual(invoice_data.invoice_no, 'AMZ-FEE-001')
        self.assertEqual(invoice_data.channel, 'amazon')
        self.assertEqual(invoice_data.total_taxable, 1000.0)
        self.assertEqual(invoice_data.total_amount, 1180.0)
    
    def test_process_line_items(self):
        """Test processing and enrichment of line items."""
        invoice_data = SellerInvoiceData(
            invoice_no='AMZ-FEE-001',
            invoice_date=datetime(2025, 8, 20),
            gstin='06ABGCS4796R1ZA',
            channel='amazon',
            line_items=[
                {
                    'expense_type': 'Closing Fee',
                    'taxable_value': 1000.0,
                    'total_value': 1180.0,
                    'tax_amount': 180.0,
                    'gst_rate': 0.18
                }
            ],
            file_path='/path/to/file.pdf'
        )
        
        processed_items = self.parser_agent._process_line_items(invoice_data, self.test_run_id)
        
        self.assertEqual(len(processed_items), 1)
        
        item = processed_items[0]
        self.assertEqual(item['expense_type'], 'Closing Fee')
        self.assertEqual(item['ledger_name'], 'Amazon Closing Fee')
        self.assertEqual(item['taxable_value'], 1000.0)
        self.assertIn('cgst_ledger', item)
        self.assertIn('sgst_ledger', item)
    
    @patch('ingestion_layer.libs.pdf_utils.parse_invoice_file')
    def test_process_invoice_file_success(self, mock_parse):
        """Test successful invoice file processing."""
        # Mock parsed data
        mock_parse.return_value = {
            'invoice_no': 'AMZ-FEE-001',
            'invoice_date': date(2025, 8, 20),
            'gstin': '06ABGCS4796R1ZA',
            'line_items': [
                {
                    'expense_type': 'Closing Fee',
                    'taxable_value': 1000.0,
                    'total_value': 1180.0,
                    'tax_amount': 180.0,
                    'gst_rate': 0.18
                }
            ]
        }
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
            result = self.parser_agent.process_invoice_file(
                tmp_file.name, 'amazon', self.test_run_id
            )
            
            self.assertTrue(result.success)
            self.assertEqual(result.processed_records, 1)
            self.assertIn('invoice_no', result.metadata)
            
            # Clean up
            os.unlink(tmp_file.name)
    
    def test_process_invoice_file_not_found(self):
        """Test processing non-existent file."""
        result = self.parser_agent.process_invoice_file(
            '/non/existent/file.pdf', 'amazon', self.test_run_id
        )
        
        self.assertFalse(result.success)
        self.assertIn('File not found', result.error_message)
    
    def test_process_multiple_invoices(self):
        """Test processing multiple invoice files."""
        # Create temporary files
        temp_files = []
        
        try:
            for i in range(3):
                tmp_file = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                temp_files.append(tmp_file.name)
                tmp_file.close()
            
            # Mock successful parsing for all files
            with patch('ingestion_layer.libs.pdf_utils.parse_invoice_file') as mock_parse:
                mock_parse.return_value = {
                    'invoice_no': 'AMZ-FEE-001',
                    'invoice_date': date(2025, 8, 20),
                    'gstin': '06ABGCS4796R1ZA',
                    'line_items': [
                        {
                            'expense_type': 'Closing Fee',
                            'taxable_value': 1000.0,
                            'total_value': 1180.0,
                            'tax_amount': 180.0,
                            'gst_rate': 0.18
                        }
                    ]
                }
                
                result = self.parser_agent.process_multiple_invoices(
                    temp_files, 'amazon', self.test_run_id
                )
                
                self.assertTrue(result.success)
                self.assertEqual(result.processed_records, 3)
                self.assertEqual(result.metadata['processed_files'], 3)
                self.assertEqual(result.metadata['failed_files'], 0)
        
        finally:
            # Clean up
            for file_path in temp_files:
                if os.path.exists(file_path):
                    os.unlink(file_path)
    
    def test_get_processing_summary_no_db(self):
        """Test processing summary without database."""
        parser_agent = SellerInvoiceParserAgent(None)
        summary = parser_agent.get_processing_summary(self.test_run_id)
        
        self.assertIn('error', summary)
        self.assertEqual(summary['error'], 'No database connection')
    
    def test_get_processing_summary_with_db(self):
        """Test processing summary with database."""
        # Mock database response
        mock_result = Mock()
        mock_result.data = [
            {
                'invoice_no': 'AMZ-FEE-001',
                'expense_type': 'Closing Fee',
                'total_value': 1180.0
            },
            {
                'invoice_no': 'AMZ-FEE-001', 
                'expense_type': 'Shipping Fee',
                'total_value': 2360.0
            }
        ]
        
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.execute.return_value = mock_result
        
        summary = self.parser_agent.get_processing_summary(self.test_run_id)
        
        self.assertEqual(summary['total_invoices'], 1)  # Unique invoice count
        self.assertEqual(summary['total_line_items'], 2)
        self.assertEqual(summary['total_amount'], 3540.0)
        self.assertIn('expense_breakdown', summary)


class TestExpenseRulesIntegration(unittest.TestCase):
    """Test integration with expense rules."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser_agent = SellerInvoiceParserAgent(None)
    
    def test_expense_type_normalization(self):
        """Test expense type normalization."""
        test_cases = [
            ('closing fee', 'Closing Fee'),
            ('SHIPPING FEE', 'Shipping Fee'),
            ('Commission charges', 'Commission'),
            ('fba fulfillment', 'Fulfillment Fee'),
            ('storage charges', 'Storage Fee')
        ]
        
        for input_type, expected_type in test_cases:
            normalized = self.parser_agent.expense_rules.normalize_expense_type(input_type)
            self.assertEqual(normalized, expected_type)
    
    def test_gst_computation(self):
        """Test GST computation for expenses."""
        taxable_amount = 1000.0
        gst_rate = 0.18
        gstin = '06ABGCS4796R1ZA'  # Haryana GSTIN
        
        gst_split = self.parser_agent.expense_rules.compute_gst_split(
            taxable_amount, gst_rate, gstin
        )
        
        # For interstate (assuming no vendor GSTIN), should be IGST
        self.assertEqual(gst_split['igst_amount'], 180.0)
        self.assertEqual(gst_split['cgst_amount'], 0.0)
        self.assertEqual(gst_split['sgst_amount'], 0.0)
    
    def test_ledger_mapping(self):
        """Test expense ledger mapping."""
        rule = self.parser_agent.expense_rules.get_expense_rule('amazon', 'Closing Fee')
        
        self.assertIsNotNone(rule)
        self.assertEqual(rule.ledger_name, 'Amazon Closing Fee')
        self.assertEqual(rule.gst_rate, 0.18)
        self.assertTrue(rule.is_input_gst)


if __name__ == '__main__':
    unittest.main()
