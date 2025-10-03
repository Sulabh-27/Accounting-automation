"""
Test suite for Tally Exporter Agent and X2Beta Writer
"""
import unittest
import os
import pandas as pd
import uuid
import tempfile
import shutil
from unittest.mock import Mock, patch

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from ingestion_layer.agents.tally_exporter import TallyExporterAgent, TallyExportResult
from ingestion_layer.libs.x2beta_writer import X2BetaWriter


class MockSupabase:
    """Mock Supabase client for testing."""
    
    def __init__(self):
        self.tally_exports = []
    
    @property
    def client(self):
        return MockClient(self)


class MockClient:
    """Mock Supabase client."""
    
    def __init__(self, parent):
        self.parent = parent
    
    def table(self, table_name):
        return MockTable(table_name, self.parent)


class MockTable:
    """Mock Supabase table."""
    
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def insert(self, data):
        if self.table_name == 'tally_exports':
            self.parent.tally_exports.extend(data)
        return MockInsert()


class MockInsert:
    """Mock insert operation."""
    
    def execute(self):
        return Mock()


class TestX2BetaWriter(unittest.TestCase):
    """Test cases for X2Beta Writer Library."""
    
    def setUp(self):
        self.writer = X2BetaWriter()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_validate_batch_data_valid(self):
        """Test batch data validation with valid data."""
        df = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Haryana',
                'fg': 'FABCON-5L',
                'total_quantity': 2,
                'total_taxable': 2118.0,
                'total_cgst': 190.62,
                'total_sgst': 190.62,
                'total_igst': 0.0
            }
        ])
        
        result = self.writer.validate_batch_data(df)
        
        self.assertTrue(result['valid'])
        self.assertEqual(result['record_count'], 1)
        self.assertEqual(result['gst_rate'], 0.18)
        self.assertEqual(len(result['errors']), 0)
    
    def test_validate_batch_data_missing_columns(self):
        """Test batch data validation with missing columns."""
        df = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Haryana'
                # Missing required columns
            }
        ])
        
        result = self.writer.validate_batch_data(df)
        
        self.assertFalse(result['valid'])
        self.assertIn('Missing required columns', result['errors'][0])
    
    def test_validate_batch_data_multiple_gst_rates(self):
        """Test batch data validation with multiple GST rates."""
        df = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Haryana',
                'fg': 'FABCON-5L',
                'total_quantity': 2,
                'total_taxable': 2118.0
            },
            {
                'gst_rate': 0.12,  # Different GST rate
                'ledger_name': 'Amazon Delhi',
                'fg': 'TOILETCLEANER',
                'total_quantity': 1,
                'total_taxable': 215.0
            }
        ])
        
        result = self.writer.validate_batch_data(df)
        
        self.assertFalse(result['valid'])
        self.assertIn('Multiple GST rates found', result['errors'][0])
    
    def test_map_batch_to_x2beta_intrastate(self):
        """Test mapping batch data to X2Beta format for intrastate transactions."""
        df = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Haryana',
                'fg': 'FABCON-5L',
                'total_quantity': 2,
                'total_taxable': 2118.0,
                'total_cgst': 190.62,
                'total_sgst': 190.62,
                'total_igst': 0.0,
                'invoice_no': 'AMZ-HR-08-0001'
            }
        ])
        
        result = self.writer.map_batch_to_x2beta(df, '06ABGCS4796R1ZA', '2025-08')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['Voucher No.'], 'AMZ-HR-08-0001')
        self.assertEqual(result.iloc[0]['Party Ledger'], 'Amazon Haryana')
        self.assertEqual(result.iloc[0]['Item Name'], 'FABCON-5L')
        self.assertEqual(result.iloc[0]['Taxable Amount'], 2118.0)
        self.assertEqual(result.iloc[0]['CGST Amount'], 190.62)
        self.assertEqual(result.iloc[0]['SGST Amount'], 190.62)
        self.assertEqual(result.iloc[0]['IGST Amount'], 0)
        self.assertIn('Output CGST @ 18%', result.iloc[0]['Output CGST Ledger'])
    
    def test_map_batch_to_x2beta_interstate(self):
        """Test mapping batch data to X2Beta format for interstate transactions."""
        df = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Delhi',
                'fg': 'FABCON-5L',
                'total_quantity': 1,
                'total_taxable': 1059.0,
                'total_cgst': 0.0,
                'total_sgst': 0.0,
                'total_igst': 190.62,
                'invoice_no': 'AMZ-DL-08-0001'
            }
        ])
        
        result = self.writer.map_batch_to_x2beta(df, '06ABGCS4796R1ZA', '2025-08')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['CGST Amount'], 0)
        self.assertEqual(result.iloc[0]['SGST Amount'], 0)
        self.assertEqual(result.iloc[0]['IGST Amount'], 190.62)
        self.assertIn('Output IGST @ 18%', result.iloc[0]['Output IGST Ledger'])
        self.assertEqual(result.iloc[0]['Output CGST Ledger'], '')
    
    def test_map_batch_to_x2beta_zero_gst(self):
        """Test mapping batch data to X2Beta format for zero GST transactions."""
        df = pd.DataFrame([
            {
                'gst_rate': 0.0,
                'ledger_name': 'Amazon Delhi',
                'fg': 'FABCON-5L',
                'total_quantity': 4,
                'total_taxable': 4236.0,
                'total_cgst': 0.0,
                'total_sgst': 0.0,
                'total_igst': 0.0,
                'invoice_no': 'AMZ-DL-08-0003'
            }
        ])
        
        result = self.writer.map_batch_to_x2beta(df, '06ABGCS4796R1ZA', '2025-08')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['CGST Amount'], 0)
        self.assertEqual(result.iloc[0]['SGST Amount'], 0)
        self.assertEqual(result.iloc[0]['IGST Amount'], 0)
        self.assertEqual(result.iloc[0]['Output CGST Ledger'], '')
        self.assertEqual(result.iloc[0]['Output SGST Ledger'], '')
        self.assertEqual(result.iloc[0]['Output IGST Ledger'], '')
    
    def test_create_default_template(self):
        """Test creating default X2Beta template."""
        wb = self.writer.create_default_template('06ABGCS4796R1ZA', 'Test Company')
        ws = wb.active
        
        # Check company info
        self.assertIn('Test Company', ws.cell(row=1, column=1).value)
        self.assertIn('06ABGCS4796R1ZA', ws.cell(row=2, column=1).value)
        
        # Check headers are present
        headers = [cell.value for cell in ws[4]]
        self.assertIn('Date', headers)
        self.assertIn('Voucher No.', headers)
        self.assertIn('Party Ledger', headers)
        self.assertIn('Taxable Amount', headers)
    
    def test_write_to_template(self):
        """Test writing data to X2Beta template."""
        # Create sample X2Beta data
        x2beta_data = pd.DataFrame([
            {
                'Date': '01-08-2025',
                'Voucher No.': 'AMZ-HR-08-0001',
                'Voucher Type': 'Sales',
                'Party Ledger': 'Amazon Haryana',
                'Party Name': 'Amazon Haryana',
                'Item Name': 'FABCON-5L',
                'Quantity': 2,
                'Rate': 1059.0,
                'Taxable Amount': 2118.0,
                'Output CGST Ledger': 'Output CGST @ 18%',
                'CGST Amount': 190.62,
                'Output SGST Ledger': 'Output SGST @ 18%',
                'SGST Amount': 190.62,
                'Output IGST Ledger': '',
                'IGST Amount': 0,
                'Total Amount': 2499.24,
                'Narration': 'Sales - FABCON-5L - 2025-08'
            }
        ])
        
        # Create a simple template
        template_path = os.path.join(self.temp_dir, 'template.xlsx')
        wb = self.writer.create_default_template('06ABGCS4796R1ZA', 'Test Company')
        wb.save(template_path)
        
        # Write data to template
        output_path = os.path.join(self.temp_dir, 'output.xlsx')
        result = self.writer.write_to_template(x2beta_data, template_path, output_path)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['record_count'], 1)
        self.assertEqual(result['total_taxable'], 2118.0)
        self.assertEqual(result['total_tax'], 381.24)
        self.assertTrue(os.path.exists(output_path))


class TestTallyExporterAgent(unittest.TestCase):
    """Test cases for Tally Exporter Agent."""
    
    def setUp(self):
        self.supabase = MockSupabase()
        self.exporter = TallyExporterAgent(self.supabase)
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_validate_template_availability_valid(self):
        """Test template availability validation for valid GSTIN."""
        # Mock template existence
        with patch('os.path.exists', return_value=True):
            with patch.object(self.exporter.x2beta_writer, 'validate_template', 
                            return_value={'valid': True, 'errors': []}):
                
                result = self.exporter.validate_template_availability('06ABGCS4796R1ZA')
                
                self.assertTrue(result['available'])
                self.assertEqual(result['company_name'], 'Zaggle Haryana Private Limited')
                self.assertEqual(result['state_name'], 'HARYANA')
    
    def test_validate_template_availability_invalid_gstin(self):
        """Test template availability validation for invalid GSTIN."""
        result = self.exporter.validate_template_availability('INVALID_GSTIN')
        
        self.assertFalse(result['available'])
        self.assertIn('No template configuration', result['error'])
    
    def test_validate_template_availability_missing_file(self):
        """Test template availability validation for missing template file."""
        with patch('os.path.exists', return_value=False):
            result = self.exporter.validate_template_availability('06ABGCS4796R1ZA')
            
            self.assertFalse(result['available'])
            self.assertIn('Template file not found', result['error'])
    
    def test_find_batch_files(self):
        """Test finding batch files in directory."""
        # Create test batch files
        batch_files = [
            'amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv',
            'amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_batch.csv',
            'flipkart_06ABGCS4796R1ZA_2025-08_18pct_batch.csv',  # Different channel
            'other_file.csv'  # Not a batch file
        ]
        
        for filename in batch_files:
            file_path = os.path.join(self.temp_dir, filename)
            with open(file_path, 'w') as f:
                f.write('test,data\n1,2')
        
        # Test finding Amazon MTR batch files
        found_files = self.exporter._find_batch_files(
            self.temp_dir, '06ABGCS4796R1ZA', 'amazon_mtr', '2025-08'
        )
        
        self.assertEqual(len(found_files), 2)
        self.assertTrue(any('18pct_batch.csv' in f for f in found_files))
        self.assertTrue(any('0pct_batch.csv' in f for f in found_files))
        self.assertFalse(any('flipkart' in f for f in found_files))
    
    def test_export_single_batch_success(self):
        """Test successful export of single batch file."""
        # Create test batch file
        batch_data = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Haryana',
                'fg': 'FABCON-5L',
                'total_quantity': 2,
                'total_taxable': 2118.0,
                'total_cgst': 190.62,
                'total_sgst': 190.62,
                'total_igst': 0.0,
                'invoice_no': 'AMZ-HR-08-0001'
            }
        ])
        
        batch_file = os.path.join(self.temp_dir, 'amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv')
        batch_data.to_csv(batch_file, index=False)
        
        # Mock template existence and validation
        with patch('os.path.exists', return_value=True):
            with patch.object(self.exporter.x2beta_writer, 'load_template'):
                with patch.object(self.exporter.x2beta_writer, 'write_to_template',
                                return_value={
                                    'success': True,
                                    'record_count': 1,
                                    'total_taxable': 2118.0,
                                    'total_tax': 381.24,
                                    'file_size': 1024
                                }):
                    
                    result = self.exporter.export_single_batch(batch_file, '06ABGCS4796R1ZA', self.temp_dir)
                    
                    self.assertTrue(result['success'])
                    self.assertEqual(result['record_count'], 1)
                    self.assertEqual(result['total_taxable'], 2118.0)
                    self.assertEqual(result['gst_rate'], 0.18)
    
    def test_get_export_summary(self):
        """Test generating export summary statistics."""
        export_results = [
            {
                'success': True,
                'record_count': 10,
                'total_taxable': 10000.0,
                'total_tax': 1800.0,
                'gst_rate': 0.18,
                'file_size': 1024
            },
            {
                'success': True,
                'record_count': 5,
                'total_taxable': 5000.0,
                'total_tax': 0.0,
                'gst_rate': 0.0,
                'file_size': 512
            },
            {
                'success': False,
                'error': 'Test error'
            }
        ]
        
        summary = self.exporter.get_export_summary(export_results)
        
        self.assertEqual(summary['total_files'], 3)
        self.assertEqual(summary['successful_exports'], 2)
        self.assertEqual(summary['failed_exports'], 1)
        self.assertEqual(summary['total_records'], 15)
        self.assertEqual(summary['total_taxable_amount'], 15000.0)
        self.assertEqual(summary['total_tax_amount'], 1800.0)
        self.assertEqual(summary['file_size_total'], 1536)
        
        # Check GST rate breakdown
        self.assertIn('18%', summary['gst_rate_breakdown'])
        self.assertIn('0%', summary['gst_rate_breakdown'])
        self.assertEqual(summary['gst_rate_breakdown']['18%']['records'], 10)
        self.assertEqual(summary['gst_rate_breakdown']['0%']['records'], 5)


class TestTallyExportIntegration(unittest.TestCase):
    """Integration tests for complete Tally export workflow."""
    
    def setUp(self):
        self.supabase = MockSupabase()
        self.exporter = TallyExporterAgent(self.supabase)
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_complete_export_workflow(self):
        """Test complete export workflow from batch files to X2Beta Excel."""
        # Create test batch files with different GST rates
        batch_data_18pct = pd.DataFrame([
            {
                'gst_rate': 0.18,
                'ledger_name': 'Amazon Haryana',
                'fg': 'FABCON-5L',
                'total_quantity': 2,
                'total_taxable': 2118.0,
                'total_cgst': 190.62,
                'total_sgst': 190.62,
                'total_igst': 0.0,
                'invoice_no': 'AMZ-HR-08-0001'
            }
        ])
        
        batch_data_0pct = pd.DataFrame([
            {
                'gst_rate': 0.0,
                'ledger_name': 'Amazon Delhi',
                'fg': 'FABCON-5L',
                'total_quantity': 4,
                'total_taxable': 4236.0,
                'total_cgst': 0.0,
                'total_sgst': 0.0,
                'total_igst': 0.0,
                'invoice_no': 'AMZ-DL-08-0003'
            }
        ])
        
        # Save batch files
        batch_dir = os.path.join(self.temp_dir, 'batches')
        os.makedirs(batch_dir, exist_ok=True)
        
        batch_file_18pct = os.path.join(batch_dir, 'amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv')
        batch_file_0pct = os.path.join(batch_dir, 'amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_batch.csv')
        
        batch_data_18pct.to_csv(batch_file_18pct, index=False)
        batch_data_0pct.to_csv(batch_file_0pct, index=False)
        
        # Mock template operations
        with patch('os.path.exists', return_value=True):
            with patch.object(self.exporter.x2beta_writer, 'load_template'):
                with patch.object(self.exporter.x2beta_writer, 'write_to_template') as mock_write:
                    mock_write.side_effect = [
                        {
                            'success': True,
                            'record_count': 1,
                            'total_taxable': 2118.0,
                            'total_tax': 381.24,
                            'file_size': 1024
                        },
                        {
                            'success': True,
                            'record_count': 1,
                            'total_taxable': 4236.0,
                            'total_tax': 0.0,
                            'file_size': 512
                        }
                    ]
                    
                    # Process batch files
                    result = self.exporter.process_batch_files(
                        batch_dir, '06ABGCS4796R1ZA', 'amazon_mtr', '2025-08', uuid.uuid4(), self.temp_dir
                    )
                    
                    # Verify results
                    self.assertTrue(result.success)
                    self.assertEqual(result.processed_files, 2)
                    self.assertEqual(result.exported_files, 2)
                    self.assertEqual(result.total_records, 2)
                    self.assertEqual(result.total_taxable, 6354.0)
                    self.assertEqual(result.total_tax, 381.24)
                    self.assertEqual(len(result.export_paths), 2)
                    self.assertEqual(len(result.gst_rates_processed), 2)
                    self.assertIn(0.18, result.gst_rates_processed)
                    self.assertIn(0.0, result.gst_rates_processed)


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_suite.addTest(unittest.makeSuite(TestX2BetaWriter))
    test_suite.addTest(unittest.makeSuite(TestTallyExporterAgent))
    test_suite.addTest(unittest.makeSuite(TestTallyExportIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)
