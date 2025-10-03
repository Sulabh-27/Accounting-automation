"""
Unit tests for Expense Tally Exporter Agent
Tests X2Beta export functionality for expenses
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

from ingestion_layer.agents.expense_tally_exporter import ExpenseTallyExporterAgent, ExpenseExportResult
from ingestion_layer.agents.expense_mapper import MappedExpense


class TestExpenseTallyExporter(unittest.TestCase):
    """Test cases for Expense Tally Exporter Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = Mock()
        self.exporter_agent = ExpenseTallyExporterAgent(self.mock_supabase)
        self.test_run_id = uuid.uuid4()
        self.test_gstin = '06ABGCS4796R1ZA'
        self.test_channel = 'amazon'
        self.test_month = '2025-08'
    
    def test_exporter_initialization(self):
        """Test expense tally exporter initialization."""
        self.assertIsNotNone(self.exporter_agent)
        self.assertIsNotNone(self.exporter_agent.x2beta_writer)
        self.assertIsNotNone(self.exporter_agent.expense_mapper)
    
    def test_validate_expense_template_exists(self):
        """Test template validation when template exists."""
        # Create temporary template file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            template_path = f"ingestion_layer/templates/X2Beta Sales Template - {self.test_gstin}.xlsx"
            
            # Mock file existence
            with patch('os.path.exists') as mock_exists:
                mock_exists.side_effect = lambda path: path == template_path
                
                validation = self.exporter_agent._validate_expense_template(self.test_gstin)
                
                self.assertTrue(validation['available'])
                self.assertIn('template_name', validation)
                self.assertIn('fallback', validation.get('note', ''))
    
    def test_validate_expense_template_not_exists(self):
        """Test template validation when template doesn't exist."""
        with patch('os.path.exists', return_value=False):
            validation = self.exporter_agent._validate_expense_template('INVALID_GSTIN')
            
            self.assertFalse(validation['available'])
            self.assertIn('error', validation)
    
    def test_convert_expenses_to_x2beta(self):
        """Test conversion of expenses to X2Beta format."""
        mapped_expenses = [
            MappedExpense(
                id=str(uuid.uuid4()),
                run_id=str(self.test_run_id),
                channel=self.test_channel,
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
        
        x2beta_df = self.exporter_agent._convert_expenses_to_x2beta(
            mapped_expenses, self.test_gstin, self.test_month
        )
        
        self.assertIsInstance(x2beta_df, pd.DataFrame)
        self.assertGreater(len(x2beta_df), 0)
        
        # Should have multiple entries per expense (expense + GST + payable)
        self.assertGreaterEqual(len(x2beta_df), 3)  # At least 3 entries
        
        # Check required columns
        required_columns = ['Date', 'Voucher No.', 'Party Ledger', 'Total Amount']
        for col in required_columns:
            self.assertIn(col, x2beta_df.columns)
        
        # Check voucher balancing (total should be zero)
        voucher_total = x2beta_df['Total Amount'].sum()
        self.assertAlmostEqual(voucher_total, 0.0, places=2)
    
    def test_convert_expenses_interstate_gst(self):
        """Test conversion with interstate GST (IGST)."""
        mapped_expenses = [
            MappedExpense(
                id=str(uuid.uuid4()),
                run_id=str(self.test_run_id),
                channel=self.test_channel,
                gstin=self.test_gstin,
                invoice_no='AMZ-FEE-002',
                invoice_date=datetime(2025, 8, 21),
                expense_type='Commission',
                ledger_name='Amazon Commission',
                taxable_value=2000.0,
                gst_rate=0.18,
                cgst_amount=0.0,
                sgst_amount=0.0,
                igst_amount=360.0,
                total_value=2360.0,
                voucher_no='EXP0625080002'
            )
        ]
        
        x2beta_df = self.exporter_agent._convert_expenses_to_x2beta(
            mapped_expenses, self.test_gstin, self.test_month
        )
        
        # Check IGST entry exists
        igst_entries = x2beta_df[x2beta_df['IGST Amount'] > 0]
        self.assertGreater(len(igst_entries), 0)
        self.assertEqual(igst_entries.iloc[0]['IGST Amount'], 360.0)
    
    def test_generate_export_file_simple(self):
        """Test export file generation with simple Excel format."""
        x2beta_data = {
            'Date': ['01-08-2025'],
            'Voucher No.': ['EXP0625080001'],
            'Voucher Type': ['Purchase'],
            'Party Ledger': ['Amazon Closing Fee'],
            'Total Amount': [1000.0]
        }
        x2beta_df = pd.DataFrame(x2beta_data)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock template not existing to force simple export
            with patch('os.path.exists', return_value=False):
                export_path = self.exporter_agent._generate_export_file(
                    x2beta_df, self.test_gstin, self.test_channel, self.test_month, temp_dir
                )
                
                self.assertTrue(os.path.exists(export_path))
                self.assertTrue(export_path.endswith('.xlsx'))
                
                # Verify file content
                df_check = pd.read_excel(export_path)
                self.assertEqual(len(df_check), 1)
                self.assertIn('Date', df_check.columns)
    
    def test_export_expenses_success(self):
        """Test successful expense export."""
        # Mock mapped expenses
        with patch.object(self.exporter_agent.expense_mapper, 'get_mapped_expenses_for_export') as mock_get:
            mock_get.return_value = [
                MappedExpense(
                    id=str(uuid.uuid4()),
                    run_id=str(self.test_run_id),
                    channel=self.test_channel,
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
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Mock template validation
                with patch.object(self.exporter_agent, '_validate_expense_template') as mock_validate:
                    mock_validate.return_value = {'available': True, 'template_name': 'test.xlsx'}
                    
                    result = self.exporter_agent.export_expenses_to_x2beta(
                        self.test_run_id, self.test_gstin, self.test_channel, self.test_month, temp_dir
                    )
                    
                    self.assertIsInstance(result, ExpenseExportResult)
                    self.assertTrue(result.success)
                    self.assertEqual(result.exported_files, 1)
                    self.assertEqual(result.total_records, 1)
                    self.assertEqual(result.total_taxable, 1000.0)
                    self.assertEqual(result.total_tax, 180.0)
    
    def test_export_expenses_no_data(self):
        """Test expense export with no data."""
        # Mock empty expenses
        with patch.object(self.exporter_agent.expense_mapper, 'get_mapped_expenses_for_export') as mock_get:
            mock_get.return_value = []
            
            with tempfile.TemporaryDirectory() as temp_dir:
                result = self.exporter_agent.export_expenses_to_x2beta(
                    self.test_run_id, self.test_gstin, self.test_channel, self.test_month, temp_dir
                )
                
                self.assertTrue(result.success)
                self.assertEqual(result.exported_files, 0)
                self.assertEqual(result.total_records, 0)
                self.assertIn('No expenses found', result.error_message)
    
    def test_export_expenses_template_not_available(self):
        """Test expense export when template not available."""
        # Mock template validation failure
        with patch.object(self.exporter_agent, '_validate_expense_template') as mock_validate:
            mock_validate.return_value = {'available': False, 'error': 'Template not found'}
            
            with tempfile.TemporaryDirectory() as temp_dir:
                result = self.exporter_agent.export_expenses_to_x2beta(
                    self.test_run_id, self.test_gstin, self.test_channel, self.test_month, temp_dir
                )
                
                self.assertFalse(result.success)
                self.assertEqual(result.exported_files, 0)
                self.assertIn('Template not found', result.error_message)
    
    def test_create_combined_sales_expense_export(self):
        """Test creating combined sales + expense export."""
        # Create temporary sales file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as sales_file:
            sales_data = pd.DataFrame({
                'Date': ['01-08-2025'],
                'Voucher No.': ['SL0625080001'],
                'Party Ledger': ['Sales Account'],
                'Total Amount': [1000.0]
            })
            sales_data.to_excel(sales_file.name, index=False)
            
            # Mock expense export
            with patch.object(self.exporter_agent, 'export_expenses_to_x2beta') as mock_export:
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as expense_file:
                    expense_data = pd.DataFrame({
                        'Date': ['02-08-2025'],
                        'Voucher No.': ['EXP0625080001'],
                        'Party Ledger': ['Amazon Closing Fee'],
                        'Total Amount': [1180.0]
                    })
                    expense_data.to_excel(expense_file.name, index=False)
                    
                    mock_export.return_value = ExpenseExportResult(
                        success=True,
                        exported_files=1,
                        export_paths=[expense_file.name],
                        total_records=1,
                        total_taxable=1000.0,
                        total_tax=180.0,
                        expense_types_processed=['Closing Fee']
                    )
                    
                    with tempfile.TemporaryDirectory() as temp_dir:
                        result = self.exporter_agent.create_combined_sales_expense_export(
                            self.test_run_id, self.test_gstin, self.test_channel, 
                            self.test_month, temp_dir, sales_file.name
                        )
                        
                        self.assertTrue(result.success)
                        self.assertEqual(result.exported_files, 2)  # Original + combined
                        
                        # Check combined file exists
                        combined_files = [p for p in result.export_paths if 'combined' in p]
                        self.assertEqual(len(combined_files), 1)
                        
                        # Verify combined file content
                        combined_df = pd.read_excel(combined_files[0])
                        self.assertEqual(len(combined_df), 2)  # Sales + expense records
            
            # Clean up
            os.unlink(sales_file.name)
    
    def test_validate_expense_export_success(self):
        """Test successful expense export validation."""
        # Create valid export file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            export_data = pd.DataFrame({
                'Date': ['01-08-2025', '01-08-2025', '01-08-2025'],
                'Voucher No.': ['EXP001', 'EXP001', 'EXP001'],
                'Party Ledger': ['Amazon Closing Fee', 'Input IGST @ 18%', 'Amazon Payable'],
                'Total Amount': [1000.0, 180.0, -1180.0]  # Balanced entries
            })
            export_data.to_excel(tmp_file.name, index=False)
            
            is_valid, errors = self.exporter_agent.validate_expense_export(tmp_file.name)
            
            self.assertTrue(is_valid)
            self.assertEqual(len(errors), 0)
            
            # Clean up
            os.unlink(tmp_file.name)
    
    def test_validate_expense_export_unbalanced(self):
        """Test expense export validation with unbalanced entries."""
        # Create unbalanced export file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            export_data = pd.DataFrame({
                'Date': ['01-08-2025', '01-08-2025'],
                'Voucher No.': ['EXP001', 'EXP001'],
                'Party Ledger': ['Amazon Closing Fee', 'Input IGST @ 18%'],
                'Total Amount': [1000.0, 180.0]  # Unbalanced - missing credit entry
            })
            export_data.to_excel(tmp_file.name, index=False)
            
            is_valid, errors = self.exporter_agent.validate_expense_export(tmp_file.name)
            
            self.assertFalse(is_valid)
            self.assertGreater(len(errors), 0)
            self.assertTrue(any('balance' in error.lower() for error in errors))
            
            # Clean up
            os.unlink(tmp_file.name)
    
    def test_validate_expense_export_file_not_found(self):
        """Test expense export validation with missing file."""
        is_valid, errors = self.exporter_agent.validate_expense_export('/non/existent/file.xlsx')
        
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any('not found' in error for error in errors))
    
    def test_get_export_summary_no_db(self):
        """Test export summary without database."""
        exporter_agent = ExpenseTallyExporterAgent(None)
        summary = exporter_agent.get_export_summary(self.test_run_id, self.test_gstin)
        
        self.assertIn('error', summary)
        self.assertEqual(summary['error'], 'No database connection')
    
    def test_get_export_summary_with_db(self):
        """Test export summary with database."""
        # Mock database response
        mock_result = Mock()
        mock_result.data = [
            {
                'file_path': '/path/to/export1.xlsx',
                'record_count': 5,
                'total_taxable': 5000.0,
                'total_tax': 900.0,
                'created_at': '2025-08-20T10:00:00'
            },
            {
                'file_path': '/path/to/export2.xlsx',
                'record_count': 3,
                'total_taxable': 3000.0,
                'total_tax': 540.0,
                'created_at': '2025-08-21T11:00:00'
            }
        ]
        
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = mock_result
        
        summary = self.exporter_agent.get_export_summary(self.test_run_id, self.test_gstin)
        
        self.assertEqual(summary['total_exports'], 2)
        self.assertEqual(summary['total_records'], 8)
        self.assertEqual(summary['total_amount'], 9440.0)  # (5000+900) + (3000+540)
        self.assertEqual(len(summary['export_files']), 2)


class TestExpenseExportResult(unittest.TestCase):
    """Test cases for ExpenseExportResult dataclass."""
    
    def test_export_result_creation(self):
        """Test creation of ExpenseExportResult."""
        result = ExpenseExportResult(
            success=True,
            exported_files=2,
            export_paths=['/path1.xlsx', '/path2.xlsx'],
            total_records=10,
            total_taxable=5000.0,
            total_tax=900.0,
            expense_types_processed=['Closing Fee', 'Commission']
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.exported_files, 2)
        self.assertEqual(len(result.export_paths), 2)
        self.assertEqual(result.total_records, 10)
        self.assertEqual(result.total_taxable, 5000.0)
        self.assertEqual(result.total_tax, 900.0)
        self.assertEqual(len(result.expense_types_processed), 2)
        self.assertIsNone(result.error_message)
    
    def test_export_result_with_error(self):
        """Test creation of ExpenseExportResult with error."""
        result = ExpenseExportResult(
            success=False,
            exported_files=0,
            export_paths=[],
            total_records=0,
            total_taxable=0.0,
            total_tax=0.0,
            expense_types_processed=[],
            error_message="Export failed"
        )
        
        self.assertFalse(result.success)
        self.assertEqual(result.exported_files, 0)
        self.assertEqual(result.error_message, "Export failed")


if __name__ == '__main__':
    unittest.main()
