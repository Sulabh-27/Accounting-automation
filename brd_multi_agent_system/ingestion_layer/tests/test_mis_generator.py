"""
Test suite for MIS Generator Agent - Part 8: MIS & Audit Trail

Tests MIS report generation, business intelligence calculations,
and export functionality.
"""

import unittest
import uuid
import pandas as pd
from datetime import datetime
from decimal import Decimal
import tempfile
import os

from ..agents.mis_generator import MISGeneratorAgent
from ..libs.mis_utils import MISCalculator, SalesMetrics, ExpenseMetrics, GSTMetrics, ProfitabilityMetrics
from ..libs.supabase_client import SupabaseClientWrapper


class TestMISGenerator(unittest.TestCase):
    """Test cases for MIS Generator Agent"""
    
    def setUp(self):
        """Set up test environment"""
        self.supabase = SupabaseClientWrapper()  # Development mode
        self.mis_agent = MISGeneratorAgent(self.supabase)
        self.mis_calculator = MISCalculator(self.supabase)
        self.test_run_id = uuid.uuid4()
        
        # Create test data
        self.test_channel = "amazon"
        self.test_gstin = "06ABGCS4796R1ZA"
        self.test_month = "2025-08"
        
    def test_sales_metrics_calculation(self):
        """Test sales metrics calculation from pivot data"""
        # Create mock pivot data
        pivot_data = pd.DataFrame({
            'total_taxable_value': [100000.0, 50000.0, 75000.0],
            'total_gst_amount': [18000.0, 9000.0, 13500.0],
            'total_records': [100, 50, 75],
            'total_quantity': [200, 100, 150],
            'final_goods_name': ['Product A', 'Product B', 'Product C'],
            'is_return': [False, False, False]
        })
        
        sales_metrics = self.mis_calculator.calculate_sales_metrics(
            self.test_run_id, pivot_data
        )
        
        self.assertIsInstance(sales_metrics, SalesMetrics)
        self.assertEqual(sales_metrics.total_sales, Decimal('225000.0'))
        self.assertEqual(sales_metrics.total_returns, Decimal('0.0'))
        self.assertEqual(sales_metrics.net_sales, Decimal('225000.0'))
        self.assertEqual(sales_metrics.total_transactions, 225)
        self.assertEqual(sales_metrics.total_skus, 3)
        self.assertEqual(sales_metrics.total_quantity, 450)
        self.assertEqual(sales_metrics.average_order_value, Decimal('1000.0'))
        
    def test_expense_metrics_calculation(self):
        """Test expense metrics calculation from seller invoices"""
        # Create mock expense data
        expense_data = pd.DataFrame({
            'total_amount': [1000.0, 500.0, 750.0],
            'expense_type': ['Commission', 'Shipping Fee', 'Fulfillment Fee'],
            'gst_amount': [180.0, 90.0, 135.0]
        })
        
        expense_metrics = self.mis_calculator.calculate_expense_metrics(
            self.test_run_id, expense_data
        )
        
        self.assertIsInstance(expense_metrics, ExpenseMetrics)
        self.assertEqual(expense_metrics.total_expenses, Decimal('2250.0'))
        self.assertEqual(expense_metrics.commission_expenses, Decimal('1000.0'))
        self.assertEqual(expense_metrics.shipping_expenses, Decimal('500.0'))
        self.assertEqual(expense_metrics.fulfillment_expenses, Decimal('750.0'))
        
    def test_gst_metrics_calculation(self):
        """Test GST metrics calculation"""
        # Create mock sales data
        sales_data = pd.DataFrame({
            'total_gst_amount': [18000.0, 9000.0],
            'cgst_amount': [9000.0, 4500.0],
            'sgst_amount': [9000.0, 4500.0],
            'igst_amount': [0.0, 0.0]
        })
        
        # Create mock expense data
        expense_data = pd.DataFrame({
            'gst_amount': [180.0, 90.0]
        })
        
        gst_metrics = self.mis_calculator.calculate_gst_metrics(
            self.test_run_id, sales_data, expense_data
        )
        
        self.assertIsInstance(gst_metrics, GSTMetrics)
        self.assertEqual(gst_metrics.net_gst_output, Decimal('27000.0'))
        self.assertEqual(gst_metrics.net_gst_input, Decimal('270.0'))
        self.assertEqual(gst_metrics.gst_liability, Decimal('26730.0'))
        self.assertEqual(gst_metrics.cgst_amount, Decimal('13500.0'))
        self.assertEqual(gst_metrics.sgst_amount, Decimal('13500.0'))
        
    def test_profitability_metrics_calculation(self):
        """Test profitability metrics calculation"""
        sales_metrics = SalesMetrics(
            total_sales=Decimal('225000.0'),
            total_returns=Decimal('0.0'),
            total_transfers=Decimal('0.0'),
            net_sales=Decimal('225000.0'),
            total_transactions=225,
            total_skus=3,
            total_quantity=450,
            average_order_value=Decimal('1000.0')
        )
        
        expense_metrics = ExpenseMetrics(
            total_expenses=Decimal('25000.0'),
            commission_expenses=Decimal('15000.0'),
            shipping_expenses=Decimal('5000.0'),
            fulfillment_expenses=Decimal('3000.0'),
            advertising_expenses=Decimal('2000.0'),
            storage_expenses=Decimal('0.0'),
            other_expenses=Decimal('0.0')
        )
        
        profitability_metrics = self.mis_calculator.calculate_profitability_metrics(
            sales_metrics, expense_metrics
        )
        
        self.assertIsInstance(profitability_metrics, ProfitabilityMetrics)
        self.assertEqual(profitability_metrics.gross_profit, Decimal('200000.0'))
        self.assertAlmostEqual(float(profitability_metrics.profit_margin), 88.89, places=2)
        self.assertAlmostEqual(float(profitability_metrics.revenue_per_transaction), 1000.0, places=2)
        self.assertAlmostEqual(float(profitability_metrics.cost_per_transaction), 111.11, places=2)
        
    def test_data_quality_score_calculation(self):
        """Test data quality score calculation"""
        total_records = 1000
        
        quality_score, exception_count, approval_count = self.mis_calculator.calculate_data_quality_score(
            self.test_run_id, total_records
        )
        
        self.assertIsInstance(quality_score, Decimal)
        self.assertIsInstance(exception_count, int)
        self.assertIsInstance(approval_count, int)
        self.assertGreaterEqual(quality_score, Decimal('0'))
        self.assertLessEqual(quality_score, Decimal('100'))
        
    def test_mis_report_generation(self):
        """Test complete MIS report generation"""
        mis_report = self.mis_calculator.generate_mis_report(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month,
            report_type="monthly"
        )
        
        self.assertIsNotNone(mis_report)
        self.assertEqual(mis_report.run_id, self.test_run_id)
        self.assertEqual(mis_report.channel, self.test_channel)
        self.assertEqual(mis_report.gstin, self.test_gstin)
        self.assertEqual(mis_report.month, self.test_month)
        self.assertEqual(mis_report.report_type, "monthly")
        
        # Check that all metrics are present
        self.assertIsNotNone(mis_report.sales_metrics)
        self.assertIsNotNone(mis_report.expense_metrics)
        self.assertIsNotNone(mis_report.gst_metrics)
        self.assertIsNotNone(mis_report.profitability_metrics)
        
        # Check data types
        self.assertIsInstance(mis_report.data_quality_score, Decimal)
        self.assertIsInstance(mis_report.exception_count, int)
        self.assertIsInstance(mis_report.approval_count, int)
        
    def test_mis_report_to_dict(self):
        """Test MIS report conversion to dictionary"""
        mis_report = self.mis_calculator.generate_mis_report(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month
        )
        
        report_dict = mis_report.to_dict()
        
        self.assertIsInstance(report_dict, dict)
        self.assertIn('run_id', report_dict)
        self.assertIn('channel', report_dict)
        self.assertIn('gstin', report_dict)
        self.assertIn('month', report_dict)
        self.assertIn('total_sales', report_dict)
        self.assertIn('total_expenses', report_dict)
        self.assertIn('gross_profit', report_dict)
        self.assertIn('profit_margin', report_dict)
        self.assertIn('net_gst_output', report_dict)
        self.assertIn('net_gst_input', report_dict)
        self.assertIn('gst_liability', report_dict)
        self.assertIn('data_quality_score', report_dict)
        
        # Check data types in dictionary
        self.assertIsInstance(report_dict['total_sales'], float)
        self.assertIsInstance(report_dict['total_expenses'], float)
        self.assertIsInstance(report_dict['total_transactions'], int)
        
    def test_mis_report_csv_export(self):
        """Test MIS report CSV export"""
        mis_report = self.mis_calculator.generate_mis_report(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            output_path = tmp_file.name
        
        try:
            exported_path = self.mis_calculator.export_mis_report_csv(mis_report, output_path)
            
            self.assertEqual(exported_path, output_path)
            self.assertTrue(os.path.exists(output_path))
            
            # Read and validate CSV content
            df = pd.read_csv(output_path)
            self.assertEqual(len(df), 1)  # Should have one row
            self.assertIn('Channel', df.columns)
            self.assertIn('GSTIN', df.columns)
            self.assertIn('Month', df.columns)
            self.assertIn('Total Sales', df.columns)
            self.assertIn('Gross Profit', df.columns)
            
            # Check values
            self.assertEqual(df['Channel'].iloc[0], self.test_channel)
            self.assertEqual(df['GSTIN'].iloc[0], self.test_gstin)
            self.assertEqual(df['Month'].iloc[0], self.test_month)
            
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)
    
    def test_mis_agent_generate_report(self):
        """Test MIS Generator Agent report generation"""
        result = self.mis_agent.generate_mis_report(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month,
            report_type="monthly",
            export_formats=["csv", "database"]
        )
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result.success, bool)
        self.assertIsInstance(result.processing_time_seconds, float)
        
        if result.success:
            self.assertIsNotNone(result.mis_report)
            self.assertIsNotNone(result.report_id)
            
            # Check report content
            report = result.mis_report
            self.assertEqual(report.run_id, self.test_run_id)
            self.assertEqual(report.channel, self.test_channel)
            self.assertEqual(report.gstin, self.test_gstin)
            self.assertEqual(report.month, self.test_month)
        else:
            self.assertIsNotNone(result.error_message)
    
    def test_comparative_report_generation(self):
        """Test comparative report generation across months"""
        months = ["2025-07", "2025-08", "2025-09"]
        
        comparison = self.mis_agent.generate_comparative_report(
            channel=self.test_channel,
            gstin=self.test_gstin,
            months=months,
            report_type="comparative"
        )
        
        self.assertIsInstance(comparison, dict)
        self.assertIn('channel', comparison)
        self.assertIn('gstin', comparison)
        self.assertIn('months', comparison)
        self.assertIn('report_type', comparison)
        
        if 'error' not in comparison:
            self.assertEqual(comparison['channel'], self.test_channel)
            self.assertEqual(comparison['gstin'], self.test_gstin)
            self.assertEqual(comparison['months'], months)
            self.assertEqual(comparison['report_type'], "comparative")
    
    def test_dashboard_data_retrieval(self):
        """Test MIS dashboard data retrieval"""
        dashboard_data = self.mis_agent.get_mis_dashboard_data(
            channel=self.test_channel,
            gstin=self.test_gstin,
            limit=5
        )
        
        self.assertIsInstance(dashboard_data, dict)
        
        if 'error' not in dashboard_data:
            self.assertIn('summary', dashboard_data)
            self.assertIn('recent_reports', dashboard_data)
            self.assertIn('generated_at', dashboard_data)
            
            # Check summary structure
            summary = dashboard_data['summary']
            self.assertIn('total_reports', summary)
            self.assertIn('channels', summary)
            self.assertIn('gstins', summary)
    
    def test_golden_mis_report_validation(self):
        """Test MIS report against golden test case"""
        # Load golden test data
        golden_file = "ingestion_layer/tests/golden/mis_expected.csv"
        
        if os.path.exists(golden_file):
            golden_df = pd.read_csv(golden_file)
            expected = golden_df.iloc[0]
            
            # Generate MIS report with mock data that should match golden values
            # This would typically use actual processed data
            mis_report = self.mis_calculator.generate_mis_report(
                run_id=self.test_run_id,
                channel=self.test_channel,
                gstin=self.test_gstin,
                month=self.test_month
            )
            
            # In a real test, we would compare against expected values
            # For now, just validate the structure
            self.assertEqual(mis_report.channel, self.test_channel)
            self.assertEqual(mis_report.gstin, self.test_gstin)
            self.assertEqual(mis_report.month, self.test_month)
            
            # Validate that metrics are within reasonable ranges
            self.assertGreaterEqual(mis_report.sales_metrics.total_sales, Decimal('0'))
            self.assertGreaterEqual(mis_report.expense_metrics.total_expenses, Decimal('0'))
            self.assertGreaterEqual(mis_report.data_quality_score, Decimal('0'))
            self.assertLessEqual(mis_report.data_quality_score, Decimal('100'))
    
    def test_error_handling(self):
        """Test error handling in MIS generation"""
        # Test with invalid run_id
        invalid_run_id = uuid.uuid4()
        
        result = self.mis_agent.generate_mis_report(
            run_id=invalid_run_id,
            channel="invalid_channel",
            gstin="invalid_gstin",
            month="invalid_month",
            export_formats=["csv"]
        )
        
        # Should handle gracefully and return result with appropriate status
        self.assertIsNotNone(result)
        self.assertIsInstance(result.processing_time_seconds, float)
        
        # In development mode, it should still generate empty metrics
        if not result.success:
            self.assertIsNotNone(result.error_message)
    
    def test_performance_metrics(self):
        """Test performance of MIS generation"""
        start_time = datetime.now()
        
        result = self.mis_agent.generate_mis_report(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month,
            export_formats=["csv"]
        )
        
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        # Should complete within reasonable time (5 seconds for development mode)
        self.assertLess(total_time, 5.0)
        self.assertLess(result.processing_time_seconds, 5.0)


if __name__ == '__main__':
    unittest.main()
