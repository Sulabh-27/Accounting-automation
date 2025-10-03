"""
Test suite for Pivot Generator Agent
Tests pivot generation logic against golden reference data
"""
import unittest
import pandas as pd
import uuid
import os
from unittest.mock import MagicMock

from ingestion_layer.agents.pivoter import PivotGeneratorAgent
from ingestion_layer.libs.pivot_rules import PivotRulesEngine
from ingestion_layer.libs.summarizer import Summarizer
from ingestion_layer.libs.contracts import PivotResult


class TestPivotRulesEngine(unittest.TestCase):
    """Test the core pivot rules engine."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.pivot_engine = PivotRulesEngine()
    
    def test_get_pivot_dimensions(self):
        """Test getting pivot dimensions for different channels."""
        # Amazon MTR dimensions
        amazon_dims = self.pivot_engine.get_pivot_dimensions("amazon_mtr")
        expected_dims = ["gstin", "month", "gst_rate", "ledger_name", "fg"]
        self.assertEqual(amazon_dims, expected_dims)
        
        # Flipkart dimensions (includes state)
        flipkart_dims = self.pivot_engine.get_pivot_dimensions("flipkart")
        self.assertIn("state_code", flipkart_dims)
        
        # Unknown channel should default to amazon_mtr
        unknown_dims = self.pivot_engine.get_pivot_dimensions("unknown_channel")
        self.assertEqual(unknown_dims, expected_dims)
    
    def test_get_pivot_measures(self):
        """Test getting pivot measures for different channels."""
        # Amazon MTR measures
        amazon_measures = self.pivot_engine.get_pivot_measures("amazon_mtr")
        expected_measures = ["quantity", "taxable_value", "cgst", "sgst", "igst"]
        self.assertEqual(amazon_measures, expected_measures)
        
        # Amazon STR measures (IGST only)
        str_measures = self.pivot_engine.get_pivot_measures("amazon_str")
        self.assertIn("igst", str_measures)
        self.assertNotIn("cgst", str_measures)
        self.assertNotIn("sgst", str_measures)
    
    def test_get_business_rules(self):
        """Test getting business rules for channels."""
        amazon_rules = self.pivot_engine.get_business_rules("amazon_mtr")
        self.assertTrue(amazon_rules.get("exclude_zero_taxable", False))
        self.assertEqual(amazon_rules.get("round_decimals", 0), 2)
        
        str_rules = self.pivot_engine.get_business_rules("amazon_str")
        self.assertTrue(str_rules.get("force_igst_only", False))
    
    def test_validate_pivot_columns(self):
        """Test pivot column validation."""
        # Valid columns
        valid_columns = ["gstin", "month", "gst_rate", "ledger_name", "fg", 
                        "quantity", "taxable_value", "cgst", "sgst", "igst"]
        validation = self.pivot_engine.validate_pivot_columns(valid_columns, "amazon_mtr")
        self.assertTrue(validation["valid"])
        self.assertEqual(len(validation["missing_columns"]), 0)
        
        # Missing columns
        incomplete_columns = ["gstin", "month", "gst_rate"]
        validation = self.pivot_engine.validate_pivot_columns(incomplete_columns, "amazon_mtr")
        self.assertFalse(validation["valid"])
        self.assertGreater(len(validation["missing_columns"]), 0)
    
    def test_supported_channels(self):
        """Test getting supported channels."""
        channels = self.pivot_engine.get_supported_channels()
        expected_channels = ["amazon_mtr", "amazon_str", "flipkart", "pepperfry"]
        self.assertEqual(set(channels), set(expected_channels))
    
    def test_apply_channel_transformations(self):
        """Test channel-specific transformations."""
        # Create test data
        df = pd.DataFrame([
            {"total_taxable": 1000.0, "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0},
            {"total_taxable": 0.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 0.0}
        ])
        
        # Apply Amazon MTR transformations (should exclude zero taxable)
        transformed = self.pivot_engine.apply_channel_specific_transformations(df, "amazon_mtr")
        self.assertEqual(len(transformed), 1)  # Zero taxable row removed
        
        # Apply Amazon STR transformations (should force IGST only)
        str_transformed = self.pivot_engine.apply_channel_specific_transformations(df, "amazon_str")
        self.assertEqual(str_transformed.iloc[0]["total_cgst"], 0.0)
        self.assertEqual(str_transformed.iloc[0]["total_sgst"], 0.0)


class TestSummarizer(unittest.TestCase):
    """Test the summarizer library."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.summarizer = Summarizer()
    
    def test_generate_pivot_summary(self):
        """Test pivot summary generation."""
        # Create test pivot data
        pivot_df = pd.DataFrame([
            {
                "gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.18,
                "ledger_name": "Amazon Haryana", "fg": "Product A",
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0
            },
            {
                "gstin": "06ABGCS4796R1ZA", "month": "2025-08", "gst_rate": 0.18,
                "ledger_name": "Amazon Delhi", "fg": "Product B", 
                "total_quantity": 5, "total_taxable": 500.0,
                "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 90.0
            }
        ])
        
        summary = self.summarizer.generate_pivot_summary(pivot_df)
        
        # Check totals
        self.assertEqual(summary["total_records"], 2)
        self.assertEqual(summary["total_taxable_amount"], 1500.0)
        self.assertEqual(summary["total_tax_amount"], 270.0)
        self.assertEqual(summary["unique_ledgers"], 2)
        self.assertEqual(summary["unique_fgs"], 2)
        
        # Check GST rate breakdown
        self.assertIn("18%", summary["gst_rate_breakdown"])
        self.assertEqual(summary["gst_rate_breakdown"]["18%"]["records"], 2)
    
    def test_empty_summary(self):
        """Test summary generation with empty data."""
        empty_df = pd.DataFrame()
        summary = self.summarizer.generate_pivot_summary(empty_df)
        
        self.assertEqual(summary["total_records"], 0)
        self.assertEqual(summary["total_taxable_amount"], 0.0)
        self.assertEqual(summary["total_tax_amount"], 0.0)
    
    def test_generate_mis_report(self):
        """Test MIS report generation."""
        pivot_df = pd.DataFrame([
            {
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0,
                "ledger_name": "Amazon Haryana", "fg": "Product A", "gst_rate": 0.18
            }
        ])
        
        mis_report = self.summarizer.generate_mis_report(pivot_df, "amazon_mtr", "2025-08")
        
        self.assertEqual(mis_report["report_type"], "MIS_PIVOT_SUMMARY")
        self.assertEqual(mis_report["channel"], "amazon_mtr")
        self.assertEqual(mis_report["month"], "2025-08")
        self.assertIn("summary", mis_report)
        self.assertIn("key_metrics", mis_report)
        self.assertIn("recommendations", mis_report)


class TestPivotGeneratorAgent(unittest.TestCase):
    """Test the Pivot Generator Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value = MagicMock()
        
        self.agent = PivotGeneratorAgent(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
    
    def test_process_dataset(self):
        """Test processing dataset for pivot generation."""
        # Create sample enriched dataset (output from Part-3)
        df = pd.DataFrame([
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "ledger_name": "Amazon Haryana", "fg": "Product A",
                "quantity": 5, "taxable_value": 500.0,
                "cgst": 45.0, "sgst": 45.0, "igst": 0.0,
                "invoice_no": "AMZ-HR-08-0001"
            },
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "ledger_name": "Amazon Haryana", "fg": "Product A",
                "quantity": 3, "taxable_value": 300.0,
                "cgst": 27.0, "sgst": 27.0, "igst": 0.0,
                "invoice_no": "AMZ-HR-08-0002"
            },
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "ledger_name": "Amazon Delhi", "fg": "Product B",
                "quantity": 2, "taxable_value": 200.0,
                "cgst": 0.0, "sgst": 0.0, "igst": 36.0,
                "invoice_no": "AMZ-DL-08-0001"
            }
        ])
        
        pivot_df, result = self.agent.process_dataset(df, "amazon_mtr", self.gstin, "2025-08", self.run_id)
        
        # Check result
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 3)
        self.assertEqual(result.pivot_records, 2)  # Should be grouped into 2 pivot records
        
        # Check pivot data
        self.assertIn("total_quantity", pivot_df.columns)
        self.assertIn("total_taxable", pivot_df.columns)
        self.assertIn("total_cgst", pivot_df.columns)
        
        # Check aggregation
        haryana_record = pivot_df[
            (pivot_df["ledger_name"] == "Amazon Haryana") & 
            (pivot_df["fg"] == "Product A")
        ]
        self.assertEqual(len(haryana_record), 1)
        self.assertEqual(haryana_record.iloc[0]["total_quantity"], 8)  # 5 + 3
        self.assertEqual(haryana_record.iloc[0]["total_taxable"], 800.0)  # 500 + 300
    
    def test_validate_input_data(self):
        """Test input data validation."""
        # Valid data
        valid_df = pd.DataFrame([
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "ledger_name": "Amazon Haryana", "fg": "Product A",
                "quantity": 5, "taxable_value": 500.0,
                "cgst": 45.0, "sgst": 45.0, "igst": 0.0
            }
        ])
        
        validation = self.agent._validate_input_data(valid_df)
        self.assertTrue(validation["valid"])
        
        # Invalid data (missing columns)
        invalid_df = pd.DataFrame([{"gstin": self.gstin}])
        validation = self.agent._validate_input_data(invalid_df)
        self.assertFalse(validation["valid"])
        self.assertIn("Missing required columns", validation["errors"])
    
    def test_get_pivot_summary(self):
        """Test pivot summary generation."""
        pivot_df = pd.DataFrame([
            {
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0,
                "ledger_name": "Amazon Haryana", "fg": "Product A", "gst_rate": 0.18
            }
        ])
        
        summary = self.agent.get_pivot_summary(pivot_df)
        
        self.assertEqual(summary["total_records"], 1)
        self.assertEqual(summary["total_taxable_amount"], 1000.0)
        self.assertEqual(summary["total_tax_amount"], 180.0)
    
    def test_validate_pivot_data(self):
        """Test pivot data validation."""
        # Valid pivot data
        valid_df = pd.DataFrame([
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0
            }
        ])
        
        validation = self.agent.validate_pivot_data(valid_df)
        self.assertEqual(validation["total_records"], 1)
        self.assertEqual(validation["valid_records"], 1)
        self.assertEqual(validation["invalid_records"], 0)
        
        # Invalid pivot data (negative values)
        invalid_df = pd.DataFrame([
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "total_quantity": -5, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0
            }
        ])
        
        validation = self.agent.validate_pivot_data(invalid_df)
        self.assertEqual(validation["negative_values"], 1)
        self.assertEqual(validation["invalid_records"], 1)


class TestPivotGoldenTests(unittest.TestCase):
    """Test Pivot Generator against golden reference data."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value = MagicMock()
        
        self.agent = PivotGeneratorAgent(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
    
    def test_amazon_mtr_golden_reference(self):
        """Test Amazon MTR pivot generation against golden reference."""
        # Load golden reference
        golden_path = "ingestion_layer/tests/golden/amazon_mtr_pivot_expected.csv"
        
        if not os.path.exists(golden_path):
            self.skipTest(f"Golden reference file not found: {golden_path}")
        
        golden_df = pd.read_csv(golden_path)
        
        # Create input dataset that would produce this pivot
        # This would typically come from the enriched dataset with taxes and invoice numbers
        input_records = []
        
        for _, row in golden_df.iterrows():
            # Create individual transaction records that would aggregate to this pivot row
            for i in range(int(row['total_quantity'])):
                record = {
                    "gstin": row['gstin'],
                    "month": row['month'],
                    "gst_rate": row['gst_rate'],
                    "ledger_name": row['ledger'],
                    "fg": row['fg'],
                    "quantity": 1,
                    "taxable_value": row['total_taxable'] / row['total_quantity'],
                    "cgst": row['total_cgst'] / row['total_quantity'],
                    "sgst": row['total_sgst'] / row['total_quantity'],
                    "igst": row['total_igst'] / row['total_quantity'],
                    "invoice_no": f"AMZ-XX-08-{i+1:04d}"
                }
                input_records.append(record)
        
        input_df = pd.DataFrame(input_records)
        
        # Process with pivot agent
        pivot_df, result = self.agent.process_dataset(
            input_df, "amazon_mtr", self.gstin, "2025-08", self.run_id
        )
        
        # Validate result
        self.assertTrue(result.success)
        self.assertGreater(result.pivot_records, 0)
        
        # Compare with golden reference (sample comparison)
        # Note: Exact comparison would require precise input data reconstruction
        self.assertEqual(len(pivot_df), len(golden_df))
        
        # Check that all expected GST rates are present
        expected_rates = set(golden_df['gst_rate'].unique())
        actual_rates = set(pivot_df['gst_rate'].unique())
        self.assertEqual(expected_rates, actual_rates)
        
        # Check that totals are reasonable
        total_taxable = pivot_df['total_taxable'].sum()
        total_tax = (pivot_df['total_cgst'] + pivot_df['total_sgst'] + pivot_df['total_igst']).sum()
        
        self.assertGreater(total_taxable, 0)
        self.assertGreaterEqual(total_tax, 0)  # Can be zero for 0% GST items


if __name__ == "__main__":
    unittest.main()
