"""
Test suite for Batch Splitter Agent
Tests batch splitting logic and GST rate separation
"""
import unittest
import pandas as pd
import uuid
import os
import tempfile
import shutil
from unittest.mock import MagicMock

from ingestion_layer.agents.batch_splitter import BatchSplitterAgent
from ingestion_layer.libs.contracts import BatchSplitResult


class TestBatchSplitterAgent(unittest.TestCase):
    """Test the Batch Splitter Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value = MagicMock()
        
        self.agent = BatchSplitterAgent(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
        
        # Create temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_process_pivot_data(self):
        """Test processing pivot data for batch splitting."""
        # Create sample pivot data with multiple GST rates
        pivot_df = pd.DataFrame([
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "ledger": "Amazon Haryana", "fg": "Product A",
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0
            },
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18,
                "ledger": "Amazon Delhi", "fg": "Product B",
                "total_quantity": 5, "total_taxable": 500.0,
                "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 90.0
            },
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.0,
                "ledger": "Amazon Delhi", "fg": "Product C",
                "total_quantity": 8, "total_taxable": 800.0,
                "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 0.0
            },
            {
                "gstin": self.gstin, "month": "2025-08", "gst_rate": 0.12,
                "ledger": "Amazon Karnataka", "fg": "Product D",
                "total_quantity": 3, "total_taxable": 300.0,
                "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 36.0
            }
        ])
        
        batch_files, result = self.agent.process_pivot_data(
            pivot_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        # Check result
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 4)
        self.assertEqual(result.batch_files_created, 3)  # 0%, 12%, 18%
        self.assertEqual(result.gst_rates_processed, 3)
        self.assertEqual(result.total_records_split, 4)
        self.assertTrue(result.validation_passed)
        
        # Check batch files were created
        self.assertEqual(len(batch_files), 3)
        
        expected_files = [
            "amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_batch.csv",
            "amazon_mtr_06ABGCS4796R1ZA_2025-08_12pct_batch.csv", 
            "amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv"
        ]
        
        for expected_file in expected_files:
            expected_path = os.path.join(self.temp_dir, expected_file)
            self.assertIn(expected_path, batch_files)
            self.assertTrue(os.path.exists(expected_path))
    
    def test_batch_file_content_validation(self):
        """Test that each batch file contains only one GST rate."""
        # Create pivot data with multiple GST rates
        pivot_df = pd.DataFrame([
            {"gst_rate": 0.18, "ledger": "Amazon A", "fg": "Product A", "total_taxable": 1000.0},
            {"gst_rate": 0.18, "ledger": "Amazon B", "fg": "Product B", "total_taxable": 500.0},
            {"gst_rate": 0.12, "ledger": "Amazon C", "fg": "Product C", "total_taxable": 300.0},
            {"gst_rate": 0.0, "ledger": "Amazon D", "fg": "Product D", "total_taxable": 200.0}
        ])
        
        batch_files, result = self.agent.process_pivot_data(
            pivot_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        # Validate each batch file
        validation = self.agent.validate_batch_files(batch_files)
        
        self.assertEqual(validation["files_validated"], len(batch_files))
        self.assertEqual(validation["files_missing"], 0)
        self.assertEqual(validation["files_empty"], 0)
        self.assertEqual(validation["gst_rate_violations"], 0)
        
        # Check individual files
        for file_path in batch_files:
            df = pd.read_csv(file_path)
            unique_rates = df['gst_rate'].nunique()
            self.assertEqual(unique_rates, 1, f"File {file_path} has multiple GST rates")
    
    def test_calculate_batch_summary(self):
        """Test batch summary calculation."""
        batch_df = pd.DataFrame([
            {
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0,
                "ledger_name": "Amazon Haryana", "fg": "Product A"
            },
            {
                "total_quantity": 5, "total_taxable": 500.0,
                "total_cgst": 45.0, "total_sgst": 45.0, "total_igst": 0.0,
                "ledger_name": "Amazon Haryana", "fg": "Product B"
            }
        ])
        
        summary = self.agent._calculate_batch_summary(batch_df, 0.18)
        
        self.assertEqual(summary["gst_rate"], 0.18)
        self.assertEqual(summary["record_count"], 2)
        self.assertEqual(summary["quantity"], 15.0)  # 10 + 5
        self.assertEqual(summary["taxable"], 1500.0)  # 1000 + 500
        self.assertEqual(summary["cgst"], 135.0)  # 90 + 45
        self.assertEqual(summary["sgst"], 135.0)  # 90 + 45
        self.assertEqual(summary["igst"], 0.0)
        self.assertEqual(summary["total_tax"], 270.0)  # 135 + 135 + 0
        self.assertEqual(summary["total_amount"], 1770.0)  # 1500 + 270
        self.assertEqual(summary["unique_ledgers"], 1)
        self.assertEqual(summary["unique_fgs"], 2)
    
    def test_validate_split_integrity(self):
        """Test split integrity validation."""
        # Original pivot data
        original_df = pd.DataFrame([
            {"total_taxable": 1000.0, "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0},
            {"total_taxable": 500.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 90.0},
            {"total_taxable": 300.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 36.0}
        ])
        
        # Batch summaries that should match the original
        batch_summaries = [
            {
                "gst_rate": 0.18, "record_count": 1, "taxable": 1000.0,
                "cgst": 90.0, "sgst": 90.0, "igst": 0.0, "total_tax": 180.0
            },
            {
                "gst_rate": 0.18, "record_count": 1, "taxable": 500.0,
                "cgst": 0.0, "sgst": 0.0, "igst": 90.0, "total_tax": 90.0
            },
            {
                "gst_rate": 0.12, "record_count": 1, "taxable": 300.0,
                "cgst": 0.0, "sgst": 0.0, "igst": 36.0, "total_tax": 36.0
            }
        ]
        
        validation = self.agent._validate_split_integrity(original_df, batch_summaries)
        
        self.assertTrue(validation["valid"])
        self.assertEqual(len(validation["errors"]), 0)
        self.assertEqual(validation["original_totals"]["record_count"], 3)
        self.assertEqual(validation["batch_totals"]["record_count"], 3)
        self.assertEqual(validation["original_totals"]["total_taxable"], 1800.0)
        self.assertEqual(validation["batch_totals"]["total_taxable"], 1800.0)
    
    def test_get_batch_summary(self):
        """Test overall batch summary generation."""
        batch_summaries = [
            {
                "gst_rate": 0.18, "record_count": 2, "taxable": 1500.0,
                "total_tax": 270.0
            },
            {
                "gst_rate": 0.12, "record_count": 1, "taxable": 300.0,
                "total_tax": 36.0
            },
            {
                "gst_rate": 0.0, "record_count": 1, "taxable": 200.0,
                "total_tax": 0.0
            }
        ]
        
        summary = self.agent.get_batch_summary(batch_summaries)
        
        self.assertEqual(summary["total_batches"], 3)
        self.assertEqual(summary["total_records"], 4)  # 2 + 1 + 1
        self.assertEqual(summary["total_taxable"], 2000.0)  # 1500 + 300 + 200
        self.assertEqual(summary["total_tax"], 306.0)  # 270 + 36 + 0
        self.assertEqual(set(summary["gst_rates"]), {0.0, 0.12, 0.18})
        self.assertEqual(len(summary["batch_breakdown"]), 3)
    
    def test_empty_pivot_data(self):
        """Test handling of empty pivot data."""
        empty_df = pd.DataFrame()
        
        batch_files, result = self.agent.process_pivot_data(
            empty_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        self.assertFalse(result.success)
        self.assertIn("No pivot data to split", result.error_message)
        self.assertEqual(len(batch_files), 0)
    
    def test_missing_gst_rate_column(self):
        """Test handling of missing GST rate column."""
        invalid_df = pd.DataFrame([
            {"ledger": "Amazon A", "fg": "Product A", "total_taxable": 1000.0}
        ])
        
        batch_files, result = self.agent.process_pivot_data(
            invalid_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        self.assertFalse(result.success)
        self.assertIn("GST rate column not found", result.error_message)
        self.assertEqual(len(batch_files), 0)
    
    def test_single_gst_rate(self):
        """Test splitting data with only one GST rate."""
        single_rate_df = pd.DataFrame([
            {
                "gst_rate": 0.18, "ledger": "Amazon A", "fg": "Product A",
                "total_quantity": 10, "total_taxable": 1000.0,
                "total_cgst": 90.0, "total_sgst": 90.0, "total_igst": 0.0
            },
            {
                "gst_rate": 0.18, "ledger": "Amazon B", "fg": "Product B",
                "total_quantity": 5, "total_taxable": 500.0,
                "total_cgst": 45.0, "total_sgst": 45.0, "total_igst": 0.0
            }
        ])
        
        batch_files, result = self.agent.process_pivot_data(
            single_rate_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.batch_files_created, 1)
        self.assertEqual(result.gst_rates_processed, 1)
        self.assertEqual(len(batch_files), 1)
        
        # Check file content
        batch_df = pd.read_csv(batch_files[0])
        self.assertEqual(len(batch_df), 2)
        self.assertEqual(batch_df['gst_rate'].nunique(), 1)
        self.assertEqual(batch_df['gst_rate'].iloc[0], 0.18)
    
    def test_cleanup_batch_files(self):
        """Test batch file cleanup functionality."""
        # Create some test files
        test_files = []
        for i in range(3):
            file_path = os.path.join(self.temp_dir, f"test_batch_{i}.csv")
            pd.DataFrame({"test": [1, 2, 3]}).to_csv(file_path, index=False)
            test_files.append(file_path)
        
        # Verify files exist
        for file_path in test_files:
            self.assertTrue(os.path.exists(file_path))
        
        # Cleanup files
        cleaned_count = self.agent.cleanup_batch_files(test_files)
        
        self.assertEqual(cleaned_count, 3)
        
        # Verify files are deleted
        for file_path in test_files:
            self.assertFalse(os.path.exists(file_path))
    
    def test_batch_file_naming_convention(self):
        """Test batch file naming convention."""
        pivot_df = pd.DataFrame([
            {"gst_rate": 0.18, "total_taxable": 1000.0},
            {"gst_rate": 0.12, "total_taxable": 500.0},
            {"gst_rate": 0.0, "total_taxable": 300.0}
        ])
        
        batch_files, result = self.agent.process_pivot_data(
            pivot_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        # Check file names follow convention
        expected_patterns = [
            "amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_batch.csv",
            "amazon_mtr_06ABGCS4796R1ZA_2025-08_12pct_batch.csv",
            "amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_batch.csv"
        ]
        
        actual_filenames = [os.path.basename(f) for f in batch_files]
        
        for expected_pattern in expected_patterns:
            self.assertIn(expected_pattern, actual_filenames)


class TestBatchSplitterIntegration(unittest.TestCase):
    """Integration tests for Batch Splitter with real-like data."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value = MagicMock()
        
        self.agent = BatchSplitterAgent(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
        
        # Create temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_realistic_pivot_data_splitting(self):
        """Test splitting realistic pivot data with various GST rates."""
        # Create realistic pivot data similar to what would come from Amazon MTR
        pivot_df = pd.DataFrame([
            # 18% GST items
            {"gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18, "ledger": "Amazon Haryana", "fg": "FABCON-5L", "total_quantity": 5, "total_taxable": 5295.0, "total_cgst": 476.55, "total_sgst": 476.55, "total_igst": 0.0},
            {"gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18, "ledger": "Amazon Delhi", "fg": "FABCON-5L", "total_quantity": 3, "total_taxable": 3177.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 571.86},
            {"gstin": self.gstin, "month": "2025-08", "gst_rate": 0.18, "ledger": "Amazon Maharashtra", "fg": "KOPAROFABCON", "total_quantity": 8, "total_taxable": 1696.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 305.28},
            
            # 0% GST items
            {"gstin": self.gstin, "month": "2025-08", "gst_rate": 0.0, "ledger": "Amazon Delhi", "fg": "FABCON-5L", "total_quantity": 4, "total_taxable": 4236.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 0.0},
            {"gstin": self.gstin, "month": "2025-08", "gst_rate": 0.0, "ledger": "Amazon Delhi", "fg": "90-X8YV-Q3DM", "total_quantity": 1, "total_taxable": 449.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 0.0},
            
            # 12% GST items (hypothetical)
            {"gstin": self.gstin, "month": "2025-08", "gst_rate": 0.12, "ledger": "Amazon Karnataka", "fg": "ESSENTIAL-ITEM", "total_quantity": 10, "total_taxable": 2000.0, "total_cgst": 0.0, "total_sgst": 0.0, "total_igst": 240.0}
        ])
        
        batch_files, result = self.agent.process_pivot_data(
            pivot_df, "amazon_mtr", self.gstin, "2025-08", self.run_id, self.temp_dir
        )
        
        # Validate overall result
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 6)
        self.assertEqual(result.batch_files_created, 3)  # 0%, 12%, 18%
        self.assertEqual(result.gst_rates_processed, 3)
        self.assertTrue(result.validation_passed)
        
        # Validate individual batch files
        gst_rate_files = {}
        for file_path in batch_files:
            df = pd.read_csv(file_path)
            gst_rate = df['gst_rate'].iloc[0]
            gst_rate_files[gst_rate] = df
        
        # Check 18% GST batch
        gst_18_df = gst_rate_files[0.18]
        self.assertEqual(len(gst_18_df), 3)
        self.assertEqual(gst_18_df['total_quantity'].sum(), 16)  # 5 + 3 + 8
        self.assertEqual(gst_18_df['total_taxable'].sum(), 10168.0)  # 5295 + 3177 + 1696
        
        # Check 0% GST batch
        gst_0_df = gst_rate_files[0.0]
        self.assertEqual(len(gst_0_df), 2)
        self.assertEqual(gst_0_df['total_quantity'].sum(), 5)  # 4 + 1
        self.assertEqual(gst_0_df['total_taxable'].sum(), 4685.0)  # 4236 + 449
        self.assertEqual(gst_0_df['total_cgst'].sum(), 0.0)
        self.assertEqual(gst_0_df['total_sgst'].sum(), 0.0)
        self.assertEqual(gst_0_df['total_igst'].sum(), 0.0)
        
        # Check 12% GST batch
        gst_12_df = gst_rate_files[0.12]
        self.assertEqual(len(gst_12_df), 1)
        self.assertEqual(gst_12_df['total_quantity'].sum(), 10)
        self.assertEqual(gst_12_df['total_taxable'].sum(), 2000.0)
        self.assertEqual(gst_12_df['total_igst'].sum(), 240.0)
        
        # Validate batch summaries
        batch_summary = self.agent.get_batch_summary(result.batch_summaries)
        self.assertEqual(batch_summary["total_batches"], 3)
        self.assertEqual(batch_summary["total_records"], 6)
        self.assertEqual(batch_summary["total_taxable"], 16853.0)  # Sum of all taxable amounts


if __name__ == "__main__":
    unittest.main()
