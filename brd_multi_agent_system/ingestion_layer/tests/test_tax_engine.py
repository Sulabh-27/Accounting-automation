"""
Test suite for Tax Engine Agent
Tests GST computation logic against golden reference data
"""
import unittest
import pandas as pd
import uuid
from decimal import Decimal
from unittest.mock import MagicMock

from ingestion_layer.libs.tax_rules import TaxRulesEngine
from ingestion_layer.agents.tax_engine import TaxEngine
from ingestion_layer.libs.contracts import TaxComputationResult


class TestTaxRulesEngine(unittest.TestCase):
    """Test the core tax rules engine."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.gstin = "06ABGCS4796R1ZA"  # Haryana GSTIN
        self.tax_engine = TaxRulesEngine(self.gstin)
    
    def test_gstin_state_extraction(self):
        """Test GSTIN state code extraction."""
        self.assertEqual(self.tax_engine.company_state_code, "06")
    
    def test_intrastate_detection(self):
        """Test intrastate transaction detection."""
        # Haryana (same state as company)
        self.assertTrue(self.tax_engine._is_intrastate("HARYANA"))
        
        # Other states (interstate)
        self.assertFalse(self.tax_engine._is_intrastate("KARNATAKA"))
        self.assertFalse(self.tax_engine._is_intrastate("DELHI"))
    
    def test_amazon_mtr_intrastate_tax(self):
        """Test Amazon MTR intrastate tax computation."""
        result = self.tax_engine.compute_amazon_mtr_tax(
            taxable_value=449.0,
            gst_rate=0.18,
            customer_state="HARYANA"  # Same state as company
        )
        
        expected = {
            "taxable_value": 449.0,
            "shipping_value": 0.0,
            "cgst": 40.41,  # 449 * 0.09
            "sgst": 40.41,  # 449 * 0.09
            "igst": 0.0,
            "gst_rate": 0.18,
            "total_tax": 80.82,
            "total_amount": 529.82
        }
        
        self.assertEqual(result, expected)
    
    def test_amazon_mtr_interstate_tax(self):
        """Test Amazon MTR interstate tax computation."""
        result = self.tax_engine.compute_amazon_mtr_tax(
            taxable_value=449.0,
            gst_rate=0.18,
            customer_state="ANDHRA PRADESH"  # Different state
        )
        
        expected = {
            "taxable_value": 449.0,
            "shipping_value": 0.0,
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 80.82,  # 449 * 0.18
            "gst_rate": 0.18,
            "total_tax": 80.82,
            "total_amount": 529.82
        }
        
        self.assertEqual(result, expected)
    
    def test_zero_gst_rate(self):
        """Test zero GST rate computation."""
        result = self.tax_engine.compute_amazon_mtr_tax(
            taxable_value=449.0,
            gst_rate=0.0,
            customer_state="DELHI"
        )
        
        expected = {
            "taxable_value": 449.0,
            "shipping_value": 0.0,
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 0.0,
            "gst_rate": 0.0,
            "total_tax": 0.0,
            "total_amount": 449.0
        }
        
        self.assertEqual(result, expected)
    
    def test_amazon_str_always_igst(self):
        """Test Amazon STR always uses IGST."""
        # Even for same state, STR should use IGST
        result = self.tax_engine.compute_amazon_str_tax(
            taxable_value=1000.0,
            gst_rate=0.18,
            customer_state="HARYANA"  # Same state as company
        )
        
        expected = {
            "taxable_value": 1000.0,
            "shipping_value": 0.0,
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 180.0,  # 1000 * 0.18
            "gst_rate": 0.18,
            "total_tax": 180.0,
            "total_amount": 1180.0
        }
        
        self.assertEqual(result, expected)
    
    def test_flipkart_tax_computation(self):
        """Test Flipkart tax computation."""
        # Interstate transaction
        result = self.tax_engine.compute_flipkart_tax(
            taxable_value=500.0,
            gst_rate=0.18,
            customer_state="KARNATAKA",
            seller_state="HARYANA"
        )
        
        expected = {
            "taxable_value": 500.0,
            "shipping_value": 0.0,
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 90.0,  # 500 * 0.18
            "gst_rate": 0.18,
            "total_tax": 90.0,
            "total_amount": 590.0
        }
        
        self.assertEqual(result, expected)
    
    def test_pepperfry_with_returns(self):
        """Test Pepperfry tax computation with returns."""
        result = self.tax_engine.compute_pepperfry_tax(
            taxable_value=1000.0,
            gst_rate=0.18,
            customer_state="KARNATAKA",  # Different from company state
            returned_qty=1,
            total_qty=2
        )
        
        # Adjusted taxable value: 1000 * (2-1)/2 = 500
        expected = {
            "taxable_value": 500.0,  # Adjusted for returns
            "shipping_value": 0.0,
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 90.0,  # 500 * 0.18
            "gst_rate": 0.18,
            "total_tax": 90.0,
            "total_amount": 590.0,
            "returned_qty": 1,
            "net_qty": 1
        }
        
        self.assertEqual(result, expected)
    
    def test_shipping_value_inclusion(self):
        """Test shipping value inclusion in tax computation."""
        result = self.tax_engine.compute_amazon_mtr_tax(
            taxable_value=1000.0,
            gst_rate=0.18,
            customer_state="DELHI",
            shipping_value=100.0
        )
        
        # Total taxable: 1000 + 100 = 1100
        # IGST: 1100 * 0.18 = 198
        expected = {
            "taxable_value": 1000.0,
            "shipping_value": 100.0,
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 198.0,
            "gst_rate": 0.18,
            "total_tax": 198.0,
            "total_amount": 1298.0
        }
        
        self.assertEqual(result, expected)
    
    def test_currency_rounding(self):
        """Test proper currency rounding."""
        result = self.tax_engine.compute_amazon_mtr_tax(
            taxable_value=333.33,  # Will create fractional tax
            gst_rate=0.18,
            customer_state="DELHI"
        )
        
        # 333.33 * 0.18 = 59.9994, should round to 60.00
        self.assertEqual(result["igst"], 60.0)
        self.assertEqual(result["total_tax"], 60.0)
        self.assertEqual(result["total_amount"], 393.33)
    
    def test_invalid_gst_rate(self):
        """Test handling of invalid GST rates."""
        with self.assertRaises(ValueError):
            self.tax_engine.compute_amazon_mtr_tax(
                taxable_value=1000.0,
                gst_rate=0.25,  # Invalid rate
                customer_state="DELHI"
            )
    
    def test_tax_computation_validation(self):
        """Test tax computation validation."""
        # Valid intrastate computation
        valid_computation = {
            "cgst": 45.0,
            "sgst": 45.0,
            "igst": 0.0,
            "total_tax": 90.0
        }
        self.assertTrue(self.tax_engine.validate_tax_computation(valid_computation))
        
        # Valid interstate computation
        valid_interstate = {
            "cgst": 0.0,
            "sgst": 0.0,
            "igst": 90.0,
            "total_tax": 90.0
        }
        self.assertTrue(self.tax_engine.validate_tax_computation(valid_interstate))
        
        # Invalid computation (both CGST/SGST and IGST)
        invalid_computation = {
            "cgst": 45.0,
            "sgst": 45.0,
            "igst": 90.0,
            "total_tax": 180.0
        }
        self.assertFalse(self.tax_engine.validate_tax_computation(invalid_computation))


class TestTaxEngineAgent(unittest.TestCase):
    """Test the Tax Engine Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.tax_engine = TaxEngine(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
    
    def test_process_dataset_amazon_mtr(self):
        """Test processing Amazon MTR dataset."""
        # Create sample dataset
        df = pd.DataFrame([
            {
                "sku": "LLQ-LAV-3L-FBA",
                "asin": "B0CZXQMSR5",
                "taxable_value": 449.0,
                "gst_rate": 0.18,
                "state_code": "ANDHRA PRADESH",
                "quantity": 1
            },
            {
                "sku": "FABCON-5L",
                "asin": "B09MZ2LBXB",
                "taxable_value": 4236.0,
                "gst_rate": 0.0,
                "state_code": "DELHI",
                "quantity": 4
            }
        ])
        
        enriched_df, result = self.tax_engine.process_dataset(
            df=df,
            channel="amazon_mtr",
            gstin=self.gstin,
            run_id=self.run_id
        )
        
        # Check result
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 2)
        self.assertEqual(result.successful_computations, 2)
        self.assertEqual(result.failed_computations, 0)
        
        # Check enriched data
        self.assertIn("cgst", enriched_df.columns)
        self.assertIn("sgst", enriched_df.columns)
        self.assertIn("igst", enriched_df.columns)
        self.assertIn("total_tax", enriched_df.columns)
        
        # Check first record (interstate, 18% GST)
        self.assertEqual(enriched_df.iloc[0]["igst"], 80.82)
        self.assertEqual(enriched_df.iloc[0]["cgst"], 0.0)
        self.assertEqual(enriched_df.iloc[0]["sgst"], 0.0)
        
        # Check second record (interstate, 0% GST)
        self.assertEqual(enriched_df.iloc[1]["igst"], 0.0)
        self.assertEqual(enriched_df.iloc[1]["total_tax"], 0.0)
    
    def test_get_tax_summary(self):
        """Test tax summary generation."""
        df = pd.DataFrame([
            {"taxable_value": 449.0, "cgst": 0.0, "sgst": 0.0, "igst": 80.82, "total_tax": 80.82, "total_amount": 529.82, "gst_rate": 0.18},
            {"taxable_value": 1059.0, "cgst": 0.0, "sgst": 0.0, "igst": 190.62, "total_tax": 190.62, "total_amount": 1249.62, "gst_rate": 0.18},
            {"taxable_value": 4236.0, "cgst": 0.0, "sgst": 0.0, "igst": 0.0, "total_tax": 0.0, "total_amount": 4236.0, "gst_rate": 0.0}
        ])
        
        summary = self.tax_engine.get_tax_summary(df)
        
        expected = {
            "total_records": 3,
            "total_taxable_amount": 5744.0,
            "total_cgst": 0.0,
            "total_sgst": 0.0,
            "total_igst": 271.44,
            "total_tax": 271.44,
            "total_amount": 6015.44,
            "avg_gst_rate": 0.12,  # (0.18 + 0.18 + 0.0) / 3
            "intrastate_records": 0,
            "interstate_records": 2  # Only records with igst > 0
        }
        
        self.assertEqual(summary, expected)
    
    def test_validate_tax_computations(self):
        """Test tax computation validation."""
        # Valid dataset
        df = pd.DataFrame([
            {"cgst": 40.41, "sgst": 40.41, "igst": 0.0, "gst_rate": 0.18, "taxable_value": 449.0, "total_tax": 80.82},
            {"cgst": 0.0, "sgst": 0.0, "igst": 80.82, "gst_rate": 0.18, "taxable_value": 449.0, "total_tax": 80.82}
        ])
        
        validation = self.tax_engine.validate_tax_computations(df)
        
        self.assertEqual(validation["total_records"], 2)
        self.assertEqual(validation["valid_computations"], 2)
        self.assertEqual(validation["invalid_computations"], 0)
        self.assertEqual(validation["missing_tax_data"], 0)


class TestTaxEngineGoldenTests(unittest.TestCase):
    """Test Tax Engine against golden reference data."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.tax_engine = TaxEngine(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
    
    def test_amazon_mtr_golden_reference(self):
        """Test Amazon MTR processing against golden reference."""
        # Load golden reference
        golden_df = pd.read_csv("ingestion_layer/tests/golden/amazon_mtr_expected.csv")
        
        # Create input dataset (without tax columns)
        input_columns = ["invoice_date", "type", "order_id", "sku", "asin", "quantity", 
                        "taxable_value", "gst_rate", "state_code", "channel", "gstin", 
                        "month", "fg", "item_resolved", "ledger_name", "ledger_resolved"]
        input_df = golden_df[input_columns].copy()
        
        # Process with tax engine
        enriched_df, result = self.tax_engine.process_dataset(
            df=input_df,
            channel="amazon_mtr",
            gstin=self.gstin,
            run_id=self.run_id
        )
        
        # Compare key tax fields with golden reference
        tolerance = 0.01  # Allow small rounding differences
        
        for index, row in golden_df.iterrows():
            if index >= len(enriched_df):
                break
                
            actual_row = enriched_df.iloc[index]
            
            # Compare tax amounts
            self.assertAlmostEqual(
                actual_row["cgst"], row["cgst"], 
                delta=tolerance, 
                msg=f"CGST mismatch at row {index}"
            )
            self.assertAlmostEqual(
                actual_row["sgst"], row["sgst"], 
                delta=tolerance, 
                msg=f"SGST mismatch at row {index}"
            )
            self.assertAlmostEqual(
                actual_row["igst"], row["igst"], 
                delta=tolerance, 
                msg=f"IGST mismatch at row {index}"
            )
            self.assertAlmostEqual(
                actual_row["total_tax"], row["total_tax"], 
                delta=tolerance, 
                msg=f"Total tax mismatch at row {index}"
            )
            self.assertAlmostEqual(
                actual_row["total_amount"], row["total_amount"], 
                delta=tolerance, 
                msg=f"Total amount mismatch at row {index}"
            )
    
    def test_tax_computation_edge_cases(self):
        """Test edge cases in tax computation."""
        test_cases = [
            # Zero taxable value
            {"taxable_value": 0.0, "gst_rate": 0.18, "state_code": "DELHI", "expected_tax": 0.0},
            # Very small amount
            {"taxable_value": 0.01, "gst_rate": 0.18, "state_code": "DELHI", "expected_tax": 0.0},
            # Large amount
            {"taxable_value": 999999.99, "gst_rate": 0.18, "state_code": "DELHI", "expected_tax": 180000.0},
            # Different GST rates
            {"taxable_value": 1000.0, "gst_rate": 0.05, "state_code": "DELHI", "expected_tax": 50.0},
            {"taxable_value": 1000.0, "gst_rate": 0.12, "state_code": "DELHI", "expected_tax": 120.0},
            {"taxable_value": 1000.0, "gst_rate": 0.28, "state_code": "DELHI", "expected_tax": 280.0},
        ]
        
        for case in test_cases:
            df = pd.DataFrame([{
                "sku": "TEST-SKU",
                "asin": "TEST-ASIN",
                "taxable_value": case["taxable_value"],
                "gst_rate": case["gst_rate"],
                "state_code": case["state_code"],
                "quantity": 1
            }])
            
            enriched_df, result = self.tax_engine.process_dataset(
                df=df,
                channel="amazon_mtr",
                gstin=self.gstin,
                run_id=self.run_id
            )
            
            self.assertTrue(result.success, f"Failed for case: {case}")
            self.assertAlmostEqual(
                enriched_df.iloc[0]["total_tax"], 
                case["expected_tax"], 
                delta=0.01,
                msg=f"Tax mismatch for case: {case}"
            )


if __name__ == "__main__":
    unittest.main()
