"""
Test suite for Invoice Numbering Agent
Tests invoice number generation patterns and uniqueness
"""
import unittest
import pandas as pd
import uuid
from unittest.mock import MagicMock

from ingestion_layer.libs.numbering_rules import NumberingRulesEngine
from ingestion_layer.agents.invoice_numbering import InvoiceNumberingAgent
from ingestion_layer.libs.contracts import InvoiceNumberingResult


class TestNumberingRulesEngine(unittest.TestCase):
    """Test the core numbering rules engine."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.gstin = "06ABGCS4796R1ZA"  # Haryana GSTIN
        self.numbering_engine = NumberingRulesEngine(self.gstin)
    
    def test_gstin_state_extraction(self):
        """Test GSTIN state code extraction."""
        self.assertEqual(self.numbering_engine.company_state_code, "HR")
    
    def test_state_code_mapping(self):
        """Test state name to code mapping."""
        self.assertEqual(self.numbering_engine._get_state_code("ANDHRA PRADESH"), "AP")
        self.assertEqual(self.numbering_engine._get_state_code("KARNATAKA"), "KA")
        self.assertEqual(self.numbering_engine._get_state_code("DELHI"), "DL")
        self.assertEqual(self.numbering_engine._get_state_code("UNKNOWN STATE"), "UN")
    
    def test_month_code_extraction(self):
        """Test month code extraction."""
        self.assertEqual(self.numbering_engine._get_month_code("2025-08"), "08")
        self.assertEqual(self.numbering_engine._get_month_code("2025-12"), "12")
        self.assertEqual(self.numbering_engine._get_month_code("08"), "08")
    
    def test_amazon_mtr_pattern(self):
        """Test Amazon MTR invoice number pattern."""
        invoice_no = self.numbering_engine.generate_invoice_number(
            channel="amazon_mtr",
            state_name="ANDHRA PRADESH",
            month="2025-08",
            sequence_number=1
        )
        
        self.assertEqual(invoice_no, "AMZ-AP-08-0001")
    
    def test_amazon_str_pattern(self):
        """Test Amazon STR invoice number pattern."""
        invoice_no = self.numbering_engine.generate_invoice_number(
            channel="amazon_str",
            state_name="KARNATAKA",
            month="2025-08",
            sequence_number=5
        )
        
        self.assertEqual(invoice_no, "AMZST-KA-08-0005")
    
    def test_flipkart_pattern(self):
        """Test Flipkart invoice number pattern."""
        invoice_no = self.numbering_engine.generate_invoice_number(
            channel="flipkart",
            state_name="DELHI",
            month="2025-08",
            sequence_number=10
        )
        
        self.assertEqual(invoice_no, "FLIP-DL-08-0010")
    
    def test_pepperfry_pattern(self):
        """Test Pepperfry invoice number pattern."""
        invoice_no = self.numbering_engine.generate_invoice_number(
            channel="pepperfry",
            state_name="MAHARASHTRA",
            month="2025-08",
            sequence_number=25
        )
        
        self.assertEqual(invoice_no, "PEPP-MH-08-0025")
    
    def test_uniqueness_enforcement(self):
        """Test invoice number uniqueness enforcement."""
        # Generate first number
        invoice_no_1 = self.numbering_engine.generate_invoice_number(
            channel="amazon_mtr",
            state_name="DELHI",
            month="2025-08",
            sequence_number=1
        )
        
        # Try to generate same number again
        invoice_no_2 = self.numbering_engine.generate_invoice_number(
            channel="amazon_mtr",
            state_name="DELHI",
            month="2025-08",
            sequence_number=1
        )
        
        # Should be different due to uniqueness enforcement
        self.assertNotEqual(invoice_no_1, invoice_no_2)
        self.assertEqual(invoice_no_1, "AMZ-DL-08-0001")
        self.assertEqual(invoice_no_2, "AMZ-DL-08-0002")  # Auto-incremented
    
    def test_batch_generation(self):
        """Test batch invoice number generation."""
        records = [
            {"state_code": "ANDHRA PRADESH"},
            {"state_code": "ANDHRA PRADESH"},
            {"state_code": "KARNATAKA"},
            {"state_code": "KARNATAKA"},
            {"state_code": "DELHI"}
        ]
        
        invoice_numbers = self.numbering_engine.generate_batch_invoice_numbers(
            records=records,
            channel="amazon_mtr",
            month="2025-08"
        )
        
        # Check that we got numbers for all records
        self.assertEqual(len(invoice_numbers), 5)
        
        # Check sequential numbering within states
        ap_numbers = [invoice_numbers[i] for i in [0, 1]]
        ka_numbers = [invoice_numbers[i] for i in [2, 3]]
        
        self.assertEqual(ap_numbers[0], "AMZ-AP-08-0001")
        self.assertEqual(ap_numbers[1], "AMZ-AP-08-0002")
        self.assertEqual(ka_numbers[0], "AMZ-KA-08-0001")
        self.assertEqual(ka_numbers[1], "AMZ-KA-08-0002")
        self.assertEqual(invoice_numbers[4], "AMZ-DL-08-0001")
    
    def test_invoice_number_validation(self):
        """Test invoice number format validation."""
        # Valid numbers
        self.assertTrue(self.numbering_engine.validate_invoice_number("AMZ-AP-08-0001", "amazon_mtr"))
        self.assertTrue(self.numbering_engine.validate_invoice_number("FLIP-KA-12", "flipkart"))
        self.assertTrue(self.numbering_engine.validate_invoice_number("PEPP-MH-08-0005", "pepperfry"))
        
        # Invalid numbers
        self.assertFalse(self.numbering_engine.validate_invoice_number("INVALID-FORMAT", "amazon_mtr"))
        self.assertFalse(self.numbering_engine.validate_invoice_number("AMZ-AP-08-0001", "flipkart"))  # Wrong channel
        self.assertFalse(self.numbering_engine.validate_invoice_number("", "amazon_mtr"))
    
    def test_invoice_number_parsing(self):
        """Test invoice number parsing."""
        parsed = self.numbering_engine.parse_invoice_number("AMZ-AP-08-0001", "amazon_mtr")
        
        expected = {
            "prefix": "AMZ",
            "state_code": "AP",
            "month_code": "08",
            "sequence": "0001"
        }
        
        self.assertEqual(parsed, expected)
    
    def test_existing_numbers_registration(self):
        """Test registration of existing invoice numbers."""
        existing_numbers = ["AMZ-AP-08-0001", "AMZ-AP-08-0002", "AMZ-KA-08-0001"]
        self.numbering_engine.register_existing_numbers(existing_numbers)
        
        # Generate new number for AP - should skip existing ones
        new_number = self.numbering_engine.generate_invoice_number(
            channel="amazon_mtr",
            state_name="ANDHRA PRADESH",
            month="2025-08",
            sequence_number=1
        )
        
        # Should get next available number
        self.assertEqual(new_number, "AMZ-AP-08-0003")
    
    def test_get_next_sequence_number(self):
        """Test getting next sequence number."""
        # Register some existing numbers
        existing_numbers = ["AMZ-AP-08-0001", "AMZ-AP-08-0003", "AMZ-AP-08-0005"]
        self.numbering_engine.register_existing_numbers(existing_numbers)
        
        next_seq = self.numbering_engine.get_next_sequence_number(
            channel="amazon_mtr",
            state_name="ANDHRA PRADESH",
            month="2025-08"
        )
        
        # Should be 6 (next after 5)
        self.assertEqual(next_seq, 6)
    
    def test_unknown_channel_handling(self):
        """Test handling of unknown channels."""
        with self.assertRaises(ValueError):
            self.numbering_engine.generate_invoice_number(
                channel="unknown_channel",
                state_name="DELHI",
                month="2025-08"
            )
    
    def test_supported_channels(self):
        """Test getting supported channels."""
        channels = self.numbering_engine.get_supported_channels()
        expected_channels = ["amazon_mtr", "amazon_str", "flipkart", "pepperfry"]
        
        self.assertEqual(set(channels), set(expected_channels))
    
    def test_pattern_examples(self):
        """Test getting pattern examples."""
        self.assertEqual(self.numbering_engine.get_pattern_example("amazon_mtr"), "AMZ-TN-04")
        self.assertEqual(self.numbering_engine.get_pattern_example("amazon_str"), "AMZST-06-04")
        self.assertEqual(self.numbering_engine.get_pattern_example("flipkart"), "FLIP-TN-04")
        self.assertEqual(self.numbering_engine.get_pattern_example("pepperfry"), "PEPP-TN-04")


class TestInvoiceNumberingAgent(unittest.TestCase):
    """Test the Invoice Numbering Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value.data = []
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value = MagicMock()
        
        self.agent = InvoiceNumberingAgent(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
    
    def test_process_dataset(self):
        """Test processing dataset for invoice numbering."""
        # Create sample dataset with tax computations
        df = pd.DataFrame([
            {
                "sku": "LLQ-LAV-3L-FBA",
                "state_code": "ANDHRA PRADESH",
                "taxable_value": 449.0,
                "gst_rate": 0.18,
                "cgst": 0.0,
                "sgst": 0.0,
                "igst": 80.82,
                "total_tax": 80.82
            },
            {
                "sku": "FABCON-5L-FBA",
                "state_code": "KARNATAKA",
                "taxable_value": 1059.0,
                "gst_rate": 0.18,
                "cgst": 0.0,
                "sgst": 0.0,
                "igst": 190.62,
                "total_tax": 190.62
            },
            {
                "sku": "DW-5L",
                "state_code": "ANDHRA PRADESH",
                "taxable_value": 999.0,
                "gst_rate": 0.18,
                "cgst": 0.0,
                "sgst": 0.0,
                "igst": 179.82,
                "total_tax": 179.82
            }
        ])
        
        enriched_df, result = self.agent.process_dataset(
            df=df,
            channel="amazon",
            gstin=self.gstin,
            month="2025-08",
            run_id=self.run_id
        )
        
        # Check result
        self.assertTrue(result.success)
        self.assertEqual(result.processed_records, 3)
        self.assertEqual(result.successful_generations, 3)
        self.assertEqual(result.failed_generations, 0)
        self.assertEqual(result.unique_invoice_numbers, 3)
        
        # Check enriched data
        self.assertIn("invoice_no", enriched_df.columns)
        
        # Check invoice number patterns
        ap_invoices = enriched_df[enriched_df["state_code"] == "ANDHRA PRADESH"]["invoice_no"].tolist()
        ka_invoices = enriched_df[enriched_df["state_code"] == "KARNATAKA"]["invoice_no"].tolist()
        
        # AP should have sequential numbers
        self.assertEqual(len(ap_invoices), 2)
        self.assertTrue(all(inv.startswith("AMZ-AP-08-") for inv in ap_invoices))
        
        # KA should have one number
        self.assertEqual(len(ka_invoices), 1)
        self.assertTrue(ka_invoices[0].startswith("AMZ-KA-08-"))
    
    def test_channel_name_normalization(self):
        """Test channel name normalization."""
        test_cases = [
            ("amazon", "amazon_mtr"),
            ("amazon_mtr", "amazon_mtr"),
            ("amazon_str", "amazon_str"),
            ("flipkart", "flipkart"),
            ("pepperfry", "pepperfry"),
            ("unknown", "amazon_mtr")  # Default fallback
        ]
        
        for input_channel, expected in test_cases:
            normalized = self.agent._normalize_channel_name(input_channel)
            self.assertEqual(normalized, expected)
    
    def test_get_numbering_summary(self):
        """Test numbering summary generation."""
        df = pd.DataFrame([
            {"invoice_no": "AMZ-AP-08-0001", "state_code": "ANDHRA PRADESH"},
            {"invoice_no": "AMZ-AP-08-0002", "state_code": "ANDHRA PRADESH"},
            {"invoice_no": "AMZ-KA-08-0001", "state_code": "KARNATAKA"},
            {"invoice_no": "", "state_code": "DELHI"}  # Missing invoice
        ])
        
        summary = self.agent.get_numbering_summary(df, "amazon_mtr")
        
        expected = {
            "total_records": 4,
            "generated_invoices": 3,  # Excluding empty invoice
            "unique_invoices": 3,
            "duplicate_invoices": 0,
            "pattern_example": "AMZ-TN-04",
            "states_covered": 3,
            "sample_invoices": ["AMZ-AP-08-0001", "AMZ-AP-08-0002", "AMZ-KA-08-0001"]
        }
        
        self.assertEqual(summary["total_records"], expected["total_records"])
        self.assertEqual(summary["generated_invoices"], expected["generated_invoices"])
        self.assertEqual(summary["unique_invoices"], expected["unique_invoices"])
        self.assertEqual(summary["duplicate_invoices"], expected["duplicate_invoices"])
        self.assertEqual(summary["states_covered"], expected["states_covered"])
    
    def test_validate_invoice_numbers(self):
        """Test invoice number validation."""
        df = pd.DataFrame([
            {"invoice_no": "AMZ-AP-08-0001"},  # Valid
            {"invoice_no": "AMZ-KA-08-0002"},  # Valid
            {"invoice_no": "INVALID-FORMAT"},  # Invalid
            {"invoice_no": ""},               # Missing
            {"invoice_no": "AMZ-AP-08-0001"}  # Duplicate
        ])
        
        validation = self.agent.validate_invoice_numbers(df, "amazon_mtr")
        
        expected = {
            "total_records": 5,
            "valid_numbers": 2,
            "invalid_numbers": 1,
            "missing_numbers": 1,
            "duplicate_numbers": 1
        }
        
        self.assertEqual(validation, expected)
    
    def test_regenerate_invoice_numbers(self):
        """Test invoice number regeneration."""
        df = pd.DataFrame([
            {"sku": "TEST-SKU", "state_code": "DELHI", "invoice_no": "OLD-NUMBER"}
        ])
        
        # Test regeneration with force=True
        enriched_df, result = self.agent.regenerate_invoice_numbers(
            df=df,
            channel="amazon_mtr",
            gstin=self.gstin,
            month="2025-08",
            run_id=self.run_id,
            force=True
        )
        
        self.assertTrue(result.success)
        self.assertNotEqual(enriched_df.iloc[0]["invoice_no"], "OLD-NUMBER")
        self.assertTrue(enriched_df.iloc[0]["invoice_no"].startswith("AMZ-DL-08-"))
    
    def test_check_invoice_uniqueness(self):
        """Test invoice uniqueness checking."""
        # Mock existing numbers in database
        self.mock_supabase.client.table.return_value.select.return_value.in_.return_value.execute.return_value.data = [
            {"invoice_no": "AMZ-AP-08-0001"},
            {"invoice_no": "AMZ-KA-08-0001"}
        ]
        
        test_numbers = ["AMZ-AP-08-0001", "AMZ-AP-08-0002", "AMZ-KA-08-0001", "AMZ-DL-08-0001"]
        
        result = self.agent.check_invoice_uniqueness(test_numbers)
        
        expected = {
            "unique_numbers": ["AMZ-AP-08-0002", "AMZ-DL-08-0001"],
            "duplicate_numbers": ["AMZ-AP-08-0001", "AMZ-KA-08-0001"],
            "total_checked": 4,
            "unique_count": 2,
            "duplicate_count": 2
        }
        
        self.assertEqual(set(result["unique_numbers"]), set(expected["unique_numbers"]))
        self.assertEqual(set(result["duplicate_numbers"]), set(expected["duplicate_numbers"]))
        self.assertEqual(result["total_checked"], expected["total_checked"])
        self.assertEqual(result["unique_count"], expected["unique_count"])
        self.assertEqual(result["duplicate_count"], expected["duplicate_count"])


class TestInvoiceNumberingGoldenTests(unittest.TestCase):
    """Test Invoice Numbering against golden reference data."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase = MagicMock()
        self.mock_supabase.client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value.data = []
        self.mock_supabase.client.table.return_value.insert.return_value.execute.return_value = MagicMock()
        
        self.agent = InvoiceNumberingAgent(self.mock_supabase)
        self.gstin = "06ABGCS4796R1ZA"
        self.run_id = uuid.uuid4()
    
    def test_amazon_mtr_golden_reference(self):
        """Test Amazon MTR invoice numbering against golden reference."""
        # Load golden reference
        golden_df = pd.read_csv("ingestion_layer/tests/golden/amazon_mtr_expected.csv")
        
        # Create input dataset (without invoice_no column)
        input_columns = [col for col in golden_df.columns if col != "invoice_no"]
        input_df = golden_df[input_columns].copy()
        
        # Process with invoice numbering agent
        enriched_df, result = self.agent.process_dataset(
            df=input_df,
            channel="amazon_mtr",
            gstin=self.gstin,
            month="2025-08",
            run_id=self.run_id
        )
        
        # Check that all invoice numbers follow the correct pattern
        for index, row in enriched_df.iterrows():
            invoice_no = row["invoice_no"]
            state_code = row["state_code"]
            
            # Check pattern
            self.assertTrue(invoice_no.startswith("AMZ-"))
            self.assertIn("-08-", invoice_no)  # Month code
            
            # Check state code mapping
            expected_state_abbr = {
                "ANDHRA PRADESH": "AP",
                "KARNATAKA": "KA",
                "DELHI": "DL",
                "JAMMU & KASHMIR": "JK",
                "TELANGANA": "TG",
                "MAHARASHTRA": "MH",
                "PUNJAB": "PB",
                "HARYANA": "HR"
            }.get(state_code, "UN")
            
            self.assertIn(f"-{expected_state_abbr}-", invoice_no)
        
        # Check uniqueness
        invoice_numbers = enriched_df["invoice_no"].tolist()
        self.assertEqual(len(invoice_numbers), len(set(invoice_numbers)), "Duplicate invoice numbers found")
        
        # Check sequential numbering within states
        state_groups = enriched_df.groupby("state_code")
        for state, group in state_groups:
            invoices = group["invoice_no"].tolist()
            # Extract sequence numbers
            sequences = []
            for inv in invoices:
                try:
                    seq = int(inv.split("-")[-1])
                    sequences.append(seq)
                except (ValueError, IndexError):
                    continue
            
            # Should be sequential starting from 1
            sequences.sort()
            expected_sequences = list(range(1, len(sequences) + 1))
            self.assertEqual(sequences, expected_sequences, f"Non-sequential numbering for state {state}")


if __name__ == "__main__":
    unittest.main()
