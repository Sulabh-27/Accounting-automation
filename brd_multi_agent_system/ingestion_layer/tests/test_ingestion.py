import os
import tempfile
import uuid
import unittest
import pandas as pd

from ingestion_layer.libs.contracts import IngestionRequest
from ingestion_layer.libs.supabase_client import SupabaseClientWrapper
from ingestion_layer.agents.amazon_mtr_agent import AmazonMTRAgent
from ingestion_layer.agents.amazon_str_agent import AmazonSTRAgent
from ingestion_layer.agents.flipkart_agent import FlipkartAgent
from ingestion_layer.agents.pepperfry_agent import PepperfryAgent
from ingestion_layer.agents.schema_validator_agent import SchemaValidatorAgent


class FakeSupabase(SupabaseClientWrapper):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.bucket = "raw-reports"
        self.uploaded = []
        self.reports = []
        self.runs = {}
        self.client = None

    def upload_file(self, local_path: str, dest_path: str | None = None) -> str:
        # Simulate upload by copying to temp area
        name = os.path.basename(local_path)
        target_dir = os.path.join(self.base_dir, "uploads")
        os.makedirs(target_dir, exist_ok=True)
        target = os.path.join(target_dir, name)
        with open(local_path, "rb") as src, open(target, "wb") as dst:
            dst.write(src.read())
        spath = f"{self.bucket}/{name}"
        self.uploaded.append(spath)
        return spath

    def insert_report_metadata(self, run_id, report_type, file_path):
        row = {"id": str(uuid.uuid4()), "run_id": str(run_id), "report_type": report_type, "file_path": file_path}
        self.reports.append(row)
        return row

    def insert_run_start(self, run_id, channel, gstin, month):
        self.runs[str(run_id)] = {"status": "running", "channel": channel, "gstin": gstin, "month": month}
        return self.runs[str(run_id)]

    def update_run_finish(self, run_id, status="success"):
        self.runs[str(run_id)]["status"] = status
        return self.runs[str(run_id)]


class TestIngestionAgents(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = self.tmp.name
        self.supa = FakeSupabase(self.base)
        self.run_id = uuid.uuid4()

    def tearDown(self):
        self.tmp.cleanup()

    def write_csv(self, name: str, df: pd.DataFrame) -> str:
        path = os.path.join(self.base, name)
        df.to_csv(path, index=False)
        return path

    def test_amazon_mtr(self):
        df = pd.DataFrame({
            "Date": ["2025-08-01", "2025-08-02"],
            "Transaction Type": ["Shipment", "Refund"],
            "Amazon Order Id": ["A1", "A2"],
            "SKU": ["S1", "S2"],
            "ASIN": ["B001", "B002"],
            "Qty": [1, 2],
            "Item Price": [100, 50],
            "Tax Rate": [18, 18],
            "Ship To State Code": ["27", "27"],
        })
        path = self.write_csv("mtr.csv", df)
        req = IngestionRequest(run_id=self.run_id, channel="amazon", gstin="22AAAAA0000A1Z5", month="2025-08", report_type="amazon_mtr", file_path=path)
        out_path = AmazonMTRAgent().process(req, self.supa)
        self.assertTrue(out_path.startswith("raw-reports/"))
        self.assertGreaterEqual(len(self.supa.reports), 1)

    def test_amazon_str(self):
        df = pd.DataFrame({
            "Posting Date": ["2025-08-03"],
            "Amazon Order Id": ["A3"],
            "ASIN": ["B003"],
            "Qty": [3],
            "Net Amount": [300],
            "Tax Rate": [18],
            "Ship To State Code": ["29"],
            "Seller State Code": ["27"],
        })
        path = self.write_csv("str.csv", df)
        req = IngestionRequest(run_id=self.run_id, channel="amazon", gstin="22AAAAA0000A1Z5", month="2025-08", report_type="amazon_str", file_path=path)
        asin_map = {"B003": "S3"}
        out_path = AmazonSTRAgent().process(req, self.supa, asin_to_sku=asin_map)
        self.assertTrue(out_path.startswith("raw-reports/"))
        self.assertGreaterEqual(len(self.supa.reports), 1)

    def test_flipkart(self):
        df = pd.DataFrame({
            "Invoice Date": ["2025-08-04"],
            "Order Id": ["F1"],
            "SKU": ["FSN1"],
            "Qty": [2],
            "Net Amount": [200],
            "Tax Rate": [18],
            "Ship To State Code": ["27"],
        })
        path = self.write_csv("flipkart.csv", df)
        req = IngestionRequest(run_id=self.run_id, channel="flipkart", gstin="22AAAAA0000A1Z5", month="2025-08", report_type="flipkart", file_path=path)
        out_path = FlipkartAgent().process(req, self.supa)
        self.assertTrue(out_path.startswith("raw-reports/"))
        self.assertGreaterEqual(len(self.supa.reports), 1)

    def test_pepperfry(self):
        sales = pd.DataFrame({
            "Invoice Date": ["2025-08-05"],
            "Order Id": ["P1"],
            "Item SKU": ["PS1"],
            "Qty": [1],
            "Net Amount": [150],
            "Tax Rate": [18],
            "State Code": ["27"],
        })
        returns = pd.DataFrame({
            "Invoice Date": ["2025-08-10"],
            "Order Id": ["P1"],
            "Item SKU": ["PS1"],
            "Qty": [1],
            "Net Amount": [150],
            "Tax Rate": [18],
            "State Code": ["27"],
        })
        sales_path = self.write_csv("pep_sales.csv", sales)
        ret_path = self.write_csv("pep_returns.csv", returns)
        req = IngestionRequest(run_id=self.run_id, channel="pepperfry", gstin="22AAAAA0000A1Z5", month="2025-08", report_type="pepperfry", file_path=sales_path)
        out_path = PepperfryAgent().process(sales_path, ret_path, req, self.supa)
        self.assertTrue(out_path.startswith("raw-reports/"))
        self.assertGreaterEqual(len(self.supa.reports), 1)

    def test_schema_validator(self):
        df = pd.DataFrame({"invoice_date": ["2025-08-01"], "gst_rate": [18], "state_code": ["27"]})
        res = SchemaValidatorAgent().validate(df, ["invoice_date", "gst_rate", "state_code"]) 
        self.assertTrue(res.success)


if __name__ == "__main__":
    unittest.main()
