from __future__ import annotations
import os
import uuid
import pandas as pd
from ..libs.contracts import IngestionRequest
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.utils import ensure_dir, safe_colname
from ..libs.csv_utils import safe_read_excel_or_csv


class AmazonMTRAgent:
    """Amazon MTR ingestion agent.

    - Keeps only Shipment & Refund rows
    - Normalizes columns to a standard schema
    - Uploads normalized CSV to Supabase storage
    """

    REQUIRED_COLS = [
        "invoice_date",
        "type",
        "order_id",
        "sku",
        "asin",
        "quantity",
        "taxable_value",
        "gst_rate",
        "state_code",
        "channel",
        "gstin",
        "month",
    ]

    def process(self, request: IngestionRequest, supabase: SupabaseClientWrapper) -> str:
        # Read Excel or CSV file
        df = safe_read_excel_or_csv(request.file_path)
        # normalize columns: lower + underscore
        df.columns = [safe_colname(c) for c in df.columns]

        # Filter to Shipment & Refund if a relevant column exists
        type_col = None
        for c in ["type", "transaction_type", "line_item_type"]:
            if c in df.columns:
                type_col = c
                break
        if type_col is not None:
            df = df[df[type_col].str.lower().isin(["shipment", "refund"])].copy()
            df.rename(columns={type_col: "type"}, inplace=True)
        else:
            df["type"] = "shipment"

        # Map actual Amazon MTR columns to standard schema
        col_map = {
            "order_id": ["order_id", "order id"],
            "sku": ["sku"],
            "asin": ["asin"],
            "quantity": ["quantity"],
            "taxable_value": ["principal_amount", "tax_exclusive_gross", "invoice_amount"],
            "gst_rate": ["igst_rate", "cgst_rate", "sgst_rate"],  # Will sum these for total GST rate
            "state_code": ["ship_to_state", "bill_from_state"],
            "invoice_date": ["invoice_date", "final_invoice_date"],
        }

        norm = {}
        for target, candidates in col_map.items():
            for c in candidates:
                if c in df.columns:
                    norm[target] = df[c]
                    break
            else:
                # default values
                if target == "gst_rate":
                    norm[target] = 0
                elif target == "quantity":
                    norm[target] = 0
                else:
                    norm[target] = ""

        norm_df = pd.DataFrame(norm)
        norm_df["channel"] = request.channel
        norm_df["gstin"] = request.gstin
        norm_df["month"] = request.month

        # basic cleanup
        for col in ["taxable_value", "gst_rate", "quantity"]:
            if col in norm_df.columns:
                norm_df[col] = pd.to_numeric(norm_df[col], errors="coerce").fillna(0)

        # ensure required columns exist
        for col in self.REQUIRED_COLS:
            if col not in norm_df.columns:
                norm_df[col] = "" if col not in ("taxable_value", "gst_rate", "quantity") else 0

        # write temp and upload
        out_dir = os.path.join(os.path.dirname(request.file_path), "normalized")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, f"amazon_mtr_{uuid.uuid4().hex}.csv")
        norm_df[self.REQUIRED_COLS].to_csv(out_path, index=False)

        storage_path = supabase.upload_file(out_path)
        supabase.insert_report_metadata(request.run_id, "amazon_mtr_normalized", storage_path)
        return storage_path
