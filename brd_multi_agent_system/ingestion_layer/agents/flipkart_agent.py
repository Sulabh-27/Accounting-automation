from __future__ import annotations
import os
import uuid
import pandas as pd

from ..libs.contracts import IngestionRequest
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.utils import ensure_dir, safe_colname


class FlipkartAgent:
    """Flipkart sales ingestion agent.

    - Adds Month and Final Date
    - Performs basic cleanup
    - Uploads normalized CSV
    """

    REQUIRED_COLS = [
        "invoice_date",
        "order_id",
        "sku",
        "quantity",
        "taxable_value",
        "gst_rate",
        "state_code",
        "channel",
        "gstin",
        "month",
        "final_date",
    ]

    def process(self, request: IngestionRequest, supabase: SupabaseClientWrapper) -> str:
        # Read Excel or CSV file
        if request.file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(request.file_path)
        else:
            df = pd.read_csv(request.file_path)
        df.columns = [safe_colname(c) for c in df.columns]

        col_map = {
            "invoice_date": ["invoice_date", "order_date", "date"],
            "order_id": ["order_id", "order"],
            "sku": ["sku", "fsn"],
            "quantity": ["quantity", "qty"],
            "taxable_value": ["taxable_value", "net_amount", "item_price"],
            "gst_rate": ["gst_rate", "tax_rate"],
            "state_code": ["ship_to_state_code", "state_code", "state"],
        }

        norm = {}
        for target, candidates in col_map.items():
            for c in candidates:
                if c in df.columns:
                    norm[target] = df[c]
                    break
            else:
                norm[target] = 0 if target in ("quantity", "taxable_value", "gst_rate") else ""

        norm_df = pd.DataFrame(norm)
        norm_df["channel"] = request.channel
        norm_df["gstin"] = request.gstin
        norm_df["month"] = request.month

        # Final Date derived from invoice_date
        if "invoice_date" in norm_df.columns:
            norm_df["final_date"] = pd.to_datetime(norm_df["invoice_date"], errors="coerce").dt.date.astype(str)
        else:
            norm_df["final_date"] = ""

        for col in ["taxable_value", "gst_rate", "quantity"]:
            if col in norm_df.columns:
                norm_df[col] = pd.to_numeric(norm_df[col], errors="coerce").fillna(0)

        for col in self.REQUIRED_COLS:
            if col not in norm_df.columns:
                norm_df[col] = "" if col not in ("taxable_value", "gst_rate", "quantity") else 0

        out_dir = os.path.join(os.path.dirname(request.file_path), "normalized")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, f"flipkart_{uuid.uuid4().hex}.csv")
        norm_df[self.REQUIRED_COLS].to_csv(out_path, index=False)

        storage_path = supabase.upload_file(out_path)
        supabase.insert_report_metadata(request.run_id, "flipkart_normalized", storage_path)
        return storage_path
