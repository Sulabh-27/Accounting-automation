from __future__ import annotations
import os
import uuid
from typing import Dict
import pandas as pd

from ..libs.contracts import IngestionRequest
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.utils import ensure_dir, safe_colname


class AmazonSTRAgent:
    """Amazon STR ingestion agent.

    - Filters inter-state shipments when possible
    - Maps ASIN to SKU using a given mapping
    - Uploads normalized CSV to Supabase storage
    """

    REQUIRED_COLS = [
        "invoice_date",
        "order_id",
        "asin",
        "sku",
        "quantity",
        "taxable_value",
        "gst_rate",
        "state_code",
        "channel",
        "gstin",
        "month",
    ]

    def process(self, request: IngestionRequest, supabase: SupabaseClientWrapper, asin_to_sku: Dict[str, str] | None = None) -> str:
        # Read Excel or CSV file
        if request.file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(request.file_path)
        else:
            df = pd.read_csv(request.file_path)
        df.columns = [safe_colname(c) for c in df.columns]

        # Attempt to filter inter-state rows
        ship_state = None
        seller_state = None
        for c in ["ship_to_state_code", "ship_state_code", "state_code"]:
            if c in df.columns:
                ship_state = c
                break
        for c in ["seller_state_code", "from_state_code", "origin_state_code"]:
            if c in df.columns:
                seller_state = c
                break
        if ship_state and seller_state:
            df = df[df[ship_state] != df[seller_state]].copy()

        # Build normalized frame
        col_map = {
            "invoice_date": ["invoice_date", "posting_date", "date"],
            "order_id": ["order_id", "amazon_order_id", "order"],
            "asin": ["asin", "asin1"],
            "quantity": ["quantity", "qty"],
            "taxable_value": ["taxable_value", "item_price", "net_amount"],
            "gst_rate": ["gst_rate", "tax_rate"],
            "state_code": [ship_state] if ship_state else ["state_code"],
        }
        norm = {}
        for target, candidates in col_map.items():
            for c in candidates:
                if c and c in df.columns:
                    norm[target] = df[c]
                    break
            else:
                norm[target] = 0 if target in ("quantity", "taxable_value", "gst_rate") else ""

        norm_df = pd.DataFrame(norm)

        # Map ASIN to SKU
        asin_to_sku = asin_to_sku or {}
        if "asin" in norm_df.columns:
            norm_df["sku"] = norm_df["asin"].map(asin_to_sku).fillna("")
        else:
            norm_df["sku"] = ""

        norm_df["channel"] = request.channel
        norm_df["gstin"] = request.gstin
        norm_df["month"] = request.month

        # numeric cleanup
        for col in ["taxable_value", "gst_rate", "quantity"]:
            if col in norm_df.columns:
                norm_df[col] = pd.to_numeric(norm_df[col], errors="coerce").fillna(0)

        for col in self.REQUIRED_COLS:
            if col not in norm_df.columns:
                norm_df[col] = "" if col not in ("taxable_value", "gst_rate", "quantity") else 0

        out_dir = os.path.join(os.path.dirname(request.file_path), "normalized")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, f"amazon_str_{uuid.uuid4().hex}.csv")
        norm_df[self.REQUIRED_COLS].to_csv(out_path, index=False)

        storage_path = supabase.upload_file(out_path)
        supabase.insert_report_metadata(request.run_id, "amazon_str_normalized", storage_path)
        return storage_path
