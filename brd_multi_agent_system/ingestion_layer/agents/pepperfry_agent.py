from __future__ import annotations
import os
import uuid
import pandas as pd

from ..libs.contracts import IngestionRequest
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.utils import ensure_dir, safe_colname


class PepperfryAgent:
    """Pepperfry sales + returns ingestion agent.

    - Merges sales and returns
    - Recomputes taxable on returned qty
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
        "is_return",
    ]

    def process(self, request_sales_path: str, request_returns_path: str, request: IngestionRequest, supabase: SupabaseClientWrapper) -> str:
        # Read Excel or CSV files
        if request_sales_path.lower().endswith(('.xlsx', '.xls')):
            sales = pd.read_excel(request_sales_path)
        else:
            sales = pd.read_csv(request_sales_path)
            
        if request_returns_path.lower().endswith(('.xlsx', '.xls')):
            returns = pd.read_excel(request_returns_path)
        else:
            returns = pd.read_csv(request_returns_path)

        sales.columns = [safe_colname(c) for c in sales.columns]
        returns.columns = [safe_colname(c) for c in returns.columns]

        def map_cols(df: pd.DataFrame) -> pd.DataFrame:
            col_map = {
                "invoice_date": ["invoice_date", "date"],
                "order_id": ["order_id", "order"],
                "sku": ["sku", "item_sku"],
                "quantity": ["quantity", "qty"],
                "taxable_value": ["taxable_value", "net_amount", "item_price"],
                "gst_rate": ["gst_rate", "tax_rate"],
                "state_code": ["state_code", "ship_to_state_code", "state"],
            }
            norm = {}
            for target, candidates in col_map.items():
                for c in candidates:
                    if c in df.columns:
                        norm[target] = df[c]
                        break
                else:
                    norm[target] = 0 if target in ("quantity", "taxable_value", "gst_rate") else ""
            return pd.DataFrame(norm)

        s_df = map_cols(sales)
        r_df = map_cols(returns)

        # mark returns
        s_df["is_return"] = False
        r_df["is_return"] = True
        # returned quantities should be negative
        r_df["quantity"] = pd.to_numeric(r_df["quantity"], errors="coerce").fillna(0) * -1
        # recompute taxable proportionally if needed (keep as provided if present)
        for col in ["taxable_value", "gst_rate"]:
            s_df[col] = pd.to_numeric(s_df[col], errors="coerce").fillna(0)
            r_df[col] = pd.to_numeric(r_df[col], errors="coerce").fillna(0)

        merged = pd.concat([s_df, r_df], ignore_index=True, sort=False)
        merged["channel"] = request.channel
        merged["gstin"] = request.gstin
        merged["month"] = request.month

        # ensure required columns
        for col in self.REQUIRED_COLS:
            if col not in merged.columns:
                merged[col] = "" if col not in ("taxable_value", "gst_rate", "quantity", "is_return") else 0

        out_dir = os.path.join(os.path.dirname(request.file_path), "normalized")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, f"pepperfry_{uuid.uuid4().hex}.csv")
        merged[self.REQUIRED_COLS].to_csv(out_path, index=False)

        storage_path = supabase.upload_file(out_path)
        supabase.insert_report_metadata(request.run_id, "pepperfry_normalized", storage_path)
        return storage_path
