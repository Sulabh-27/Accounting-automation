from __future__ import annotations
import os
import uuid
from typing import Dict, Optional
import pandas as pd
from ..libs.utils import safe_colname
from ..libs.csv_utils import safe_read_csv
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.utils import ensure_dir, safe_colname
from ..libs.column_mappings import AMAZON_MTR_MAPPING, AMAZON_STR_MAPPING, FLIPKART_MAPPING, PEPPERFRY_MAPPING, STANDARD_SCHEMA
from ..libs.contracts import IngestionRequest


class UniversalAgent:
    """
    Universal ingestion agent that can handle different channels and report types
    based on configurable column mappings.
    """

    def __init__(self):
        self.mappings = {
            "amazon_mtr": AMAZON_MTR_MAPPING,
            "amazon_str": AMAZON_STR_MAPPING,
            "flipkart": FLIPKART_MAPPING,
            "pepperfry": PEPPERFRY_MAPPING
        }

    def read_file(self, file_path: str, sheet_name: Optional[str] = None, header: int = 0) -> pd.DataFrame:
        """Read Excel or CSV file with flexible options."""
        if file_path.lower().endswith(('.xlsx', '.xls')):
            return pd.read_excel(file_path, sheet_name=sheet_name, header=header)
        else:
            return safe_read_csv(file_path)

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to lowercase with underscores."""
        df = df.copy()
        df.columns = [safe_colname(c) for c in df.columns]
        return df

    def map_columns(self, df: pd.DataFrame, mapping: Dict[str, list], request: IngestionRequest) -> pd.DataFrame:
        """Map source columns to standard schema using the provided mapping."""
        norm = {}
        
        # Map each target column
        for target, candidates in mapping.items():
            found = False
            for candidate in candidates:
                candidate_norm = safe_colname(candidate)
                if candidate_norm in df.columns:
                    if target == "gst_rate":
                        # Special handling for GST rate - sum CGST + SGST + IGST
                        gst_cols = [c for c in ["cgst_rate", "sgst_rate", "igst_rate", "utgst_rate"] if c in df.columns]
                        if gst_cols:
                            gst_sum = df[gst_cols].fillna(0).sum(axis=1)
                            norm[target] = gst_sum
                        else:
                            norm[target] = pd.to_numeric(df[candidate_norm], errors="coerce").fillna(0)
                    else:
                        norm[target] = df[candidate_norm]
                    found = True
                    break
            
            if not found:
                # Default values for missing columns
                if target in ["quantity", "taxable_value", "gst_rate"]:
                    norm[target] = 0
                else:
                    norm[target] = ""

        norm_df = pd.DataFrame(norm)
        
        # Add metadata
        norm_df["channel"] = request.channel
        norm_df["gstin"] = request.gstin
        norm_df["month"] = request.month
        
        # Add type column if not present
        if "type" not in norm_df.columns:
            if "transaction_type" in norm_df.columns:
                # Filter and map transaction types
                type_map = {"shipment": "shipment", "refund": "refund", "return": "refund"}
                norm_df["type"] = norm_df["transaction_type"].str.lower().map(type_map).fillna("shipment")
            else:
                norm_df["type"] = "shipment"
        
        # Ensure numeric columns
        for col in ["taxable_value", "gst_rate", "quantity"]:
            if col in norm_df.columns:
                norm_df[col] = pd.to_numeric(norm_df[col], errors="coerce").fillna(0)
        
        return norm_df

    def process(self, request: IngestionRequest, supabase: SupabaseClientWrapper, 
                asin_to_sku: Optional[Dict[str, str]] = None) -> str:
        """Process any supported file type based on the report type."""
        
        # Get the appropriate mapping
        mapping = self.mappings.get(request.report_type)
        if not mapping:
            raise ValueError(f"Unsupported report type: {request.report_type}")
        
        # Read and normalize the file
        df = self.read_file(request.file_path)
        df = self.normalize_columns(df)
        
        # Apply filters based on report type
        if request.report_type == "amazon_mtr":
            # Filter to Shipment & Refund only
            type_col = None
            for c in ["transaction_type", "type"]:
                if c in df.columns:
                    type_col = c
                    break
            if type_col:
                df = df[df[type_col].str.lower().isin(["shipment", "refund"])].copy()
        
        elif request.report_type == "amazon_str":
            # Filter inter-state transactions
            ship_state = None
            seller_state = None
            for c in ["ship_to_state", "destination_state"]:
                if c in df.columns:
                    ship_state = c
                    break
            for c in ["ship_from_state", "seller_state_code"]:
                if c in df.columns:
                    seller_state = c
                    break
            if ship_state and seller_state:
                df = df[df[ship_state] != df[seller_state]].copy()
        
        # Map columns to standard schema
        norm_df = self.map_columns(df, mapping, request)
        
        # Apply ASIN to SKU mapping if provided
        if asin_to_sku and "asin" in norm_df.columns:
            if "sku" not in norm_df.columns or norm_df["sku"].isna().all():
                norm_df["sku"] = norm_df["asin"].map(asin_to_sku).fillna(norm_df.get("sku", ""))
        
        # Ensure all required columns exist
        for col in STANDARD_SCHEMA:
            if col not in norm_df.columns:
                if col in ["taxable_value", "gst_rate", "quantity"]:
                    norm_df[col] = 0
                else:
                    norm_df[col] = ""
        
        # Save normalized file
        out_dir = os.path.join(os.path.dirname(request.file_path), "normalized")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, f"{request.report_type}_{uuid.uuid4().hex}.csv")
        norm_df[STANDARD_SCHEMA].to_csv(out_path, index=False)
        
        # Upload to Supabase
        storage_path = supabase.upload_file(out_path)
        supabase.insert_report_metadata(request.run_id, f"{request.report_type}_normalized", storage_path)
        
        return storage_path
