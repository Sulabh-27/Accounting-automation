from __future__ import annotations
import pandas as pd
from typing import Tuple, List, Dict

from ..libs.contracts import ItemMappingRequest, MappingResult
from ..libs.supabase_client import SupabaseClientWrapper


class ItemMasterResolver:
    """
    Resolves SKU/ASIN to Final Goods (FG) mapping using item_master table.
    Creates approval requests for missing mappings.
    """

    def __init__(self, supabase: SupabaseClientWrapper):
        self.supabase = supabase
        self.cache = {}  # Cache for resolved mappings

    def resolve_item_mapping(self, sku: str, asin: str = None) -> Tuple[str, bool]:
        """
        Resolve SKU/ASIN to FG name.
        
        Returns:
            Tuple[str, bool]: (fg_name, is_resolved)
            - If resolved: (actual_fg_name, True)
            - If not resolved: ("", False)
        """
        # Check cache first
        cache_key = f"{sku}|{asin or ''}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Try to find in item_master by SKU first
        item_record = self.supabase.get_item_master(sku=sku)
        
        # If not found by SKU, try by ASIN
        if not item_record and asin:
            item_record = self.supabase.get_item_master(asin=asin)

        if item_record:
            fg_name = item_record.get("fg", "")
            self.cache[cache_key] = (fg_name, True)
            return fg_name, True
        else:
            # Not found - will need approval
            self.cache[cache_key] = ("", False)
            return "", False

    def create_approval_request(self, sku: str, asin: str = None, item_code: str = None) -> dict:
        """Create approval request for missing item mapping."""
        payload = {
            "sku": sku,
            "asin": asin,
            "item_code": item_code,
            "suggested_fg": f"{sku}_FG",  # Suggested name
            "gst_rate": 0.18  # Default GST rate
        }
        
        return self.supabase.insert_approval_request("item", payload)

    def process_dataset(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, MappingResult]:
        """
        Process entire dataset to resolve item mappings.
        
        Args:
            df: DataFrame with columns ['sku', 'asin', ...]
            
        Returns:
            Tuple[pd.DataFrame, MappingResult]: (enriched_df, result)
        """
        if df.empty:
            return df, MappingResult(success=True, mapped_count=0, pending_approvals=0)

        # Ensure required columns exist
        if 'sku' not in df.columns:
            df['sku'] = ""
        if 'asin' not in df.columns:
            df['asin'] = ""

        # Initialize result columns
        df['fg'] = ""
        df['item_resolved'] = False

        mapped_count = 0
        pending_approvals = 0
        errors = []
        missing_items = set()

        try:
            # Process each unique SKU/ASIN combination
            unique_items = df[['sku', 'asin']].drop_duplicates()
            
            for _, row in unique_items.iterrows():
                sku = str(row['sku']) if pd.notna(row['sku']) else ""
                asin = str(row['asin']) if pd.notna(row['asin']) else ""
                
                if not sku and not asin:
                    continue
                
                fg_name, is_resolved = self.resolve_item_mapping(sku, asin)
                
                if is_resolved:
                    # Update all matching rows
                    mask = (df['sku'] == sku) & (df['asin'] == asin)
                    df.loc[mask, 'fg'] = fg_name
                    df.loc[mask, 'item_resolved'] = True
                    mapped_count += mask.sum()
                else:
                    # Add to missing items for approval
                    missing_key = (sku, asin)
                    if missing_key not in missing_items:
                        missing_items.add(missing_key)
                        self.create_approval_request(sku, asin)
                        pending_approvals += 1

        except Exception as e:
            errors.append(f"Error processing dataset: {str(e)}")

        result = MappingResult(
            success=len(errors) == 0,
            mapped_count=mapped_count,
            pending_approvals=pending_approvals,
            errors=errors
        )

        return df, result

    def get_mapping_stats(self, df: pd.DataFrame) -> Dict[str, int]:
        """Get statistics about item mapping coverage."""
        if df.empty:
            return {"total_items": 0, "mapped_items": 0, "unmapped_items": 0, "coverage_pct": 0}

        total_items = len(df)
        mapped_items = df['item_resolved'].sum() if 'item_resolved' in df.columns else 0
        unmapped_items = total_items - mapped_items
        coverage_pct = int((mapped_items / total_items * 100)) if total_items > 0 else 0

        return {
            "total_items": total_items,
            "mapped_items": mapped_items,
            "unmapped_items": unmapped_items,
            "coverage_pct": coverage_pct
        }

    def load_item_master_from_excel(self, excel_path: str, approver: str = "system") -> int:
        """
        Load item master data from Excel file into database.
        
        Args:
            excel_path: Path to Item Master Excel file
            approver: Who approved these mappings
            
        Returns:
            int: Number of records loaded
        """
        try:
            df = pd.read_excel(excel_path)
            
            # Normalize column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            # Map common column variations
            column_mapping = {
                'sales_portal_sku': 'sku',
                'portal_sku': 'sku',
                'amazon_asin': 'asin',
                'asin_code': 'asin',
                'tally_new_sku': 'fg',
                'final_goods': 'fg',
                'fg_name': 'fg',
                'item_name': 'fg',
                'gst_rate_%': 'gst_rate',
                'tax_rate': 'gst_rate'
            }
            
            # Rename columns if they exist
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df.rename(columns={old_name: new_name}, inplace=True)
            
            # Ensure required columns
            required_cols = ['sku', 'fg']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Set defaults
            if 'asin' not in df.columns:
                df['asin'] = ""
            if 'item_code' not in df.columns:
                df['item_code'] = df['sku']
            if 'gst_rate' not in df.columns:
                df['gst_rate'] = 0.18
            
            # Clean data
            df = df.dropna(subset=['sku', 'fg'])
            df['gst_rate'] = pd.to_numeric(df['gst_rate'], errors='coerce').fillna(0.18)
            
            # Insert records
            loaded_count = 0
            for _, row in df.iterrows():
                try:
                    self.supabase.insert_item_master(
                        sku=str(row['sku']),
                        asin=str(row['asin']) if pd.notna(row['asin']) else "",
                        item_code=str(row['item_code']),
                        fg=str(row['fg']),
                        gst_rate=float(row['gst_rate']),
                        approved_by=approver
                    )
                    loaded_count += 1
                except Exception as e:
                    # Skip duplicates or other errors
                    continue
            
            return loaded_count
            
        except Exception as e:
            raise ValueError(f"Failed to load item master from {excel_path}: {str(e)}")
