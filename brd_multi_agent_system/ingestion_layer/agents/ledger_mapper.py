from __future__ import annotations
import pandas as pd
from typing import Tuple, List, Dict

from ..libs.contracts import LedgerMappingRequest, MappingResult
from ..libs.supabase_client import SupabaseClientWrapper


class LedgerMapper:
    """
    Maps channel + state_code to ledger names using ledger_master table.
    Creates approval requests for missing mappings.
    """

    def __init__(self, supabase: SupabaseClientWrapper):
        self.supabase = supabase
        self.cache = {}  # Cache for resolved mappings

    def resolve_ledger_mapping(self, channel: str, state_code: str) -> Tuple[str, bool]:
        """
        Resolve channel + state_code to ledger name.
        
        Returns:
            Tuple[str, bool]: (ledger_name, is_resolved)
            - If resolved: (actual_ledger_name, True)
            - If not resolved: ("", False)
        """
        # Normalize inputs
        channel = channel.lower().strip()
        state_code = state_code.upper().strip()
        
        # Check cache first
        cache_key = f"{channel}|{state_code}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Try to find in ledger_master
        ledger_record = self.supabase.get_ledger_master(channel, state_code)

        if ledger_record:
            ledger_name = ledger_record.get("ledger_name", "")
            self.cache[cache_key] = (ledger_name, True)
            return ledger_name, True
        else:
            # Not found - will need approval
            self.cache[cache_key] = ("", False)
            return "", False

    def create_approval_request(self, channel: str, state_code: str) -> dict:
        """Create approval request for missing ledger mapping."""
        # Generate suggested ledger name
        channel_name = channel.title()
        state_abbr = self._get_state_abbreviation(state_code)
        suggested_ledger = f"{channel_name} Sales - {state_abbr}"
        
        payload = {
            "channel": channel.lower(),
            "state_code": state_code.upper(),
            "suggested_ledger_name": suggested_ledger
        }
        
        return self.supabase.insert_approval_request("ledger", payload)

    def _get_state_abbreviation(self, state_code: str) -> str:
        """Get state abbreviation from full state name."""
        state_abbr_map = {
            "ANDHRA PRADESH": "AP",
            "ARUNACHAL PRADESH": "AR",
            "ASSAM": "AS",
            "BIHAR": "BR",
            "CHHATTISGARH": "CG",
            "GOA": "GA",
            "GUJARAT": "GJ",
            "HARYANA": "HR",
            "HIMACHAL PRADESH": "HP",
            "JHARKHAND": "JH",
            "KARNATAKA": "KA",
            "KERALA": "KL",
            "MADHYA PRADESH": "MP",
            "MAHARASHTRA": "MH",
            "MANIPUR": "MN",
            "MEGHALAYA": "ML",
            "MIZORAM": "MZ",
            "NAGALAND": "NL",
            "ODISHA": "OR",
            "PUNJAB": "PB",
            "RAJASTHAN": "RJ",
            "SIKKIM": "SK",
            "TAMIL NADU": "TN",
            "TELANGANA": "TG",
            "TRIPURA": "TR",
            "UTTAR PRADESH": "UP",
            "UTTARAKHAND": "UK",
            "WEST BENGAL": "WB",
            "DELHI": "DL",
            "JAMMU & KASHMIR": "JK",
            "LADAKH": "LA",
            "CHANDIGARH": "CH",
            "DADRA & NAGAR HAVELI": "DN",
            "DAMAN & DIU": "DD",
            "LAKSHADWEEP": "LD",
            "PUDUCHERRY": "PY"
        }
        
        return state_abbr_map.get(state_code.upper(), state_code[:2])

    def process_dataset(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, MappingResult]:
        """
        Process entire dataset to resolve ledger mappings.
        
        Args:
            df: DataFrame with columns ['channel', 'state_code', ...]
            
        Returns:
            Tuple[pd.DataFrame, MappingResult]: (enriched_df, result)
        """
        if df.empty:
            return df, MappingResult(success=True, mapped_count=0, pending_approvals=0)

        # Ensure required columns exist
        if 'channel' not in df.columns:
            df['channel'] = ""
        if 'state_code' not in df.columns:
            df['state_code'] = ""

        # Initialize result columns
        df['ledger_name'] = ""
        df['ledger_resolved'] = False

        mapped_count = 0
        pending_approvals = 0
        errors = []
        missing_ledgers = set()

        try:
            # Process each unique channel/state combination
            unique_ledgers = df[['channel', 'state_code']].drop_duplicates()
            
            for _, row in unique_ledgers.iterrows():
                channel = str(row['channel']) if pd.notna(row['channel']) else ""
                state_code = str(row['state_code']) if pd.notna(row['state_code']) else ""
                
                if not channel or not state_code:
                    continue
                
                ledger_name, is_resolved = self.resolve_ledger_mapping(channel, state_code)
                
                if is_resolved:
                    # Update all matching rows
                    mask = (df['channel'] == channel) & (df['state_code'] == state_code)
                    df.loc[mask, 'ledger_name'] = ledger_name
                    df.loc[mask, 'ledger_resolved'] = True
                    mapped_count += mask.sum()
                else:
                    # Add to missing ledgers for approval
                    missing_key = (channel, state_code)
                    if missing_key not in missing_ledgers:
                        missing_ledgers.add(missing_key)
                        self.create_approval_request(channel, state_code)
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
        """Get statistics about ledger mapping coverage."""
        if df.empty:
            return {"total_records": 0, "mapped_records": 0, "unmapped_records": 0, "coverage_pct": 0}

        total_records = len(df)
        mapped_records = df['ledger_resolved'].sum() if 'ledger_resolved' in df.columns else 0
        unmapped_records = total_records - mapped_records
        coverage_pct = int((mapped_records / total_records * 100)) if total_records > 0 else 0

        return {
            "total_records": total_records,
            "mapped_records": mapped_records,
            "unmapped_records": unmapped_records,
            "coverage_pct": coverage_pct
        }

    def load_ledger_master_from_excel(self, excel_path: str, approver: str = "system") -> int:
        """
        Load ledger master data from Excel file into database.
        
        Args:
            excel_path: Path to Ledger Master Excel file
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
                'sales_channel': 'channel',
                'platform': 'channel',
                'state': 'state_code',
                'state_name': 'state_code',
                'ledger': 'ledger_name',
                'account_name': 'ledger_name',
                'tally_ledger': 'ledger_name'
            }
            
            # Rename columns if they exist
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df.rename(columns={old_name: new_name}, inplace=True)
            
            # Ensure required columns
            required_cols = ['channel', 'state_code', 'ledger_name']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Clean data
            df = df.dropna(subset=['channel', 'state_code', 'ledger_name'])
            df['channel'] = df['channel'].str.lower().str.strip()
            df['state_code'] = df['state_code'].str.upper().str.strip()
            
            # Insert records
            loaded_count = 0
            for _, row in df.iterrows():
                try:
                    self.supabase.insert_ledger_master(
                        channel=str(row['channel']),
                        state_code=str(row['state_code']),
                        ledger_name=str(row['ledger_name']),
                        approved_by=approver
                    )
                    loaded_count += 1
                except Exception as e:
                    # Skip duplicates or other errors
                    continue
            
            return loaded_count
            
        except Exception as e:
            raise ValueError(f"Failed to load ledger master from {excel_path}: {str(e)}")

    def generate_default_ledgers(self, channels: List[str], states: List[str], approver: str = "system") -> int:
        """
        Generate default ledger mappings for given channels and states.
        
        Args:
            channels: List of channel names
            states: List of state codes
            approver: Who approved these mappings
            
        Returns:
            int: Number of ledgers created
        """
        created_count = 0
        
        for channel in channels:
            for state in states:
                try:
                    channel_clean = channel.lower().strip()
                    state_clean = state.upper().strip()
                    
                    # Check if mapping already exists
                    existing = self.supabase.get_ledger_master(channel_clean, state_clean)
                    if existing:
                        continue
                    
                    # Generate ledger name
                    channel_title = channel_clean.title()
                    state_abbr = self._get_state_abbreviation(state_clean)
                    ledger_name = f"{channel_title} Sales - {state_abbr}"
                    
                    # Insert mapping
                    self.supabase.insert_ledger_master(
                        channel=channel_clean,
                        state_code=state_clean,
                        ledger_name=ledger_name,
                        approved_by=approver
                    )
                    created_count += 1
                    
                except Exception as e:
                    # Skip errors and continue
                    continue
        
        return created_count
