"""
Pivot Generator Agent
Groups enriched data by dimensions and aggregates totals for accounting systems
"""
import pandas as pd
import uuid
from typing import Dict, Any, List, Tuple
from datetime import datetime

from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.pivot_rules import PivotRulesEngine
from ..libs.summarizer import Summarizer
from ..libs.contracts import PivotResult


class PivotGeneratorAgent:
    """
    Agent responsible for pivoting enriched transaction data into accounting summaries.
    
    Groups data by key dimensions (GSTIN, Month, Ledger, FG, GST Rate) and 
    aggregates quantities and tax amounts for accounting system consumption.
    """
    
    def __init__(self, supabase_client: SupabaseClientWrapper):
        """
        Initialize the Pivot Generator Agent.
        
        Args:
            supabase_client: Supabase client for database operations
        """
        self.supabase = supabase_client
        self.pivot_engine = PivotRulesEngine()
        self.summarizer = Summarizer()
    
    def process_dataset(self, 
                       df: pd.DataFrame, 
                       channel: str, 
                       gstin: str, 
                       month: str,
                       run_id: uuid.UUID) -> Tuple[pd.DataFrame, PivotResult]:
        """
        Process enriched dataset and generate pivot summaries.
        
        Args:
            df: Enriched DataFrame with FG, Ledger, Taxes, Invoice Numbers
            channel: Channel name (amazon_mtr, flipkart, etc.)
            gstin: Company GSTIN
            month: Processing month (YYYY-MM format)
            run_id: Unique run identifier
            
        Returns:
            Tuple of (pivot_df, result)
        """
        try:
            print(f"    🔄 Starting pivot generation for {len(df)} records...")
            
            # Validate input data
            validation_result = self._validate_input_data(df)
            if not validation_result["valid"]:
                return pd.DataFrame(), PivotResult(
                    success=False,
                    error_message=f"Input validation failed: {validation_result['errors']}"
                )
            
            # Get pivot dimensions and measures
            dimensions = self.pivot_engine.get_pivot_dimensions(channel)
            measures = self.pivot_engine.get_pivot_measures(channel)
            
            # Group and aggregate data
            pivot_df = self._create_pivot_summary(df, dimensions, measures, gstin, month)
            
            # Apply business rules and validations
            pivot_df = self._apply_business_rules(pivot_df, channel)
            
            # Generate summary statistics
            summary_stats = self.summarizer.generate_pivot_summary(pivot_df)
            
            # Save to database
            if len(pivot_df) > 0:
                self._save_pivot_summaries(pivot_df, run_id, channel, gstin, month)
            
            # Create result
            result = PivotResult(
                success=True,
                processed_records=len(df),
                pivot_records=len(pivot_df),
                total_taxable_amount=summary_stats["total_taxable"],
                total_tax_amount=summary_stats["total_tax"],
                unique_ledgers=summary_stats["unique_ledgers"],
                unique_fgs=summary_stats["unique_fgs"],
                gst_rate_breakdown=summary_stats["gst_rate_breakdown"]
            )
            
            print(f"    ✅ Pivot generation complete: {len(pivot_df)} summary records")
            
            return pivot_df, result
            
        except Exception as e:
            print(f"    ❌ Error in pivot generation: {e}")
            return pd.DataFrame(), PivotResult(
                success=False,
                error_message=str(e)
            )
    
    def _validate_input_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate input DataFrame has required columns."""
        required_columns = [
            'gstin', 'month', 'ledger_name', 'fg', 'quantity',
            'taxable_value', 'cgst', 'sgst', 'igst', 'gst_rate'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return {
                "valid": False,
                "errors": f"Missing required columns: {missing_columns}"
            }
        
        # Check for null values in critical columns
        critical_nulls = []
        for col in ['gstin', 'month', 'gst_rate']:
            if df[col].isnull().any():
                critical_nulls.append(col)
        
        if critical_nulls:
            return {
                "valid": False,
                "errors": f"Null values found in critical columns: {critical_nulls}"
            }
        
        return {"valid": True, "errors": []}
    
    def _create_pivot_summary(self, 
                             df: pd.DataFrame, 
                             dimensions: List[str], 
                             measures: List[str],
                             gstin: str,
                             month: str) -> pd.DataFrame:
        """Create pivot summary by grouping and aggregating data."""
        
        # Ensure we have the required dimensions
        groupby_columns = []
        for dim in dimensions:
            if dim in df.columns:
                groupby_columns.append(dim)
        
        if not groupby_columns:
            raise ValueError(f"No valid groupby columns found from dimensions: {dimensions}")
        
        # Group by dimensions and aggregate measures
        agg_dict = {}
        for measure in measures:
            if measure in df.columns:
                if measure == 'quantity':
                    agg_dict[measure] = 'sum'
                else:
                    agg_dict[measure] = 'sum'
        
        if not agg_dict:
            raise ValueError(f"No valid measures found from: {measures}")
        
        # Perform groupby and aggregation
        pivot_df = df.groupby(groupby_columns).agg(agg_dict).reset_index()
        
        # Rename columns to match expected output format
        column_mapping = {
            'quantity': 'total_quantity',
            'taxable_value': 'total_taxable',
            'cgst': 'total_cgst',
            'sgst': 'total_sgst',
            'igst': 'total_igst'
        }
        
        pivot_df = pivot_df.rename(columns=column_mapping)
        
        # Add computed columns
        if all(col in pivot_df.columns for col in ['total_cgst', 'total_sgst', 'total_igst']):
            pivot_df['total_tax'] = pivot_df['total_cgst'] + pivot_df['total_sgst'] + pivot_df['total_igst']
        
        if all(col in pivot_df.columns for col in ['total_taxable', 'total_tax']):
            pivot_df['total_amount'] = pivot_df['total_taxable'] + pivot_df['total_tax']
        
        # Round numerical columns
        numeric_columns = ['total_quantity', 'total_taxable', 'total_cgst', 'total_sgst', 
                          'total_igst', 'total_tax', 'total_amount']
        for col in numeric_columns:
            if col in pivot_df.columns:
                pivot_df[col] = pivot_df[col].round(2)
        
        return pivot_df
    
    def _apply_business_rules(self, pivot_df: pd.DataFrame, channel: str) -> pd.DataFrame:
        """Apply channel-specific business rules to pivot data."""
        
        # Remove zero-value records (optional based on business requirements)
        if 'total_taxable' in pivot_df.columns:
            pivot_df = pivot_df[pivot_df['total_taxable'] > 0].copy()
        
        # Sort by key dimensions for consistent output
        sort_columns = []
        for col in ['gstin', 'month', 'gst_rate', 'ledger_name', 'fg']:
            if col in pivot_df.columns:
                sort_columns.append(col)
        
        if sort_columns:
            pivot_df = pivot_df.sort_values(sort_columns).reset_index(drop=True)
        
        return pivot_df
    
    def _save_pivot_summaries(self, 
                             pivot_df: pd.DataFrame, 
                             run_id: uuid.UUID,
                             channel: str,
                             gstin: str,
                             month: str) -> None:
        """Save pivot summaries to Supabase database."""
        
        try:
            # Prepare records for database insertion
            records = []
            for _, row in pivot_df.iterrows():
                record = {
                    'run_id': str(run_id),
                    'channel': channel,
                    'gstin': gstin,
                    'month': month,
                    'gst_rate': float(row.get('gst_rate', 0)),
                    'ledger': row.get('ledger_name', ''),
                    'fg': row.get('fg', ''),
                    'total_quantity': float(row.get('total_quantity', 0)),
                    'total_taxable': float(row.get('total_taxable', 0)),
                    'total_cgst': float(row.get('total_cgst', 0)),
                    'total_sgst': float(row.get('total_sgst', 0)),
                    'total_igst': float(row.get('total_igst', 0))
                }
                records.append(record)
            
            # Insert into database
            if records:
                self.supabase.client.table('pivot_summaries').insert(records).execute()
                print(f"    💾 Saved {len(records)} pivot summary records to database")
            
        except Exception as e:
            print(f"    ⚠️  Warning: Could not save pivot summaries to database: {e}")
    
    def get_pivot_summary(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for pivot data."""
        return self.summarizer.generate_pivot_summary(pivot_df)
    
    def validate_pivot_data(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """Validate pivot data integrity."""
        
        validation_result = {
            "total_records": len(pivot_df),
            "valid_records": 0,
            "invalid_records": 0,
            "missing_data": 0,
            "negative_values": 0,
            "validation_errors": []
        }
        
        if len(pivot_df) == 0:
            validation_result["validation_errors"].append("No pivot data to validate")
            return validation_result
        
        for index, row in pivot_df.iterrows():
            is_valid = True
            
            # Check for missing critical data
            if pd.isna(row.get('gstin')) or pd.isna(row.get('month')) or pd.isna(row.get('gst_rate')):
                validation_result["missing_data"] += 1
                is_valid = False
            
            # Check for negative values where not expected
            numeric_columns = ['total_quantity', 'total_taxable', 'total_cgst', 'total_sgst', 'total_igst']
            for col in numeric_columns:
                if col in row and row[col] < 0:
                    validation_result["negative_values"] += 1
                    is_valid = False
                    break
            
            if is_valid:
                validation_result["valid_records"] += 1
            else:
                validation_result["invalid_records"] += 1
        
        return validation_result
    
    def export_pivot_csv(self, pivot_df: pd.DataFrame, output_path: str) -> bool:
        """Export pivot data to CSV file."""
        try:
            pivot_df.to_csv(output_path, index=False)
            print(f"    💾 Pivot data exported to: {output_path}")
            return True
        except Exception as e:
            print(f"    ❌ Error exporting pivot CSV: {e}")
            return False
