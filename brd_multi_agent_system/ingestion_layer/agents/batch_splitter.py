"""
Batch Splitter Agent
Splits pivot data into separate files by GST rate for accounting system consumption
"""
import pandas as pd
import uuid
import os
from typing import List, Dict, Any, Optional, Tuple
from ..libs.contracts import BatchSplitResult
from ..libs.utils import ensure_dir
from ..libs.csv_utils import safe_read_csv
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.contracts import BatchSplitResult


class BatchSplitterAgent:
    """
    Agent responsible for splitting pivot data into separate batches by GST rate.
    
    Each batch file contains transactions with only one GST rate, making it easier
    for accounting systems to process and import the data.
    """
    
    def __init__(self, supabase_client: SupabaseClientWrapper):
        """
        Initialize the Batch Splitter Agent.
        
        Args:
            supabase_client: Supabase client for database operations
        """
        self.supabase = supabase_client
    
    def process_pivot_data(self, 
                          pivot_df: pd.DataFrame, 
                          channel: str, 
                          gstin: str, 
                          month: str,
                          run_id: uuid.UUID,
                          output_dir: str = "ingestion_layer/data/batches") -> Tuple[List[str], BatchSplitResult]:
        """
        Split pivot data into separate batch files by GST rate.
        
        Args:
            pivot_df: Pivot DataFrame to split
            channel: Channel name (amazon_mtr, flipkart, etc.)
            gstin: Company GSTIN
            month: Processing month (YYYY-MM format)
            run_id: Unique run identifier
            output_dir: Directory to save batch files
            
        Returns:
            Tuple of (batch_file_paths, result)
        """
        try:
            print(f"    🔄 Starting batch splitting for {len(pivot_df)} pivot records...")
            
            # Validate input data
            if len(pivot_df) == 0:
                return [], BatchSplitResult(
                    success=False,
                    error_message="No pivot data to split"
                )
            
            if 'gst_rate' not in pivot_df.columns:
                return [], BatchSplitResult(
                    success=False,
                    error_message="GST rate column not found in pivot data"
                )
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Get unique GST rates
            gst_rates = sorted(pivot_df['gst_rate'].unique())
            print(f"    📊 Found {len(gst_rates)} unique GST rates: {[f'{rate*100}%' for rate in gst_rates]}")
            
            # Split data by GST rate
            batch_files = []
            batch_summaries = []
            total_records_split = 0
            
            for gst_rate in gst_rates:
                # Filter data for this GST rate
                rate_data = pivot_df[pivot_df['gst_rate'] == gst_rate].copy()
                
                if len(rate_data) == 0:
                    continue
                
                # Generate batch file name
                rate_str = f"{int(gst_rate * 100)}pct" if gst_rate > 0 else "0pct"
                batch_filename = f"{channel}_{gstin}_{month}_{rate_str}_batch.csv"
                batch_filepath = os.path.join(output_dir, batch_filename)
                
                # Save batch file
                rate_data.to_csv(batch_filepath, index=False)
                batch_files.append(batch_filepath)
                
                # Calculate batch summary
                batch_summary = self._calculate_batch_summary(rate_data, gst_rate)
                batch_summaries.append(batch_summary)
                
                total_records_split += len(rate_data)
                
                print(f"    📄 Created batch: {batch_filename} ({len(rate_data)} records, ₹{batch_summary['total_taxable']:,.2f} taxable)")
            
            # Save batch registry to database
            if batch_files:
                self._save_batch_registry(batch_files, batch_summaries, run_id, channel, gstin, month)
            
            # Validate split integrity
            validation_result = self._validate_split_integrity(pivot_df, batch_summaries)
            
            # Create result
            result = BatchSplitResult(
                success=True,
                processed_records=len(pivot_df),
                batch_files_created=len(batch_files),
                total_records_split=total_records_split,
                gst_rates_processed=len(gst_rates),
                batch_summaries=batch_summaries,
                validation_passed=validation_result["valid"]
            )
            
            print(f"    ✅ Batch splitting complete: {len(batch_files)} files created")
            
            return batch_files, result
            
        except Exception as e:
            print(f"    ❌ Error in batch splitting: {e}")
            return [], BatchSplitResult(
                success=False,
                error_message=str(e)
            )
    
    def _calculate_batch_summary(self, batch_df: pd.DataFrame, gst_rate: float) -> Dict[str, Any]:
        """Calculate summary statistics for a batch."""
        
        summary = {
            "gst_rate": gst_rate,
            "record_count": len(batch_df),
            "total_quantity": 0.0,
            "total_taxable": 0.0,
            "total_cgst": 0.0,
            "total_sgst": 0.0,
            "total_igst": 0.0,
            "total_tax": 0.0,
            "total_amount": 0.0,
            "unique_ledgers": 0,
            "unique_fgs": 0
        }
        
        # Calculate totals
        numeric_columns = ['total_quantity', 'total_taxable', 'total_cgst', 'total_sgst', 'total_igst']
        for col in numeric_columns:
            if col in batch_df.columns:
                summary[col.replace('total_', '')] = float(batch_df[col].sum())
        
        # Calculate derived totals
        summary["total_tax"] = summary["cgst"] + summary["sgst"] + summary["igst"]
        summary["total_amount"] = summary["taxable"] + summary["total_tax"]
        
        # Count unique dimensions
        if 'ledger_name' in batch_df.columns:
            summary["unique_ledgers"] = batch_df['ledger_name'].nunique()
        
        if 'fg' in batch_df.columns:
            summary["unique_fgs"] = batch_df['fg'].nunique()
        
        # Round numerical values
        for key in summary:
            if isinstance(summary[key], float):
                summary[key] = round(summary[key], 2)
        
        return summary
    
    def _save_batch_registry(self, 
                            batch_files: List[str], 
                            batch_summaries: List[Dict[str, Any]],
                            run_id: uuid.UUID,
                            channel: str,
                            gstin: str,
                            month: str) -> None:
        """Save batch registry information to Supabase database."""
        
        try:
            records = []
            for i, (file_path, summary) in enumerate(zip(batch_files, batch_summaries)):
                record = {
                    'run_id': str(run_id),
                    'channel': channel,
                    'gstin': gstin,
                    'month': month,
                    'gst_rate': float(summary['gst_rate']),
                    'file_path': os.path.basename(file_path),  # Store just filename
                    'record_count': summary['record_count'],
                    'total_taxable': summary['taxable'],
                    'total_tax': summary['total_tax']
                }
                records.append(record)
            
            # Insert into database
            if records:
                self.supabase.client.table('batch_registry').insert(records).execute()
                print(f"    💾 Saved {len(records)} batch registry records to database")
            
        except Exception as e:
            print(f"    ⚠️  Warning: Could not save batch registry to database: {e}")
    
    def _validate_split_integrity(self, 
                                 original_df: pd.DataFrame, 
                                 batch_summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate that split batches sum up to original totals."""
        
        # Calculate original totals
        original_totals = {
            "record_count": len(original_df),
            "total_taxable": 0.0,
            "total_tax": 0.0
        }
        
        numeric_columns = ['total_taxable', 'total_cgst', 'total_sgst', 'total_igst']
        for col in numeric_columns:
            if col in original_df.columns:
                key = col.replace('total_', '')
                if key == 'taxable':
                    original_totals["total_taxable"] = float(original_df[col].sum())
                elif key in ['cgst', 'sgst', 'igst']:
                    original_totals["total_tax"] += float(original_df[col].sum())
        
        # Calculate batch totals
        batch_totals = {
            "record_count": sum(s['record_count'] for s in batch_summaries),
            "total_taxable": sum(s['taxable'] for s in batch_summaries),
            "total_tax": sum(s['total_tax'] for s in batch_summaries)
        }
        
        # Validate with tolerance
        tolerance = 0.01
        validation_result = {
            "valid": True,
            "errors": [],
            "original_totals": original_totals,
            "batch_totals": batch_totals
        }
        
        # Check record count
        if original_totals["record_count"] != batch_totals["record_count"]:
            validation_result["valid"] = False
            validation_result["errors"].append(
                f"Record count mismatch: {original_totals['record_count']} vs {batch_totals['record_count']}"
            )
        
        # Check taxable amount
        if abs(original_totals["total_taxable"] - batch_totals["total_taxable"]) > tolerance:
            validation_result["valid"] = False
            validation_result["errors"].append(
                f"Taxable amount mismatch: {original_totals['total_taxable']} vs {batch_totals['total_taxable']}"
            )
        
        # Check tax amount
        if abs(original_totals["total_tax"] - batch_totals["total_tax"]) > tolerance:
            validation_result["valid"] = False
            validation_result["errors"].append(
                f"Tax amount mismatch: {original_totals['total_tax']} vs {batch_totals['total_tax']}"
            )
        
        return validation_result
    
    def get_batch_summary(self, batch_summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate overall summary of batch splitting operation."""
        
        if not batch_summaries:
            return {
                "total_batches": 0,
                "total_records": 0,
                "total_taxable": 0.0,
                "total_tax": 0.0,
                "gst_rates": []
            }
        
        summary = {
            "total_batches": len(batch_summaries),
            "total_records": sum(s['record_count'] for s in batch_summaries),
            "total_taxable": sum(s['taxable'] for s in batch_summaries),
            "total_tax": sum(s['total_tax'] for s in batch_summaries),
            "gst_rates": sorted(list(set(s['gst_rate'] for s in batch_summaries))),
            "batch_breakdown": []
        }
        
        # Add per-batch breakdown
        for batch in batch_summaries:
            breakdown = {
                "gst_rate": f"{batch['gst_rate']*100}%",
                "records": batch['record_count'],
                "taxable": f"₹{batch['taxable']:,.2f}",
                "tax": f"₹{batch['total_tax']:,.2f}"
            }
            summary["batch_breakdown"].append(breakdown)
        
        return summary
    
    def validate_batch_files(self, batch_files: List[str]) -> Dict[str, Any]:
        """Validate that batch files exist and contain expected data."""
        
        validation_result = {
            "files_validated": 0,
            "files_missing": 0,
            "files_empty": 0,
            "gst_rate_violations": 0,
            "validation_errors": []
        }
        
        for file_path in batch_files:
            if not os.path.exists(file_path):
                validation_result["files_missing"] += 1
                validation_result["validation_errors"].append(f"File missing: {file_path}")
                continue
            
            try:
                # Read and validate file
                df = safe_read_csv(file_path)
                
                if len(df) == 0:
                    validation_result["files_empty"] += 1
                    validation_result["validation_errors"].append(f"Empty file: {file_path}")
                    continue
                
                # Check GST rate consistency
                if 'gst_rate' in df.columns:
                    unique_rates = df['gst_rate'].nunique()
                    if unique_rates > 1:
                        validation_result["gst_rate_violations"] += 1
                        validation_result["validation_errors"].append(
                            f"Multiple GST rates in batch file: {file_path} ({unique_rates} rates)"
                        )
                
                validation_result["files_validated"] += 1
                
            except Exception as e:
                validation_result["validation_errors"].append(f"Error reading {file_path}: {e}")
        
        return validation_result
    
    def cleanup_batch_files(self, batch_files: List[str]) -> int:
        """Clean up batch files (for testing or cleanup purposes)."""
        cleaned_count = 0
        
        for file_path in batch_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    cleaned_count += 1
            except Exception as e:
                print(f"    ⚠️  Warning: Could not delete {file_path}: {e}")
        
        return cleaned_count
