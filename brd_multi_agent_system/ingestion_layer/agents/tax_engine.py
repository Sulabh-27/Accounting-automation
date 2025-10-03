"""
Tax Engine Agent
Processes datasets and applies channel-specific GST computation rules
"""
import pandas as pd
import uuid
from typing import Dict, List, Tuple
from datetime import datetime

from ..libs.contracts import TaxComputationRequest, TaxComputationResult
from ..libs.tax_rules import TaxRulesEngine
from ..libs.supabase_client import SupabaseClientWrapper


class TaxEngine:
    """
    Tax computation agent that applies GST rules based on channel and transaction type.
    Processes enriched datasets from Part-2 and adds tax computations.
    """
    
    def __init__(self, supabase_client: SupabaseClientWrapper):
        """
        Initialize Tax Engine.
        
        Args:
            supabase_client: Supabase client for data persistence
        """
        self.supabase = supabase_client
        self.tax_rules = None  # Will be initialized per request
    
    def process_dataset(self, 
                       df: pd.DataFrame, 
                       channel: str, 
                       gstin: str,
                       run_id: uuid.UUID) -> Tuple[pd.DataFrame, TaxComputationResult]:
        """
        Process dataset and apply tax computations.
        
        Args:
            df: Input dataset with normalized data
            channel: Channel name (amazon_mtr, flipkart, etc.)
            gstin: Company GSTIN
            run_id: Run ID for tracking
            
        Returns:
            Tuple of (enriched_dataframe, computation_result)
        """
        try:
            # Initialize tax rules engine
            self.tax_rules = TaxRulesEngine(gstin)
            
            # Create a copy to avoid modifying original
            enriched_df = df.copy()
            
            # Add tax computation columns
            tax_columns = ['cgst', 'sgst', 'igst', 'total_tax', 'total_amount', 'shipping_value']
            for col in tax_columns:
                enriched_df[col] = 0.0
            
            # Process each row
            tax_records = []
            successful_computations = 0
            failed_computations = 0
            
            for index, row in enriched_df.iterrows():
                try:
                    # Extract required fields
                    taxable_value = float(row.get('taxable_value', 0))
                    gst_rate = float(row.get('gst_rate', 0))
                    state_code = str(row.get('state_code', ''))
                    sku = str(row.get('sku', ''))
                    
                    # Apply channel-specific tax computation
                    tax_computation = self._compute_tax_by_channel(
                        channel=channel,
                        taxable_value=taxable_value,
                        gst_rate=gst_rate,
                        state_code=state_code,
                        row_data=row
                    )
                    
                    # Update dataframe with computed values
                    for key, value in tax_computation.items():
                        if key in enriched_df.columns:
                            enriched_df.at[index, key] = value
                    
                    # Prepare record for Supabase
                    tax_record = {
                        'run_id': str(run_id),
                        'channel': channel,
                        'gstin': gstin,
                        'state_code': state_code,
                        'sku': sku,
                        'taxable_value': tax_computation['taxable_value'],
                        'shipping_value': tax_computation.get('shipping_value', 0.0),
                        'cgst': tax_computation['cgst'],
                        'sgst': tax_computation['sgst'],
                        'igst': tax_computation['igst'],
                        'gst_rate': tax_computation['gst_rate']
                    }
                    tax_records.append(tax_record)
                    successful_computations += 1
                    
                except Exception as e:
                    print(f"  ⚠️  Tax computation failed for row {index}: {e}")
                    failed_computations += 1
                    continue
            
            # Store tax computations in Supabase
            if tax_records:
                self._store_tax_computations(tax_records)
            
            # Create result summary
            result = TaxComputationResult(
                success=True,
                processed_records=len(df),
                successful_computations=successful_computations,
                failed_computations=failed_computations,
                total_tax_amount=float(enriched_df['total_tax'].sum()),
                total_taxable_amount=float(enriched_df['taxable_value'].sum())
            )
            
            return enriched_df, result
            
        except Exception as e:
            result = TaxComputationResult(
                success=False,
                processed_records=len(df),
                successful_computations=0,
                failed_computations=len(df),
                error_message=str(e)
            )
            return df, result
    
    def _compute_tax_by_channel(self, 
                               channel: str, 
                               taxable_value: float, 
                               gst_rate: float, 
                               state_code: str,
                               row_data: pd.Series) -> Dict[str, float]:
        """
        Apply channel-specific tax computation logic.
        
        Args:
            channel: Channel name
            taxable_value: Taxable amount
            gst_rate: GST rate
            state_code: Customer state
            row_data: Full row data for additional context
            
        Returns:
            Dict with computed tax values
        """
        # Get shipping value if available
        shipping_value = float(row_data.get('shipping_value', 0))
        
        if channel == 'amazon_mtr':
            return self.tax_rules.compute_amazon_mtr_tax(
                taxable_value=taxable_value,
                gst_rate=gst_rate,
                customer_state=state_code,
                shipping_value=shipping_value
            )
        
        elif channel == 'amazon_str':
            return self.tax_rules.compute_amazon_str_tax(
                taxable_value=taxable_value,
                gst_rate=gst_rate,
                customer_state=state_code,
                shipping_value=shipping_value
            )
        
        elif channel == 'flipkart':
            # For Flipkart, seller state might be in the data
            seller_state = row_data.get('seller_state', None)
            return self.tax_rules.compute_flipkart_tax(
                taxable_value=taxable_value,
                gst_rate=gst_rate,
                customer_state=state_code,
                seller_state=seller_state,
                shipping_value=shipping_value
            )
        
        elif channel == 'pepperfry':
            # For Pepperfry, handle returns
            returned_qty = int(row_data.get('returned_qty', 0))
            total_qty = int(row_data.get('quantity', 1))
            return self.tax_rules.compute_pepperfry_tax(
                taxable_value=taxable_value,
                gst_rate=gst_rate,
                customer_state=state_code,
                returned_qty=returned_qty,
                total_qty=total_qty,
                shipping_value=shipping_value
            )
        
        else:
            # Default computation (same as Amazon MTR)
            return self.tax_rules.compute_amazon_mtr_tax(
                taxable_value=taxable_value,
                gst_rate=gst_rate,
                customer_state=state_code,
                shipping_value=shipping_value
            )
    
    def _store_tax_computations(self, tax_records: List[Dict]):
        """
        Store tax computation records in Supabase.
        
        Args:
            tax_records: List of tax computation records
        """
        try:
            # Batch insert tax computations
            response = self.supabase.client.table('tax_computations').insert(tax_records).execute()
            print(f"  💾 Stored {len(tax_records)} tax computation records")
        except Exception as e:
            print(f"  ⚠️  Failed to store tax computations: {e}")
    
    def get_tax_summary(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Generate tax computation summary statistics.
        
        Args:
            df: Dataset with tax computations
            
        Returns:
            Dict with summary statistics
        """
        if df.empty:
            return {
                "total_records": 0,
                "total_taxable_amount": 0.0,
                "total_cgst": 0.0,
                "total_sgst": 0.0,
                "total_igst": 0.0,
                "total_tax": 0.0,
                "total_amount": 0.0
            }
        
        return {
            "total_records": len(df),
            "total_taxable_amount": float(df['taxable_value'].sum()),
            "total_cgst": float(df.get('cgst', pd.Series([0])).sum()),
            "total_sgst": float(df.get('sgst', pd.Series([0])).sum()),
            "total_igst": float(df.get('igst', pd.Series([0])).sum()),
            "total_tax": float(df.get('total_tax', pd.Series([0])).sum()),
            "total_amount": float(df.get('total_amount', pd.Series([0])).sum()),
            "avg_gst_rate": float(df.get('gst_rate', pd.Series([0])).mean()),
            "intrastate_records": len(df[df.get('cgst', pd.Series([0])) > 0]),
            "interstate_records": len(df[df.get('igst', pd.Series([0])) > 0])
        }
    
    def validate_tax_computations(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Validate tax computations in the dataset.
        
        Args:
            df: Dataset with tax computations
            
        Returns:
            Dict with validation results
        """
        validation_results = {
            "total_records": len(df),
            "valid_computations": 0,
            "invalid_computations": 0,
            "missing_tax_data": 0
        }
        
        required_columns = ['cgst', 'sgst', 'igst', 'gst_rate', 'taxable_value']
        
        for index, row in df.iterrows():
            try:
                # Check if required columns exist and have valid data
                if any(col not in df.columns for col in required_columns):
                    validation_results["missing_tax_data"] += 1
                    continue
                
                # Create tax computation dict for validation
                computation = {
                    'cgst': float(row.get('cgst', 0)),
                    'sgst': float(row.get('sgst', 0)),
                    'igst': float(row.get('igst', 0)),
                    'gst_rate': float(row.get('gst_rate', 0)),
                    'total_tax': float(row.get('total_tax', 0))
                }
                
                # Validate using tax rules engine
                if self.tax_rules and self.tax_rules.validate_tax_computation(computation):
                    validation_results["valid_computations"] += 1
                else:
                    validation_results["invalid_computations"] += 1
                    
            except Exception:
                validation_results["invalid_computations"] += 1
        
        return validation_results
    
    def recompute_taxes_for_channel(self, 
                                   channel: str, 
                                   gstin: str, 
                                   run_id: uuid.UUID) -> TaxComputationResult:
        """
        Recompute taxes for all records of a specific channel and run.
        
        Args:
            channel: Channel name
            gstin: Company GSTIN
            run_id: Run ID
            
        Returns:
            Computation result
        """
        try:
            # Fetch existing records from Supabase
            response = self.supabase.client.table('tax_computations').select('*').eq('run_id', str(run_id)).eq('channel', channel).execute()
            
            if not response.data:
                return TaxComputationResult(
                    success=False,
                    error_message="No records found for recomputation"
                )
            
            # Convert to DataFrame for processing
            df = pd.DataFrame(response.data)
            
            # Reprocess with current tax rules
            enriched_df, result = self.process_dataset(df, channel, gstin, run_id)
            
            return result
            
        except Exception as e:
            return TaxComputationResult(
                success=False,
                error_message=f"Recomputation failed: {str(e)}"
            )
