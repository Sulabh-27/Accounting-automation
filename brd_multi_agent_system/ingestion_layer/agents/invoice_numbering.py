"""
Invoice Numbering Agent
Generates unique invoice numbers for processed transactions
"""
import pandas as pd
import uuid
from typing import Dict, List, Tuple
from datetime import datetime

from ..libs.contracts import InvoiceNumberingRequest, InvoiceNumberingResult
from ..libs.numbering_rules import NumberingRulesEngine
from ..libs.supabase_client import SupabaseClientWrapper


class InvoiceNumberingAgent:
    """
    Invoice numbering agent that generates unique invoice numbers
    based on channel-specific patterns and ensures uniqueness.
    """
    
    def __init__(self, supabase_client: SupabaseClientWrapper):
        """
        Initialize Invoice Numbering Agent.
        
        Args:
            supabase_client: Supabase client for data persistence
        """
        self.supabase = supabase_client
        self.numbering_engine = None  # Will be initialized per request
    
    def process_dataset(self, 
                       df: pd.DataFrame, 
                       channel: str, 
                       gstin: str,
                       month: str,
                       run_id: uuid.UUID) -> Tuple[pd.DataFrame, InvoiceNumberingResult]:
        """
        Process dataset and generate invoice numbers.
        
        Args:
            df: Input dataset with tax computations
            channel: Channel name (amazon_mtr, flipkart, etc.)
            gstin: Company GSTIN
            month: Month in YYYY-MM format
            run_id: Run ID for tracking
            
        Returns:
            Tuple of (enriched_dataframe, numbering_result)
        """
        try:
            # Initialize numbering engine
            self.numbering_engine = NumberingRulesEngine(gstin)
            
            # Load existing invoice numbers to avoid duplicates
            self._load_existing_invoice_numbers(channel, gstin, month)
            
            # Create a copy to avoid modifying original
            enriched_df = df.copy()
            
            # Add invoice number column
            enriched_df['invoice_no'] = ''
            
            # Generate invoice numbers
            invoice_records = []
            successful_generations = 0
            failed_generations = 0
            
            # Group by state for sequential numbering
            state_groups = enriched_df.groupby('state_code')
            
            for state_code, group in state_groups:
                try:
                    # Generate invoice numbers for this state group
                    for index, row in group.iterrows():
                        try:
                            # Generate unique invoice number
                            invoice_number = self.numbering_engine.generate_invoice_number(
                                channel=self._normalize_channel_name(channel),
                                state_name=state_code,
                                month=month
                            )
                            
                            # Update dataframe
                            enriched_df.at[index, 'invoice_no'] = invoice_number
                            
                            # Prepare record for Supabase
                            invoice_record = {
                                'run_id': str(run_id),
                                'channel': channel,
                                'gstin': gstin,
                                'state_code': state_code,
                                'invoice_no': invoice_number,
                                'month': month
                            }
                            invoice_records.append(invoice_record)
                            successful_generations += 1
                            
                        except Exception as e:
                            print(f"  ⚠️  Invoice generation failed for row {index}: {e}")
                            failed_generations += 1
                            continue
                            
                except Exception as e:
                    print(f"  ⚠️  Invoice generation failed for state {state_code}: {e}")
                    failed_generations += len(group)
                    continue
            
            # Store invoice registry in Supabase
            if invoice_records:
                self._store_invoice_registry(invoice_records)
            
            # Create result summary
            result = InvoiceNumberingResult(
                success=True,
                processed_records=len(df),
                successful_generations=successful_generations,
                failed_generations=failed_generations,
                unique_invoice_numbers=len(set(enriched_df['invoice_no'].tolist())),
                duplicate_numbers=successful_generations - len(set(enriched_df['invoice_no'].tolist()))
            )
            
            return enriched_df, result
            
        except Exception as e:
            result = InvoiceNumberingResult(
                success=False,
                processed_records=len(df),
                successful_generations=0,
                failed_generations=len(df),
                error_message=str(e)
            )
            return df, result
    
    def _normalize_channel_name(self, channel: str) -> str:
        """
        Normalize channel name for numbering engine.
        
        Args:
            channel: Original channel name
            
        Returns:
            Normalized channel name
        """
        # Map channel names to numbering engine patterns
        channel_mapping = {
            'amazon': 'amazon_mtr',
            'amazon_mtr': 'amazon_mtr',
            'amazon_str': 'amazon_str',
            'flipkart': 'flipkart',
            'pepperfry': 'pepperfry'
        }
        
        return channel_mapping.get(channel.lower(), 'amazon_mtr')
    
    def _load_existing_invoice_numbers(self, channel: str, gstin: str, month: str):
        """
        Load existing invoice numbers from Supabase to avoid duplicates.
        
        Args:
            channel: Channel name
            gstin: Company GSTIN
            month: Month string
        """
        try:
            # Query existing invoice numbers for this channel/gstin/month
            response = self.supabase.client.table('invoice_registry').select('invoice_no').eq('channel', channel).eq('gstin', gstin).eq('month', month).execute()
            
            if response.data:
                existing_numbers = [record['invoice_no'] for record in response.data]
                self.numbering_engine.register_existing_numbers(existing_numbers)
                print(f"  📋 Loaded {len(existing_numbers)} existing invoice numbers")
            
        except Exception as e:
            print(f"  ⚠️  Failed to load existing invoice numbers: {e}")
    
    def _store_invoice_registry(self, invoice_records: List[Dict]):
        """
        Store invoice registry records in Supabase.
        
        Args:
            invoice_records: List of invoice registry records
        """
        try:
            # Batch insert invoice registry
            response = self.supabase.client.table('invoice_registry').insert(invoice_records).execute()
            print(f"  💾 Stored {len(invoice_records)} invoice registry records")
        except Exception as e:
            print(f"  ⚠️  Failed to store invoice registry: {e}")
    
    def get_numbering_summary(self, df: pd.DataFrame, channel: str) -> Dict[str, any]:
        """
        Generate invoice numbering summary statistics.
        
        Args:
            df: Dataset with invoice numbers
            channel: Channel name
            
        Returns:
            Dict with summary statistics
        """
        if df.empty or 'invoice_no' not in df.columns:
            return {
                "total_records": 0,
                "generated_invoices": 0,
                "unique_invoices": 0,
                "duplicate_invoices": 0,
                "pattern_example": self.numbering_engine.get_pattern_example(self._normalize_channel_name(channel)) if self.numbering_engine else "N/A"
            }
        
        invoice_numbers = df['invoice_no'].dropna().tolist()
        unique_invoices = set(invoice_numbers)
        
        return {
            "total_records": len(df),
            "generated_invoices": len(invoice_numbers),
            "unique_invoices": len(unique_invoices),
            "duplicate_invoices": len(invoice_numbers) - len(unique_invoices),
            "pattern_example": self.numbering_engine.get_pattern_example(self._normalize_channel_name(channel)) if self.numbering_engine else "N/A",
            "states_covered": len(df['state_code'].unique()) if 'state_code' in df.columns else 0,
            "sample_invoices": list(unique_invoices)[:5]  # First 5 samples
        }
    
    def validate_invoice_numbers(self, df: pd.DataFrame, channel: str) -> Dict[str, int]:
        """
        Validate invoice numbers in the dataset.
        
        Args:
            df: Dataset with invoice numbers
            channel: Channel name
            
        Returns:
            Dict with validation results
        """
        validation_results = {
            "total_records": len(df),
            "valid_numbers": 0,
            "invalid_numbers": 0,
            "missing_numbers": 0,
            "duplicate_numbers": 0
        }
        
        if 'invoice_no' not in df.columns:
            validation_results["missing_numbers"] = len(df)
            return validation_results
        
        invoice_numbers = df['invoice_no'].tolist()
        seen_numbers = set()
        normalized_channel = self._normalize_channel_name(channel)
        
        for invoice_no in invoice_numbers:
            if pd.isna(invoice_no) or invoice_no == '':
                validation_results["missing_numbers"] += 1
            elif invoice_no in seen_numbers:
                validation_results["duplicate_numbers"] += 1
            elif self.numbering_engine and self.numbering_engine.validate_invoice_number(invoice_no, normalized_channel):
                validation_results["valid_numbers"] += 1
                seen_numbers.add(invoice_no)
            else:
                validation_results["invalid_numbers"] += 1
                seen_numbers.add(invoice_no)
        
        return validation_results
    
    def regenerate_invoice_numbers(self, 
                                  df: pd.DataFrame, 
                                  channel: str, 
                                  gstin: str,
                                  month: str,
                                  run_id: uuid.UUID,
                                  force: bool = False) -> Tuple[pd.DataFrame, InvoiceNumberingResult]:
        """
        Regenerate invoice numbers for the dataset.
        
        Args:
            df: Dataset to regenerate numbers for
            channel: Channel name
            gstin: Company GSTIN
            month: Month string
            run_id: Run ID
            force: Whether to force regeneration even if numbers exist
            
        Returns:
            Tuple of (enriched_dataframe, numbering_result)
        """
        try:
            # Clear existing numbers if force is True
            if force and 'invoice_no' in df.columns:
                df = df.copy()
                df['invoice_no'] = ''
            
            # Check if numbers already exist
            if not force and 'invoice_no' in df.columns and not df['invoice_no'].isna().all():
                existing_count = len(df[df['invoice_no'].notna() & (df['invoice_no'] != '')])
                if existing_count > 0:
                    result = InvoiceNumberingResult(
                        success=True,
                        processed_records=len(df),
                        successful_generations=existing_count,
                        failed_generations=0,
                        unique_invoice_numbers=len(df['invoice_no'].unique()),
                        message=f"Using existing invoice numbers ({existing_count} found)"
                    )
                    return df, result
            
            # Generate new numbers
            return self.process_dataset(df, channel, gstin, month, run_id)
            
        except Exception as e:
            result = InvoiceNumberingResult(
                success=False,
                processed_records=len(df),
                error_message=f"Regeneration failed: {str(e)}"
            )
            return df, result
    
    def get_invoice_number_analytics(self, channel: str, gstin: str, month: str) -> Dict[str, any]:
        """
        Get analytics for invoice numbers in a specific period.
        
        Args:
            channel: Channel name
            gstin: Company GSTIN
            month: Month string
            
        Returns:
            Dict with analytics data
        """
        try:
            # Query invoice registry
            response = self.supabase.client.table('invoice_registry').select('*').eq('channel', channel).eq('gstin', gstin).eq('month', month).execute()
            
            if not response.data:
                return {"message": "No invoice data found for the specified period"}
            
            df = pd.DataFrame(response.data)
            
            # Generate analytics
            analytics = {
                "total_invoices": len(df),
                "unique_states": len(df['state_code'].unique()),
                "states_breakdown": df['state_code'].value_counts().to_dict(),
                "date_range": {
                    "earliest": df['created_at'].min() if 'created_at' in df.columns else None,
                    "latest": df['created_at'].max() if 'created_at' in df.columns else None
                },
                "sample_numbers": df['invoice_no'].head(10).tolist()
            }
            
            return analytics
            
        except Exception as e:
            return {"error": f"Analytics generation failed: {str(e)}"}
    
    def check_invoice_uniqueness(self, invoice_numbers: List[str]) -> Dict[str, List[str]]:
        """
        Check uniqueness of invoice numbers against existing registry.
        
        Args:
            invoice_numbers: List of invoice numbers to check
            
        Returns:
            Dict with unique and duplicate numbers
        """
        try:
            # Query existing numbers
            response = self.supabase.client.table('invoice_registry').select('invoice_no').in_('invoice_no', invoice_numbers).execute()
            
            existing_numbers = set([record['invoice_no'] for record in response.data])
            input_numbers = set(invoice_numbers)
            
            return {
                "unique_numbers": list(input_numbers - existing_numbers),
                "duplicate_numbers": list(input_numbers & existing_numbers),
                "total_checked": len(invoice_numbers),
                "unique_count": len(input_numbers - existing_numbers),
                "duplicate_count": len(input_numbers & existing_numbers)
            }
            
        except Exception as e:
            return {"error": f"Uniqueness check failed: {str(e)}"}
