"""
Exception Handler Agent for Part 7: Exception Handling & Approval Workflows
Detects, categorizes, and manages exceptions throughout the pipeline
"""
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging
import re

from ..libs.error_codes import ErrorCodes, create_exception_record, get_error_definition
from ..libs.notification_utils import notify_exception
from ..libs.supabase_client import SupabaseClientWrapper


@dataclass
class ExceptionResult:
    """Result of exception detection and handling."""
    total_records: int
    exceptions_detected: int
    critical_exceptions: int
    warnings: int
    auto_resolved: int
    requires_approval: int
    processing_successful: bool
    exception_summary: Dict[str, int]


class ExceptionHandler:
    """Handles exception detection and management across all pipeline stages."""
    
    def __init__(self, supabase_client: Optional[SupabaseClientWrapper] = None):
        self.supabase = supabase_client
        self.logger = logging.getLogger(__name__)
        self.exceptions: List[Dict[str, Any]] = []
    
    def detect_mapping_exceptions(
        self,
        df: pd.DataFrame,
        run_id: uuid.UUID,
        record_type: str = "sales"
    ) -> ExceptionResult:
        """Detect mapping-related exceptions in dataset."""
        
        exceptions_found = []
        total_records = len(df)
        
        # Check for missing SKU mappings
        if 'sku' in df.columns and 'final_goods_name' in df.columns:
            missing_sku_mask = (
                df['sku'].notna() & 
                (df['final_goods_name'].isna() | (df['final_goods_name'] == ''))
            )
            
            for idx, row in df[missing_sku_mask].iterrows():
                exception = create_exception_record(
                    error_code="MAP-001",
                    record_type=record_type,
                    record_id=str(row.get('sku', 'unknown')),
                    error_details={
                        'sku': str(row.get('sku', '')),
                        'asin': str(row.get('asin', '')),
                        'channel': str(row.get('channel', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check for missing ASIN mappings
        if 'asin' in df.columns and 'final_goods_name' in df.columns:
            missing_asin_mask = (
                df['asin'].notna() & 
                df['sku'].isna() &
                (df['final_goods_name'].isna() | (df['final_goods_name'] == ''))
            )
            
            for idx, row in df[missing_asin_mask].iterrows():
                exception = create_exception_record(
                    error_code="MAP-002",
                    record_type=record_type,
                    record_id=str(row.get('asin', 'unknown')),
                    error_details={
                        'asin': str(row.get('asin', '')),
                        'channel': str(row.get('channel', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check for missing ledger mappings
        if 'ledger_name' in df.columns:
            missing_ledger_mask = (
                df['ledger_name'].isna() | (df['ledger_name'] == '')
            )
            
            for idx, row in df[missing_ledger_mask].iterrows():
                exception = create_exception_record(
                    error_code="LED-001",
                    record_type=record_type,
                    record_id=f"{row.get('channel', 'unknown')}_{row.get('state_code', 'unknown')}",
                    error_details={
                        'channel': str(row.get('channel', '')),
                        'state_code': str(row.get('state_code', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check for invalid state codes
        if 'state_code' in df.columns:
            valid_state_codes = {
                'AP', 'AR', 'AS', 'BR', 'CG', 'GA', 'GJ', 'HR', 'HP', 'JH', 'KA', 'KL',
                'MP', 'MH', 'MN', 'ML', 'MZ', 'NL', 'OR', 'PB', 'RJ', 'SK', 'TN', 'TS',
                'TR', 'UP', 'UK', 'WB', 'AN', 'CH', 'DH', 'DL', 'JK', 'LA', 'LD', 'PY'
            }
            
            invalid_state_mask = ~df['state_code'].isin(valid_state_codes)
            
            for idx, row in df[invalid_state_mask].iterrows():
                exception = create_exception_record(
                    error_code="LED-002",
                    record_type=record_type,
                    record_id=str(row.get('state_code', 'unknown')),
                    error_details={
                        'state_code': str(row.get('state_code', '')),
                        'channel': str(row.get('channel', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        return self._process_exceptions(exceptions_found, total_records)
    
    def detect_gst_exceptions(
        self,
        df: pd.DataFrame,
        run_id: uuid.UUID,
        record_type: str = "sales"
    ) -> ExceptionResult:
        """Detect GST-related exceptions in dataset."""
        
        exceptions_found = []
        total_records = len(df)
        
        # Check for invalid GST rates
        if 'gst_rate' in df.columns:
            valid_gst_rates = {0.0, 0.05, 0.12, 0.18, 0.28}
            invalid_gst_mask = ~df['gst_rate'].isin(valid_gst_rates)
            
            for idx, row in df[invalid_gst_mask].iterrows():
                exception = create_exception_record(
                    error_code="GST-001",
                    record_type=record_type,
                    record_id=f"rate_{row.get('gst_rate', 'unknown')}",
                    error_details={
                        'gst_rate': float(row.get('gst_rate', 0)),
                        'sku': str(row.get('sku', '')),
                        'final_goods_name': str(row.get('final_goods_name', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check for missing GST rates on taxable transactions
        if 'gst_rate' in df.columns and 'taxable_value' in df.columns:
            missing_gst_mask = (
                (df['taxable_value'] > 0) & 
                (df['gst_rate'].isna())
            )
            
            for idx, row in df[missing_gst_mask].iterrows():
                exception = create_exception_record(
                    error_code="GST-003",
                    record_type=record_type,
                    record_id=f"txn_{idx}",
                    error_details={
                        'taxable_value': float(row.get('taxable_value', 0)),
                        'sku': str(row.get('sku', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check GST calculation accuracy
        if all(col in df.columns for col in ['taxable_value', 'gst_rate', 'total_tax']):
            df_calc = df.copy()
            df_calc['expected_tax'] = df_calc['taxable_value'] * df_calc['gst_rate']
            df_calc['tax_diff'] = abs(df_calc['total_tax'] - df_calc['expected_tax'])
            
            # Allow small rounding differences (up to 0.01)
            tax_mismatch_mask = df_calc['tax_diff'] > 0.01
            
            for idx, row in df_calc[tax_mismatch_mask].iterrows():
                exception = create_exception_record(
                    error_code="GST-002",
                    record_type=record_type,
                    record_id=f"calc_{idx}",
                    error_details={
                        'taxable_value': float(row.get('taxable_value', 0)),
                        'gst_rate': float(row.get('gst_rate', 0)),
                        'computed_tax': float(row.get('total_tax', 0)),
                        'expected_tax': float(row.get('expected_tax', 0)),
                        'difference': float(row.get('tax_diff', 0)),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        return self._process_exceptions(exceptions_found, total_records)
    
    def detect_invoice_exceptions(
        self,
        df: pd.DataFrame,
        run_id: uuid.UUID,
        record_type: str = "sales"
    ) -> ExceptionResult:
        """Detect invoice-related exceptions in dataset."""
        
        exceptions_found = []
        total_records = len(df)
        
        # Check for duplicate invoice numbers
        if 'invoice_no' in df.columns:
            duplicate_invoices = df[df['invoice_no'].duplicated(keep=False)]
            
            for idx, row in duplicate_invoices.iterrows():
                exception = create_exception_record(
                    error_code="INV-001",
                    record_type=record_type,
                    record_id=str(row.get('invoice_no', 'unknown')),
                    error_details={
                        'invoice_no': str(row.get('invoice_no', '')),
                        'duplicate_count': int(df[df['invoice_no'] == row['invoice_no']].shape[0]),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check invoice number format
        if 'invoice_no' in df.columns and 'channel' in df.columns:
            for idx, row in df.iterrows():
                invoice_no = str(row.get('invoice_no', ''))
                channel = str(row.get('channel', ''))
                
                # Expected patterns by channel
                patterns = {
                    'amazon': r'^AMZ[A-Z]{2}\d{9}$',
                    'flipkart': r'^FK[A-Z]{2}\d{9}$',
                    'pepperfry': r'^PP[A-Z]{2}\d{9}$'
                }
                
                if channel in patterns:
                    if not re.match(patterns[channel], invoice_no):
                        exception = create_exception_record(
                            error_code="INV-002",
                            record_type=record_type,
                            record_id=invoice_no,
                            error_details={
                                'invoice_no': invoice_no,
                                'channel': channel,
                                'expected_pattern': patterns[channel],
                                'row_index': int(idx)
                            }
                        )
                        exception['run_id'] = str(run_id)
                        exceptions_found.append(exception)
        
        # Check invoice dates
        if 'invoice_date' in df.columns:
            current_date = datetime.now().date()
            
            for idx, row in df.iterrows():
                try:
                    if pd.notna(row['invoice_date']):
                        invoice_date = pd.to_datetime(row['invoice_date']).date()
                        
                        # Check if date is too far in future (more than 1 day)
                        if (invoice_date - current_date).days > 1:
                            exception = create_exception_record(
                                error_code="INV-003",
                                record_type=record_type,
                                record_id=f"date_{invoice_date}",
                                error_details={
                                    'invoice_date': str(invoice_date),
                                    'current_date': str(current_date),
                                    'days_difference': (invoice_date - current_date).days,
                                    'row_index': int(idx)
                                }
                            )
                            exception['run_id'] = str(run_id)
                            exceptions_found.append(exception)
                except (ValueError, TypeError):
                    exception = create_exception_record(
                        error_code="INV-003",
                        record_type=record_type,
                        record_id=f"invalid_date_{idx}",
                        error_details={
                            'invoice_date': str(row.get('invoice_date', '')),
                            'error': 'Invalid date format',
                            'row_index': int(idx)
                        }
                    )
                    exception['run_id'] = str(run_id)
                    exceptions_found.append(exception)
        
        return self._process_exceptions(exceptions_found, total_records)
    
    def detect_data_quality_exceptions(
        self,
        df: pd.DataFrame,
        run_id: uuid.UUID,
        record_type: str = "sales"
    ) -> ExceptionResult:
        """Detect data quality exceptions in dataset."""
        
        exceptions_found = []
        total_records = len(df)
        
        # Check for negative amounts
        amount_columns = ['taxable_value', 'total_tax', 'total_amount']
        for col in amount_columns:
            if col in df.columns:
                negative_mask = df[col] < 0
                
                for idx, row in df[negative_mask].iterrows():
                    exception = create_exception_record(
                        error_code="DAT-001",
                        record_type=record_type,
                        record_id=f"{col}_{idx}",
                        error_details={
                            'column': col,
                            'value': float(row.get(col, 0)),
                            'sku': str(row.get('sku', '')),
                            'row_index': int(idx)
                        }
                    )
                    exception['run_id'] = str(run_id)
                    exceptions_found.append(exception)
        
        # Check for zero or negative quantities
        if 'quantity' in df.columns:
            invalid_qty_mask = df['quantity'] <= 0
            
            for idx, row in df[invalid_qty_mask].iterrows():
                exception = create_exception_record(
                    error_code="DAT-002",
                    record_type=record_type,
                    record_id=f"qty_{idx}",
                    error_details={
                        'quantity': float(row.get('quantity', 0)),
                        'sku': str(row.get('sku', '')),
                        'row_index': int(idx)
                    }
                )
                exception['run_id'] = str(run_id)
                exceptions_found.append(exception)
        
        # Check for missing required data
        required_columns = ['invoice_date', 'taxable_value', 'channel', 'gstin']
        for col in required_columns:
            if col in df.columns:
                missing_mask = df[col].isna() | (df[col] == '')
                
                for idx, row in df[missing_mask].iterrows():
                    exception = create_exception_record(
                        error_code="DAT-003",
                        record_type=record_type,
                        record_id=f"{col}_{idx}",
                        error_details={
                            'missing_column': col,
                            'row_index': int(idx)
                        }
                    )
                    exception['run_id'] = str(run_id)
                    exceptions_found.append(exception)
        
        return self._process_exceptions(exceptions_found, total_records)
    
    def detect_schema_exceptions(
        self,
        df: pd.DataFrame,
        run_id: uuid.UUID,
        expected_columns: List[str],
        record_type: str = "sales"
    ) -> ExceptionResult:
        """Detect schema-related exceptions in dataset."""
        
        exceptions_found = []
        total_records = len(df)
        
        # Check for missing required columns
        missing_columns = set(expected_columns) - set(df.columns)
        for col in missing_columns:
            exception = create_exception_record(
                error_code="SCH-001",
                record_type=record_type,
                record_id=col,
                error_details={
                    'missing_column': col,
                    'available_columns': list(df.columns),
                    'expected_columns': expected_columns
                }
            )
            exception['run_id'] = str(run_id)
            exceptions_found.append(exception)
        
        # Check data types for numeric columns
        numeric_columns = ['taxable_value', 'gst_rate', 'quantity', 'total_tax', 'total_amount']
        for col in numeric_columns:
            if col in df.columns:
                non_numeric_mask = pd.to_numeric(df[col], errors='coerce').isna() & df[col].notna()
                
                for idx, row in df[non_numeric_mask].iterrows():
                    exception = create_exception_record(
                        error_code="SCH-002",
                        record_type=record_type,
                        record_id=f"{col}_{idx}",
                        error_details={
                            'column': col,
                            'value': str(row.get(col, '')),
                            'expected_type': 'numeric',
                            'row_index': int(idx)
                        }
                    )
                    exception['run_id'] = str(run_id)
                    exceptions_found.append(exception)
        
        return self._process_exceptions(exceptions_found, total_records)
    
    def _process_exceptions(
        self,
        exceptions: List[Dict[str, Any]],
        total_records: int
    ) -> ExceptionResult:
        """Process and categorize detected exceptions."""
        
        if not exceptions:
            return ExceptionResult(
                total_records=total_records,
                exceptions_detected=0,
                critical_exceptions=0,
                warnings=0,
                auto_resolved=0,
                requires_approval=0,
                processing_successful=True,
                exception_summary={}
            )
        
        # Store exceptions for later database insertion
        self.exceptions.extend(exceptions)
        
        # Categorize exceptions
        critical_count = sum(1 for e in exceptions if e['severity'] == 'critical')
        warning_count = sum(1 for e in exceptions if e['severity'] == 'warning')
        error_count = sum(1 for e in exceptions if e['severity'] == 'error')
        
        # Count auto-resolvable and approval-required exceptions
        auto_resolved = 0
        requires_approval = 0
        
        for exception in exceptions:
            error_def = get_error_definition(exception['error_code'])
            if error_def:
                if error_def.auto_resolve:
                    auto_resolved += 1
                if error_def.requires_approval:
                    requires_approval += 1
        
        # Create summary by error code
        exception_summary = {}
        for exception in exceptions:
            code = exception['error_code']
            exception_summary[code] = exception_summary.get(code, 0) + 1
        
        # Send notifications for critical exceptions
        for exception in exceptions:
            if exception['severity'] in ['critical', 'error']:
                notify_exception(
                    error_code=exception['error_code'],
                    error_message=exception['error_message'],
                    record_type=exception['record_type'],
                    record_id=exception.get('record_id'),
                    severity=exception['severity'],
                    details=exception.get('error_details')
                )
        
        # Determine if processing should continue
        processing_successful = critical_count == 0
        
        return ExceptionResult(
            total_records=total_records,
            exceptions_detected=len(exceptions),
            critical_exceptions=critical_count,
            warnings=warning_count,
            auto_resolved=auto_resolved,
            requires_approval=requires_approval,
            processing_successful=processing_successful,
            exception_summary=exception_summary
        )
    
    def save_exceptions_to_database(self) -> bool:
        """Save all detected exceptions to database."""
        
        if not self.exceptions or not self.supabase:
            return True
        
        try:
            # Insert exceptions in batches
            batch_size = 100
            for i in range(0, len(self.exceptions), batch_size):
                batch = self.exceptions[i:i + batch_size]
                
                # Prepare data for insertion
                insert_data = []
                for exception in batch:
                    insert_data.append({
                        'run_id': exception['run_id'],
                        'record_type': exception['record_type'],
                        'record_id': exception.get('record_id'),
                        'error_code': exception['error_code'],
                        'error_message': exception['error_message'],
                        'error_details': exception.get('error_details', {}),
                        'severity': exception['severity']
                    })
                
                # Insert batch
                result = self.supabase.client.table('exceptions').insert(insert_data).execute()
                
                if not result.data:
                    self.logger.error(f"Failed to insert exception batch {i//batch_size + 1}")
                    return False
            
            self.logger.info(f"Successfully saved {len(self.exceptions)} exceptions to database")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving exceptions to database: {e}")
            return False
    
    def get_exception_summary(self) -> Dict[str, Any]:
        """Get summary of all detected exceptions."""
        
        if not self.exceptions:
            return {
                'total_exceptions': 0,
                'by_severity': {},
                'by_category': {},
                'by_error_code': {}
            }
        
        # Group by severity
        by_severity = {}
        for exception in self.exceptions:
            severity = exception['severity']
            by_severity[severity] = by_severity.get(severity, 0) + 1
        
        # Group by category
        by_category = {}
        for exception in self.exceptions:
            category = exception['error_code'].split('-')[0]
            by_category[category] = by_category.get(category, 0) + 1
        
        # Group by error code
        by_error_code = {}
        for exception in self.exceptions:
            code = exception['error_code']
            by_error_code[code] = by_error_code.get(code, 0) + 1
        
        return {
            'total_exceptions': len(self.exceptions),
            'by_severity': by_severity,
            'by_category': by_category,
            'by_error_code': by_error_code
        }
    
    def clear_exceptions(self):
        """Clear stored exceptions."""
        self.exceptions.clear()
