"""
Seller Invoice Parser Agent
Parses seller invoices and credit notes from PDF/Excel files
Extracts structured data for expense processing
"""
import os
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

from ..libs.pdf_utils import parse_invoice_file
from ..libs.expense_rules import ExpenseRulesEngine
from ..libs.contracts import ProcessingResult


@dataclass
class SellerInvoiceData:
    """Structured data from parsed seller invoice."""
    invoice_no: str
    invoice_date: datetime
    gstin: Optional[str]
    channel: str
    line_items: List[Dict]
    file_path: str
    total_taxable: float = 0.0
    total_gst: float = 0.0
    total_amount: float = 0.0


class SellerInvoiceParserAgent:
    """Agent for parsing seller invoices and credit notes."""
    
    def __init__(self, supabase_client=None):
        self.supabase = supabase_client
        self.expense_rules = ExpenseRulesEngine()
        self.logger = logging.getLogger(__name__)
    
    def process_invoice_file(self, file_path: str, channel: str, run_id: uuid.UUID) -> ProcessingResult:
        """Process a single seller invoice file."""
        
        try:
            self.logger.info(f"Processing seller invoice: {file_path}")
            
            # Validate file exists
            if not os.path.exists(file_path):
                return ProcessingResult(
                    success=False,
                    error_message=f"File not found: {file_path}",
                    processed_records=0
                )
            
            # Parse the invoice file
            parsed_data = parse_invoice_file(file_path)
            
            if not parsed_data:
                return ProcessingResult(
                    success=False,
                    error_message="No data could be extracted from file",
                    processed_records=0
                )
            
            # Validate required fields
            validation_result = self._validate_parsed_data(parsed_data)
            if not validation_result[0]:
                return ProcessingResult(
                    success=False,
                    error_message=f"Validation failed: {'; '.join(validation_result[1])}",
                    processed_records=0
                )
            
            # Create structured invoice data
            invoice_data = self._create_invoice_data(parsed_data, channel, file_path)
            
            # Process line items
            processed_items = self._process_line_items(invoice_data, run_id)
            
            # Store in database
            if self.supabase:
                self._store_invoice_data(invoice_data, processed_items, run_id)
            
            self.logger.info(f"Successfully processed invoice {invoice_data.invoice_no} with {len(processed_items)} line items")
            
            return ProcessingResult(
                success=True,
                processed_records=len(processed_items),
                metadata={
                    'invoice_no': invoice_data.invoice_no,
                    'invoice_date': invoice_data.invoice_date.isoformat(),
                    'total_amount': invoice_data.total_amount,
                    'line_items': len(processed_items)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error processing invoice file {file_path}: {e}")
            return ProcessingResult(
                success=False,
                error_message=str(e),
                processed_records=0
            )
    
    def process_multiple_invoices(self, invoice_files: List[str], channel: str, run_id: uuid.UUID) -> ProcessingResult:
        """Process multiple seller invoice files."""
        
        total_processed = 0
        total_failed = 0
        processed_invoices = []
        failed_files = []
        
        for file_path in invoice_files:
            result = self.process_invoice_file(file_path, channel, run_id)
            
            if result.success:
                total_processed += result.processed_records
                processed_invoices.append({
                    'file': os.path.basename(file_path),
                    'records': result.processed_records,
                    'metadata': result.metadata
                })
            else:
                total_failed += 1
                failed_files.append({
                    'file': os.path.basename(file_path),
                    'error': result.error_message
                })
        
        success = total_failed == 0
        
        return ProcessingResult(
            success=success,
            processed_records=total_processed,
            metadata={
                'total_files': len(invoice_files),
                'processed_files': len(processed_invoices),
                'failed_files': total_failed,
                'processed_invoices': processed_invoices,
                'failed_files_list': failed_files
            },
            error_message=f"{total_failed} files failed to process" if total_failed > 0 else None
        )
    
    def _validate_parsed_data(self, parsed_data: Dict) -> Tuple[bool, List[str]]:
        """Validate parsed invoice data."""
        
        errors = []
        
        # Check required fields
        if not parsed_data.get('invoice_no'):
            errors.append("Invoice number not found")
        
        if not parsed_data.get('invoice_date'):
            errors.append("Invoice date not found")
        
        if not parsed_data.get('line_items'):
            errors.append("No line items found in invoice")
        
        # Validate line items
        line_items = parsed_data.get('line_items', [])
        for i, item in enumerate(line_items):
            if not item.get('expense_type'):
                errors.append(f"Line item {i+1}: Missing expense type")
            
            if not isinstance(item.get('taxable_value'), (int, float)) or item.get('taxable_value', 0) < 0:
                errors.append(f"Line item {i+1}: Invalid taxable value")
            
            if not isinstance(item.get('total_value'), (int, float)) or item.get('total_value', 0) < 0:
                errors.append(f"Line item {i+1}: Invalid total value")
        
        return len(errors) == 0, errors
    
    def _create_invoice_data(self, parsed_data: Dict, channel: str, file_path: str) -> SellerInvoiceData:
        """Create structured invoice data from parsed data."""
        
        # Ensure invoice_date is a datetime object
        invoice_date = parsed_data['invoice_date']
        if isinstance(invoice_date, str):
            # Try to parse string date
            for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y']:
                try:
                    invoice_date = datetime.strptime(invoice_date, fmt)
                    break
                except ValueError:
                    continue
        elif hasattr(invoice_date, 'date'):
            # Convert date to datetime
            invoice_date = datetime.combine(invoice_date, datetime.min.time())
        
        # Calculate totals
        line_items = parsed_data['line_items']
        total_taxable = sum(item.get('taxable_value', 0) for item in line_items)
        total_gst = sum(item.get('tax_amount', 0) for item in line_items)
        total_amount = sum(item.get('total_value', 0) for item in line_items)
        
        return SellerInvoiceData(
            invoice_no=parsed_data['invoice_no'],
            invoice_date=invoice_date,
            gstin=parsed_data.get('gstin'),
            channel=channel,
            line_items=line_items,
            file_path=file_path,
            total_taxable=total_taxable,
            total_gst=total_gst,
            total_amount=total_amount
        )
    
    def _process_line_items(self, invoice_data: SellerInvoiceData, run_id: uuid.UUID) -> List[Dict]:
        """Process and enrich line items with expense rules."""
        
        processed_items = []
        
        for item in invoice_data.line_items:
            # Normalize expense type
            expense_type = self.expense_rules.normalize_expense_type(item['expense_type'])
            
            # Get expense rule
            rule = self.expense_rules.get_expense_rule(invoice_data.channel, expense_type)
            
            if not rule:
                self.logger.warning(f"No expense rule found for {invoice_data.channel} - {expense_type}")
                # Use default values
                ledger_name = f"{invoice_data.channel.title()} {expense_type}"
                gst_rate = item.get('gst_rate', 0.18)
            else:
                ledger_name = rule.ledger_name
                gst_rate = rule.gst_rate
            
            # Calculate GST split
            taxable_value = item['taxable_value']
            gst_split = self.expense_rules.compute_gst_split(
                taxable_value, 
                gst_rate, 
                invoice_data.gstin or '06ABGCS4796R1ZA'  # Default GSTIN if not provided
            )
            
            # Get GST ledger names
            gst_ledgers = self.expense_rules.get_gst_ledger_names(gst_split, is_input_gst=True)
            
            processed_item = {
                'id': str(uuid.uuid4()),
                'run_id': str(run_id),
                'channel': invoice_data.channel,
                'gstin': invoice_data.gstin,
                'invoice_no': invoice_data.invoice_no,
                'invoice_date': invoice_data.invoice_date.date(),
                'expense_type': expense_type,
                'taxable_value': taxable_value,
                'gst_rate': gst_rate,
                'cgst': gst_split['cgst_amount'],
                'sgst': gst_split['sgst_amount'],
                'igst': gst_split['igst_amount'],
                'total_value': item['total_value'],
                'ledger_name': ledger_name,
                'file_path': invoice_data.file_path,
                'processing_status': 'processed',
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                # Additional fields for X2Beta export
                'cgst_ledger': gst_ledgers.get('cgst_ledger'),
                'sgst_ledger': gst_ledgers.get('sgst_ledger'),
                'igst_ledger': gst_ledgers.get('igst_ledger')
            }
            
            processed_items.append(processed_item)
        
        return processed_items
    
    def _store_invoice_data(self, invoice_data: SellerInvoiceData, processed_items: List[Dict], run_id: uuid.UUID):
        """Store invoice data in Supabase."""
        
        try:
            # Prepare records for database insertion
            db_records = []
            
            for item in processed_items:
                # Remove fields that don't exist in the database schema
                db_record = {k: v for k, v in item.items() 
                           if k not in ['cgst_ledger', 'sgst_ledger', 'igst_ledger']}
                db_records.append(db_record)
            
            # Insert into seller_invoices table
            if db_records:
                result = self.supabase.client.table("seller_invoices").insert(db_records).execute()
                self.logger.info(f"Inserted {len(db_records)} seller invoice records")
            
        except Exception as e:
            self.logger.error(f"Error storing invoice data: {e}")
            raise
    
    def get_processing_summary(self, run_id: uuid.UUID) -> Dict:
        """Get processing summary for a run."""
        
        if not self.supabase:
            return {'error': 'No database connection'}
        
        try:
            # Get all seller invoices for this run
            result = self.supabase.client.table("seller_invoices").select("*").eq("run_id", str(run_id)).execute()
            
            if not result.data:
                return {
                    'total_invoices': 0,
                    'total_line_items': 0,
                    'total_amount': 0.0,
                    'expense_breakdown': {}
                }
            
            invoices = result.data
            
            # Calculate summary
            total_line_items = len(invoices)
            total_amount = sum(inv['total_value'] for inv in invoices)
            
            # Group by invoice number to count unique invoices
            unique_invoices = set(inv['invoice_no'] for inv in invoices)
            
            # Group by expense type
            expense_breakdown = {}
            for inv in invoices:
                exp_type = inv['expense_type']
                if exp_type not in expense_breakdown:
                    expense_breakdown[exp_type] = {'count': 0, 'amount': 0.0}
                expense_breakdown[exp_type]['count'] += 1
                expense_breakdown[exp_type]['amount'] += inv['total_value']
            
            return {
                'total_invoices': len(unique_invoices),
                'total_line_items': total_line_items,
                'total_amount': total_amount,
                'expense_breakdown': expense_breakdown
            }
            
        except Exception as e:
            self.logger.error(f"Error getting processing summary: {e}")
            return {'error': str(e)}
    
    def get_invoices_for_export(self, run_id: uuid.UUID, gstin: Optional[str] = None) -> List[Dict]:
        """Get processed invoices ready for Tally export."""
        
        if not self.supabase:
            return []
        
        try:
            query = self.supabase.client.table("seller_invoices").select("*").eq("run_id", str(run_id))
            
            if gstin:
                query = query.eq("gstin", gstin)
            
            result = query.execute()
            return result.data or []
            
        except Exception as e:
            self.logger.error(f"Error getting invoices for export: {e}")
            return []
