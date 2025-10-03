"""
Expense Mapper Agent
Maps parsed seller invoices to appropriate Tally ledger accounts
Handles GST computation and ledger mapping for expense transactions
"""
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

from ..libs.expense_rules import ExpenseRulesEngine, ExpenseRule
from ..libs.contracts import ProcessingResult


@dataclass
class MappedExpense:
    """Mapped expense ready for Tally export."""
    id: str
    run_id: str
    channel: str
    gstin: str
    invoice_no: str
    invoice_date: datetime
    expense_type: str
    ledger_name: str
    taxable_value: float
    gst_rate: float
    cgst_amount: float
    sgst_amount: float
    igst_amount: float
    total_value: float
    cgst_ledger: Optional[str] = None
    sgst_ledger: Optional[str] = None
    igst_ledger: Optional[str] = None
    voucher_no: Optional[str] = None
    voucher_type: str = "Purchase"


class ExpenseMapperAgent:
    """Agent for mapping expenses to Tally ledger accounts."""
    
    def __init__(self, supabase_client=None):
        self.supabase = supabase_client
        self.expense_rules = ExpenseRulesEngine()
        self.logger = logging.getLogger(__name__)
    
    def process_parsed_invoices(self, run_id: uuid.UUID, gstin: str) -> ProcessingResult:
        """Process parsed seller invoices and map to ledger accounts."""
        
        try:
            self.logger.info(f"Processing parsed invoices for mapping - Run: {run_id}, GSTIN: {gstin}")
            
            # Get parsed invoices from database
            parsed_invoices = self._get_parsed_invoices(run_id, gstin)
            
            if not parsed_invoices:
                return ProcessingResult(
                    success=True,
                    processed_records=0,
                    metadata={'message': 'No parsed invoices found for mapping'}
                )
            
            # Map expenses to ledger accounts
            mapped_expenses = []
            mapping_errors = []
            
            for invoice in parsed_invoices:
                try:
                    mapped_expense = self._map_invoice_to_ledger(invoice, gstin)
                    mapped_expenses.append(mapped_expense)
                except Exception as e:
                    mapping_errors.append({
                        'invoice_no': invoice.get('invoice_no', 'Unknown'),
                        'error': str(e)
                    })
                    self.logger.error(f"Error mapping invoice {invoice.get('invoice_no')}: {e}")
            
            # Update database with mapping results
            if mapped_expenses:
                self._update_mapping_status(mapped_expenses)
            
            # Generate summary
            summary = self._generate_mapping_summary(mapped_expenses)
            
            success = len(mapping_errors) == 0
            
            return ProcessingResult(
                success=success,
                processed_records=len(mapped_expenses),
                metadata={
                    'mapped_expenses': len(mapped_expenses),
                    'mapping_errors': len(mapping_errors),
                    'error_details': mapping_errors,
                    'summary': summary
                },
                error_message=f"{len(mapping_errors)} mapping errors occurred" if mapping_errors else None
            )
            
        except Exception as e:
            self.logger.error(f"Error in expense mapping process: {e}")
            return ProcessingResult(
                success=False,
                error_message=str(e),
                processed_records=0
            )
    
    def _get_parsed_invoices(self, run_id: uuid.UUID, gstin: str) -> List[Dict]:
        """Get parsed invoices from database."""
        
        if not self.supabase:
            # Return mock data for testing
            return self._get_mock_parsed_invoices()
        
        try:
            result = self.supabase.client.table("seller_invoices").select("*").eq("run_id", str(run_id)).eq("gstin", gstin).execute()
            return result.data or []
        except Exception as e:
            self.logger.error(f"Error fetching parsed invoices: {e}")
            return []
    
    def _get_mock_parsed_invoices(self) -> List[Dict]:
        """Get mock parsed invoices for testing."""
        
        return [
            {
                'id': str(uuid.uuid4()),
                'run_id': str(uuid.uuid4()),
                'channel': 'amazon',
                'gstin': '06ABGCS4796R1ZA',
                'invoice_no': 'AMZ-FEE-001',
                'invoice_date': datetime(2025, 8, 20).date(),
                'expense_type': 'Closing Fee',
                'taxable_value': 1000.0,
                'gst_rate': 0.18,
                'cgst': 90.0,
                'sgst': 90.0,
                'igst': 0.0,
                'total_value': 1180.0,
                'processing_status': 'processed'
            },
            {
                'id': str(uuid.uuid4()),
                'run_id': str(uuid.uuid4()),
                'channel': 'amazon',
                'gstin': '06ABGCS4796R1ZA',
                'invoice_no': 'AMZ-FEE-001',
                'invoice_date': datetime(2025, 8, 20).date(),
                'expense_type': 'Shipping Fee',
                'taxable_value': 2000.0,
                'gst_rate': 0.18,
                'cgst': 180.0,
                'sgst': 180.0,
                'igst': 0.0,
                'total_value': 2360.0,
                'processing_status': 'processed'
            }
        ]
    
    def _map_invoice_to_ledger(self, invoice: Dict, gstin: str) -> MappedExpense:
        """Map a single invoice to ledger accounts."""
        
        # Get expense rule for this expense type
        rule = self.expense_rules.get_expense_rule(invoice['channel'], invoice['expense_type'])
        
        if not rule:
            # Create default mapping
            ledger_name = f"{invoice['channel'].title()} {invoice['expense_type']}"
            self.logger.warning(f"No rule found for {invoice['channel']} - {invoice['expense_type']}, using default: {ledger_name}")
        else:
            ledger_name = rule.ledger_name
        
        # Compute GST split
        gst_split = self.expense_rules.compute_gst_split(
            invoice['taxable_value'],
            invoice['gst_rate'],
            gstin
        )
        
        # Get GST ledger names
        gst_ledgers = self.expense_rules.get_gst_ledger_names(gst_split, is_input_gst=True)
        
        # Generate voucher number
        voucher_no = self._generate_voucher_number(invoice, gstin)
        
        return MappedExpense(
            id=invoice['id'],
            run_id=invoice['run_id'],
            channel=invoice['channel'],
            gstin=gstin,
            invoice_no=invoice['invoice_no'],
            invoice_date=invoice['invoice_date'],
            expense_type=invoice['expense_type'],
            ledger_name=ledger_name,
            taxable_value=invoice['taxable_value'],
            gst_rate=invoice['gst_rate'],
            cgst_amount=gst_split['cgst_amount'],
            sgst_amount=gst_split['sgst_amount'],
            igst_amount=gst_split['igst_amount'],
            total_value=invoice['total_value'],
            cgst_ledger=gst_ledgers.get('cgst_ledger'),
            sgst_ledger=gst_ledgers.get('sgst_ledger'),
            igst_ledger=gst_ledgers.get('igst_ledger'),
            voucher_no=voucher_no,
            voucher_type="Purchase"
        )
    
    def _generate_voucher_number(self, invoice: Dict, gstin: str) -> str:
        """Generate voucher number for expense entry."""
        
        # Extract state code from GSTIN
        state_code = gstin[:2] if gstin else "00"
        
        # Get month from invoice date
        if isinstance(invoice['invoice_date'], str):
            invoice_date = datetime.strptime(invoice['invoice_date'], '%Y-%m-%d').date()
        else:
            invoice_date = invoice['invoice_date']
        
        month_str = invoice_date.strftime('%m')
        year_str = invoice_date.strftime('%y')
        
        # Create voucher number: EXP{state_code}{year}{month}{sequence}
        # For now, use invoice number as sequence identifier
        invoice_seq = invoice['invoice_no'].replace('-', '').replace('_', '')[-4:] if len(invoice['invoice_no']) >= 4 else '0001'
        
        voucher_no = f"EXP{state_code}{year_str}{month_str}{invoice_seq}"
        
        return voucher_no
    
    def _update_mapping_status(self, mapped_expenses: List[MappedExpense]):
        """Update database with mapping results."""
        
        if not self.supabase:
            self.logger.info(f"Mock: Would update {len(mapped_expenses)} expense mappings in database")
            return
        
        try:
            # Update seller_invoices table with mapping status
            for expense in mapped_expenses:
                update_data = {
                    'ledger_name': expense.ledger_name,
                    'processing_status': 'mapped',
                    'updated_at': datetime.now().isoformat()
                }
                
                self.supabase.client.table("seller_invoices").update(update_data).eq("id", expense.id).execute()
            
            self.logger.info(f"Updated {len(mapped_expenses)} expense mappings in database")
            
        except Exception as e:
            self.logger.error(f"Error updating mapping status: {e}")
            raise
    
    def _generate_mapping_summary(self, mapped_expenses: List[MappedExpense]) -> Dict:
        """Generate summary of mapping results."""
        
        if not mapped_expenses:
            return {
                'total_expenses': 0,
                'total_amount': 0.0,
                'expense_types': {},
                'ledger_mapping': {},
                'gst_summary': {}
            }
        
        total_amount = sum(exp.total_value for exp in mapped_expenses)
        
        # Group by expense type
        expense_types = {}
        for exp in mapped_expenses:
            if exp.expense_type not in expense_types:
                expense_types[exp.expense_type] = {'count': 0, 'amount': 0.0}
            expense_types[exp.expense_type]['count'] += 1
            expense_types[exp.expense_type]['amount'] += exp.total_value
        
        # Group by ledger
        ledger_mapping = {}
        for exp in mapped_expenses:
            if exp.ledger_name not in ledger_mapping:
                ledger_mapping[exp.ledger_name] = {'count': 0, 'amount': 0.0}
            ledger_mapping[exp.ledger_name]['count'] += 1
            ledger_mapping[exp.ledger_name]['amount'] += exp.total_value
        
        # GST summary
        total_cgst = sum(exp.cgst_amount for exp in mapped_expenses)
        total_sgst = sum(exp.sgst_amount for exp in mapped_expenses)
        total_igst = sum(exp.igst_amount for exp in mapped_expenses)
        
        gst_summary = {
            'total_cgst': total_cgst,
            'total_sgst': total_sgst,
            'total_igst': total_igst,
            'total_gst': total_cgst + total_sgst + total_igst,
            'intrastate_transactions': sum(1 for exp in mapped_expenses if exp.cgst_amount > 0),
            'interstate_transactions': sum(1 for exp in mapped_expenses if exp.igst_amount > 0)
        }
        
        return {
            'total_expenses': len(mapped_expenses),
            'total_amount': total_amount,
            'expense_types': expense_types,
            'ledger_mapping': ledger_mapping,
            'gst_summary': gst_summary
        }
    
    def get_mapped_expenses_for_export(self, run_id: uuid.UUID, gstin: str) -> List[MappedExpense]:
        """Get mapped expenses ready for Tally export."""
        
        try:
            # Get mapped invoices from database
            if self.supabase:
                result = self.supabase.client.table("seller_invoices").select("*").eq("run_id", str(run_id)).eq("gstin", gstin).eq("processing_status", "mapped").execute()
                invoices = result.data or []
            else:
                # Use mock data
                invoices = self._get_mock_parsed_invoices()
            
            # Convert to MappedExpense objects
            mapped_expenses = []
            for invoice in invoices:
                # Re-compute mapping to ensure consistency
                mapped_expense = self._map_invoice_to_ledger(invoice, gstin)
                mapped_expenses.append(mapped_expense)
            
            return mapped_expenses
            
        except Exception as e:
            self.logger.error(f"Error getting mapped expenses for export: {e}")
            return []
    
    def validate_expense_mapping(self, run_id: uuid.UUID, gstin: str) -> Tuple[bool, List[str]]:
        """Validate expense mapping completeness and accuracy."""
        
        errors = []
        
        try:
            mapped_expenses = self.get_mapped_expenses_for_export(run_id, gstin)
            
            if not mapped_expenses:
                errors.append("No mapped expenses found")
                return False, errors
            
            for expense in mapped_expenses:
                # Validate required fields
                if not expense.ledger_name:
                    errors.append(f"Missing ledger name for expense {expense.id}")
                
                if not expense.voucher_no:
                    errors.append(f"Missing voucher number for expense {expense.id}")
                
                # Validate amounts
                if expense.taxable_value <= 0:
                    errors.append(f"Invalid taxable value for expense {expense.id}")
                
                if expense.total_value <= 0:
                    errors.append(f"Invalid total value for expense {expense.id}")
                
                # Validate GST computation
                computed_gst = expense.cgst_amount + expense.sgst_amount + expense.igst_amount
                expected_gst = expense.total_value - expense.taxable_value
                
                if abs(computed_gst - expected_gst) > 0.01:  # Allow small rounding differences
                    errors.append(f"GST computation mismatch for expense {expense.id}")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            return False, errors
    
    def add_custom_expense_rule(self, channel: str, expense_type: str, ledger_name: str, gst_rate: float) -> bool:
        """Add a custom expense mapping rule."""
        
        try:
            rule = ExpenseRule(
                channel=channel,
                expense_type=expense_type,
                ledger_name=ledger_name,
                gst_rate=gst_rate,
                is_input_gst=True
            )
            
            self.expense_rules.add_custom_rule(rule)
            
            # Store in database if available
            if self.supabase:
                rule_data = {
                    'channel': channel,
                    'expense_type': expense_type,
                    'ledger_name': ledger_name,
                    'gst_rate': gst_rate,
                    'is_active': True,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                
                self.supabase.client.table("expense_mapping").upsert(rule_data).execute()
            
            self.logger.info(f"Added custom expense rule: {channel} - {expense_type} -> {ledger_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding custom expense rule: {e}")
            return False
