"""
Expense Tally Exporter Agent
Exports mapped expenses to X2Beta Excel format for Tally import
Chains with Part-5 sales exporter to create combined sales + expense exports
"""
import os
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

from ..libs.x2beta_writer import X2BetaWriter
from ..libs.contracts import ProcessingResult
from ..agents.expense_mapper import ExpenseMapperAgent, MappedExpense


@dataclass
class ExpenseExportResult:
    """Result of expense export operation."""
    success: bool
    exported_files: int
    export_paths: List[str]
    total_records: int
    total_taxable: float
    total_tax: float
    expense_types_processed: List[str]
    error_message: Optional[str] = None


class ExpenseTallyExporterAgent:
    """Agent for exporting expenses to X2Beta Excel format."""
    
    def __init__(self, supabase_client=None):
        self.supabase = supabase_client
        self.x2beta_writer = X2BetaWriter()
        self.expense_mapper = ExpenseMapperAgent(supabase_client)
        self.logger = logging.getLogger(__name__)
    
    def export_expenses_to_x2beta(self, run_id: uuid.UUID, gstin: str, channel: str, 
                                 month: str, export_dir: str) -> ExpenseExportResult:
        """Export expenses to X2Beta Excel format."""
        
        try:
            self.logger.info(f"Starting expense export - Run: {run_id}, GSTIN: {gstin}")
            
            # Get mapped expenses
            mapped_expenses = self.expense_mapper.get_mapped_expenses_for_export(run_id, gstin)
            
            if not mapped_expenses:
                return ExpenseExportResult(
                    success=True,
                    exported_files=0,
                    export_paths=[],
                    total_records=0,
                    total_taxable=0.0,
                    total_tax=0.0,
                    expense_types_processed=[],
                    error_message="No expenses found for export"
                )
            
            # Validate template availability
            template_validation = self._validate_expense_template(gstin)
            if not template_validation['available']:
                return ExpenseExportResult(
                    success=False,
                    exported_files=0,
                    export_paths=[],
                    total_records=0,
                    total_taxable=0.0,
                    total_tax=0.0,
                    expense_types_processed=[],
                    error_message=f"Expense template not available: {template_validation.get('error', 'Unknown error')}"
                )
            
            # Create export directory
            os.makedirs(export_dir, exist_ok=True)
            
            # Convert expenses to X2Beta format
            x2beta_df = self._convert_expenses_to_x2beta(mapped_expenses, gstin, month)
            
            # Generate export file
            export_path = self._generate_export_file(x2beta_df, gstin, channel, month, export_dir)
            
            # Calculate totals
            total_taxable = sum(exp.taxable_value for exp in mapped_expenses)
            total_tax = sum(exp.cgst_amount + exp.sgst_amount + exp.igst_amount for exp in mapped_expenses)
            expense_types = list(set(exp.expense_type for exp in mapped_expenses))
            
            # Store export metadata in database
            if self.supabase:
                self._store_export_metadata(run_id, gstin, channel, month, export_path, 
                                          len(mapped_expenses), total_taxable, total_tax)
            
            self.logger.info(f"Successfully exported {len(mapped_expenses)} expenses to {export_path}")
            
            return ExpenseExportResult(
                success=True,
                exported_files=1,
                export_paths=[export_path],
                total_records=len(mapped_expenses),
                total_taxable=total_taxable,
                total_tax=total_tax,
                expense_types_processed=expense_types
            )
            
        except Exception as e:
            self.logger.error(f"Error in expense export: {e}")
            return ExpenseExportResult(
                success=False,
                exported_files=0,
                export_paths=[],
                total_records=0,
                total_taxable=0.0,
                total_tax=0.0,
                expense_types_processed=[],
                error_message=str(e)
            )
    
    def _validate_expense_template(self, gstin: str) -> Dict:
        """Validate that expense template exists for GSTIN."""
        
        template_name = f"X2Beta Expense Template - {gstin}.xlsx"
        template_path = f"ingestion_layer/templates/{template_name}"
        
        if os.path.exists(template_path):
            return {
                'available': True,
                'template_name': template_name,
                'template_path': template_path
            }
        else:
            # Try to use sales template as fallback
            sales_template = f"ingestion_layer/templates/X2Beta Sales Template - {gstin}.xlsx"
            if os.path.exists(sales_template):
                return {
                    'available': True,
                    'template_name': f"X2Beta Sales Template - {gstin}.xlsx",
                    'template_path': sales_template,
                    'note': 'Using sales template as fallback for expenses'
                }
            else:
                return {
                    'available': False,
                    'error': f"No template found for GSTIN {gstin}"
                }
    
    def _convert_expenses_to_x2beta(self, expenses: List[MappedExpense], gstin: str, month: str) -> pd.DataFrame:
        """Convert mapped expenses to X2Beta DataFrame format."""
        
        x2beta_records = []
        
        for expense in expenses:
            # Base expense entry (debit to expense ledger)
            base_record = {
                'Date': expense.invoice_date.strftime('%d-%m-%Y'),
                'Voucher No.': expense.voucher_no,
                'Voucher Type': expense.voucher_type,
                'Party Ledger': expense.ledger_name,
                'Item Name': f"{expense.expense_type} - {expense.invoice_no}",
                'Quantity': 1,
                'Rate': expense.taxable_value,
                'Taxable Amount': expense.taxable_value,
                'CGST Amount': 0.0,
                'SGST Amount': 0.0,
                'IGST Amount': 0.0,
                'Total Amount': expense.taxable_value,
                'Narration': f"{expense.expense_type} expense from {expense.channel} - Invoice: {expense.invoice_no}"
            }
            x2beta_records.append(base_record)
            
            # Input GST entries (debit to input GST ledgers)
            if expense.cgst_amount > 0:
                cgst_record = {
                    'Date': expense.invoice_date.strftime('%d-%m-%Y'),
                    'Voucher No.': expense.voucher_no,
                    'Voucher Type': expense.voucher_type,
                    'Party Ledger': expense.cgst_ledger or f"Input CGST @ {expense.gst_rate*50:.0f}%",
                    'Item Name': f"Input CGST - {expense.invoice_no}",
                    'Quantity': 1,
                    'Rate': expense.cgst_amount,
                    'Taxable Amount': 0.0,
                    'CGST Amount': expense.cgst_amount,
                    'SGST Amount': 0.0,
                    'IGST Amount': 0.0,
                    'Total Amount': expense.cgst_amount,
                    'Narration': f"Input CGST on {expense.expense_type}"
                }
                x2beta_records.append(cgst_record)
            
            if expense.sgst_amount > 0:
                sgst_record = {
                    'Date': expense.invoice_date.strftime('%d-%m-%Y'),
                    'Voucher No.': expense.voucher_no,
                    'Voucher Type': expense.voucher_type,
                    'Party Ledger': expense.sgst_ledger or f"Input SGST @ {expense.gst_rate*50:.0f}%",
                    'Item Name': f"Input SGST - {expense.invoice_no}",
                    'Quantity': 1,
                    'Rate': expense.sgst_amount,
                    'Taxable Amount': 0.0,
                    'CGST Amount': 0.0,
                    'SGST Amount': expense.sgst_amount,
                    'IGST Amount': 0.0,
                    'Total Amount': expense.sgst_amount,
                    'Narration': f"Input SGST on {expense.expense_type}"
                }
                x2beta_records.append(sgst_record)
            
            if expense.igst_amount > 0:
                igst_record = {
                    'Date': expense.invoice_date.strftime('%d-%m-%Y'),
                    'Voucher No.': expense.voucher_no,
                    'Voucher Type': expense.voucher_type,
                    'Party Ledger': expense.igst_ledger or f"Input IGST @ {expense.gst_rate*100:.0f}%",
                    'Item Name': f"Input IGST - {expense.invoice_no}",
                    'Quantity': 1,
                    'Rate': expense.igst_amount,
                    'Taxable Amount': 0.0,
                    'CGST Amount': 0.0,
                    'SGST Amount': 0.0,
                    'IGST Amount': expense.igst_amount,
                    'Total Amount': expense.igst_amount,
                    'Narration': f"Input IGST on {expense.expense_type}"
                }
                x2beta_records.append(igst_record)
            
            # Credit entry to vendor/marketplace ledger
            vendor_ledger = f"{expense.channel.title()} Payable"
            credit_record = {
                'Date': expense.invoice_date.strftime('%d-%m-%Y'),
                'Voucher No.': expense.voucher_no,
                'Voucher Type': expense.voucher_type,
                'Party Ledger': vendor_ledger,
                'Item Name': f"Payable - {expense.invoice_no}",
                'Quantity': 1,
                'Rate': -expense.total_value,  # Credit entry (negative)
                'Taxable Amount': -expense.total_value,
                'CGST Amount': 0.0,
                'SGST Amount': 0.0,
                'IGST Amount': 0.0,
                'Total Amount': -expense.total_value,
                'Narration': f"Amount payable to {expense.channel} for {expense.expense_type}"
            }
            x2beta_records.append(credit_record)
        
        return pd.DataFrame(x2beta_records)
    
    def _generate_export_file(self, x2beta_df: pd.DataFrame, gstin: str, channel: str, 
                            month: str, export_dir: str) -> str:
        """Generate X2Beta Excel export file."""
        
        # Create filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{channel}_expenses_{gstin}_{month}_x2beta_{timestamp}.xlsx"
        export_path = os.path.join(export_dir, filename)
        
        # Load template
        template_path = f"ingestion_layer/templates/X2Beta Sales Template - {gstin}.xlsx"
        
        if os.path.exists(template_path):
            # Use template-based export
            try:
                self.x2beta_writer.write_to_template(x2beta_df, template_path, export_path)
            except Exception as e:
                self.logger.warning(f"Template export failed, using simple Excel: {e}")
                # Fallback to simple Excel export
                x2beta_df.to_excel(export_path, index=False, sheet_name="Expense Vouchers")
        else:
            # Simple Excel export
            x2beta_df.to_excel(export_path, index=False, sheet_name="Expense Vouchers")
        
        return export_path
    
    def _store_export_metadata(self, run_id: uuid.UUID, gstin: str, channel: str, month: str,
                             export_path: str, record_count: int, total_taxable: float, total_tax: float):
        """Store export metadata in database."""
        
        try:
            export_record = {
                'id': str(uuid.uuid4()),
                'run_id': str(run_id),
                'channel': channel,
                'gstin': gstin,
                'month': month,
                'expense_type': 'Mixed',  # Multiple expense types
                'template_name': f"X2Beta Expense Template - {gstin}.xlsx",
                'file_path': export_path,
                'file_size': os.path.getsize(export_path) if os.path.exists(export_path) else 0,
                'record_count': record_count,
                'total_taxable': total_taxable,
                'total_tax': total_tax,
                'export_status': 'success',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            self.supabase.client.table("expense_exports").insert(export_record).execute()
            self.logger.info(f"Stored expense export metadata for {record_count} records")
            
        except Exception as e:
            self.logger.error(f"Error storing export metadata: {e}")
    
    def create_combined_sales_expense_export(self, run_id: uuid.UUID, gstin: str, channel: str,
                                           month: str, export_dir: str, sales_export_path: str) -> ExpenseExportResult:
        """Create combined sales + expense X2Beta export file."""
        
        try:
            self.logger.info(f"Creating combined sales + expense export")
            
            # Get expense export
            expense_result = self.export_expenses_to_x2beta(run_id, gstin, channel, month, export_dir)
            
            if not expense_result.success or not expense_result.export_paths:
                return expense_result
            
            # Load sales data
            if not os.path.exists(sales_export_path):
                return ExpenseExportResult(
                    success=False,
                    exported_files=0,
                    export_paths=[],
                    total_records=0,
                    total_taxable=0.0,
                    total_tax=0.0,
                    expense_types_processed=[],
                    error_message=f"Sales export file not found: {sales_export_path}"
                )
            
            # Read sales and expense data
            try:
                sales_df = pd.read_excel(sales_export_path, skiprows=4)  # Skip header rows
            except:
                sales_df = pd.read_excel(sales_export_path)
            
            expense_df = pd.read_excel(expense_result.export_paths[0])
            
            # Combine data
            combined_df = pd.concat([sales_df, expense_df], ignore_index=True)
            
            # Create combined export file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            combined_filename = f"{channel}_combined_{gstin}_{month}_x2beta_{timestamp}.xlsx"
            combined_path = os.path.join(export_dir, combined_filename)
            
            # Export combined file
            combined_df.to_excel(combined_path, index=False, sheet_name="Combined Vouchers")
            
            # Update result
            expense_result.export_paths.append(combined_path)
            expense_result.exported_files += 1
            
            self.logger.info(f"Created combined export: {combined_path}")
            
            return expense_result
            
        except Exception as e:
            self.logger.error(f"Error creating combined export: {e}")
            return ExpenseExportResult(
                success=False,
                exported_files=0,
                export_paths=[],
                total_records=0,
                total_taxable=0.0,
                total_tax=0.0,
                expense_types_processed=[],
                error_message=str(e)
            )
    
    def get_export_summary(self, run_id: uuid.UUID, gstin: str) -> Dict:
        """Get summary of expense exports for a run."""
        
        if not self.supabase:
            return {'error': 'No database connection'}
        
        try:
            result = self.supabase.client.table("expense_exports").select("*").eq("run_id", str(run_id)).eq("gstin", gstin).execute()
            
            if not result.data:
                return {
                    'total_exports': 0,
                    'total_records': 0,
                    'total_amount': 0.0,
                    'export_files': []
                }
            
            exports = result.data
            
            total_records = sum(exp['record_count'] for exp in exports)
            total_amount = sum(exp['total_taxable'] + exp['total_tax'] for exp in exports)
            
            export_files = [
                {
                    'file_path': exp['file_path'],
                    'record_count': exp['record_count'],
                    'total_amount': exp['total_taxable'] + exp['total_tax'],
                    'created_at': exp['created_at']
                }
                for exp in exports
            ]
            
            return {
                'total_exports': len(exports),
                'total_records': total_records,
                'total_amount': total_amount,
                'export_files': export_files
            }
            
        except Exception as e:
            self.logger.error(f"Error getting export summary: {e}")
            return {'error': str(e)}
    
    def validate_expense_export(self, export_path: str) -> Tuple[bool, List[str]]:
        """Validate generated expense export file."""
        
        errors = []
        
        try:
            if not os.path.exists(export_path):
                errors.append(f"Export file not found: {export_path}")
                return False, errors
            
            # Read and validate Excel file
            df = pd.read_excel(export_path)
            
            if df.empty:
                errors.append("Export file is empty")
                return False, errors
            
            # Check required columns
            required_columns = ['Date', 'Voucher No.', 'Party Ledger', 'Total Amount']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                errors.append(f"Missing required columns: {missing_columns}")
            
            # Validate data integrity
            if 'Total Amount' in df.columns:
                total_sum = df['Total Amount'].sum()
                if abs(total_sum) > 0.01:  # Should balance to zero (debits + credits)
                    errors.append(f"Voucher entries don't balance: total = {total_sum}")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            return False, errors
