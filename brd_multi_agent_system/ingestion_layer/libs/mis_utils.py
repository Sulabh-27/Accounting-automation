"""
MIS Utilities Library for Part-8: MIS & Audit Trail

Provides management information system utilities for aggregating sales, expenses,
and generating business intelligence reports from processed e-commerce data.
"""

import pandas as pd
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal

from .supabase_client import SupabaseClientWrapper


@dataclass
class SalesMetrics:
    """Sales performance metrics"""
    total_sales: Decimal
    total_returns: Decimal
    total_transfers: Decimal
    net_sales: Decimal
    total_transactions: int
    total_skus: int
    total_quantity: int
    average_order_value: Decimal


@dataclass
class ExpenseMetrics:
    """Expense breakdown metrics"""
    total_expenses: Decimal
    commission_expenses: Decimal
    shipping_expenses: Decimal
    fulfillment_expenses: Decimal
    advertising_expenses: Decimal
    storage_expenses: Decimal
    other_expenses: Decimal


@dataclass
class GSTMetrics:
    """GST computation metrics"""
    net_gst_output: Decimal  # GST collected on sales
    net_gst_input: Decimal   # GST paid on expenses
    gst_liability: Decimal   # Net GST payable
    cgst_amount: Decimal
    sgst_amount: Decimal
    igst_amount: Decimal


@dataclass
class ProfitabilityMetrics:
    """Profitability analysis metrics"""
    gross_profit: Decimal
    profit_margin: Decimal
    revenue_per_transaction: Decimal
    cost_per_transaction: Decimal
    return_rate: Decimal


@dataclass
class MISReport:
    """Complete MIS report structure"""
    run_id: uuid.UUID
    channel: str
    gstin: str
    month: str
    report_type: str
    
    sales_metrics: SalesMetrics
    expense_metrics: ExpenseMetrics
    gst_metrics: GSTMetrics
    profitability_metrics: ProfitabilityMetrics
    
    processing_status: str
    data_quality_score: Decimal
    exception_count: int
    approval_count: int
    
    created_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            'run_id': str(self.run_id),
            'channel': self.channel,
            'gstin': self.gstin,
            'month': self.month,
            'report_type': self.report_type,
            
            # Sales metrics
            'total_sales': float(self.sales_metrics.total_sales),
            'total_returns': float(self.sales_metrics.total_returns),
            'total_transfers': float(self.sales_metrics.total_transfers),
            'net_sales': float(self.sales_metrics.net_sales),
            'total_transactions': self.sales_metrics.total_transactions,
            'total_skus': self.sales_metrics.total_skus,
            'total_quantity': self.sales_metrics.total_quantity,
            
            # Expense metrics
            'total_expenses': float(self.expense_metrics.total_expenses),
            'commission_expenses': float(self.expense_metrics.commission_expenses),
            'shipping_expenses': float(self.expense_metrics.shipping_expenses),
            'fulfillment_expenses': float(self.expense_metrics.fulfillment_expenses),
            'advertising_expenses': float(self.expense_metrics.advertising_expenses),
            'other_expenses': float(self.expense_metrics.other_expenses),
            
            # GST metrics
            'net_gst_output': float(self.gst_metrics.net_gst_output),
            'net_gst_input': float(self.gst_metrics.net_gst_input),
            'gst_liability': float(self.gst_metrics.gst_liability),
            
            # Profitability metrics
            'gross_profit': float(self.profitability_metrics.gross_profit),
            'profit_margin': float(self.profitability_metrics.profit_margin),
            
            # Quality metrics
            'processing_status': self.processing_status,
            'data_quality_score': float(self.data_quality_score),
            'exception_count': self.exception_count,
            'approval_count': self.approval_count,
            
            'created_at': self.created_at
        }


class MISCalculator:
    """
    Management Information System calculator for business intelligence
    
    Features:
    - Sales performance analysis
    - Expense breakdown and categorization
    - GST computation and liability calculation
    - Profitability metrics and margin analysis
    - Data quality assessment
    - Multi-channel and multi-GSTIN support
    """
    
    def __init__(self, supabase_client: Optional[SupabaseClientWrapper] = None):
        self.supabase = supabase_client or SupabaseClientWrapper()
    
    def calculate_sales_metrics(
        self,
        run_id: uuid.UUID,
        pivot_data: Optional[pd.DataFrame] = None
    ) -> SalesMetrics:
        """
        Calculate sales performance metrics
        
        Args:
            run_id: Processing run identifier
            pivot_data: Optional pivot data DataFrame
            
        Returns:
            Sales metrics summary
        """
        try:
            # Get pivot summaries from database if not provided
            if pivot_data is None:
                pivot_data = self._get_pivot_summaries(run_id)
            
            if pivot_data.empty:
                return self._empty_sales_metrics()
            
            # Calculate sales metrics
            total_sales = Decimal(str(pivot_data['total_taxable_value'].sum()))
            total_returns = Decimal(str(pivot_data[pivot_data['is_return'] == True]['total_taxable_value'].sum())) if 'is_return' in pivot_data.columns else Decimal('0')
            total_transfers = Decimal('0')  # Placeholder for future transfer tracking
            net_sales = total_sales - total_returns
            
            total_transactions = int(pivot_data['total_records'].sum())
            total_skus = len(pivot_data['final_goods_name'].unique()) if 'final_goods_name' in pivot_data.columns else 0
            total_quantity = int(pivot_data['total_quantity'].sum()) if 'total_quantity' in pivot_data.columns else 0
            
            average_order_value = net_sales / total_transactions if total_transactions > 0 else Decimal('0')
            
            return SalesMetrics(
                total_sales=total_sales,
                total_returns=total_returns,
                total_transfers=total_transfers,
                net_sales=net_sales,
                total_transactions=total_transactions,
                total_skus=total_skus,
                total_quantity=total_quantity,
                average_order_value=average_order_value
            )
            
        except Exception as e:
            print(f"⚠️  Error calculating sales metrics: {e}")
            return self._empty_sales_metrics()
    
    def calculate_expense_metrics(
        self,
        run_id: uuid.UUID,
        expense_data: Optional[pd.DataFrame] = None
    ) -> ExpenseMetrics:
        """
        Calculate expense breakdown metrics
        
        Args:
            run_id: Processing run identifier
            expense_data: Optional expense data DataFrame
            
        Returns:
            Expense metrics summary
        """
        try:
            # Get seller invoices from database if not provided
            if expense_data is None:
                expense_data = self._get_seller_invoices(run_id)
            
            if expense_data.empty:
                return self._empty_expense_metrics()
            
            # Calculate expense metrics by type
            total_expenses = Decimal(str(expense_data['total_amount'].sum()))
            
            # Categorize expenses by type
            commission_expenses = self._sum_expense_by_type(expense_data, ['Commission', 'Referral Fee'])
            shipping_expenses = self._sum_expense_by_type(expense_data, ['Shipping Fee', 'Delivery Fee'])
            fulfillment_expenses = self._sum_expense_by_type(expense_data, ['Fulfillment Fee', 'Pick & Pack Fee'])
            advertising_expenses = self._sum_expense_by_type(expense_data, ['Advertising Fee', 'Promotion Fee'])
            storage_expenses = self._sum_expense_by_type(expense_data, ['Storage Fee', 'Warehouse Fee'])
            
            other_expenses = total_expenses - (commission_expenses + shipping_expenses + 
                                            fulfillment_expenses + advertising_expenses + storage_expenses)
            
            return ExpenseMetrics(
                total_expenses=total_expenses,
                commission_expenses=commission_expenses,
                shipping_expenses=shipping_expenses,
                fulfillment_expenses=fulfillment_expenses,
                advertising_expenses=advertising_expenses,
                storage_expenses=storage_expenses,
                other_expenses=other_expenses
            )
            
        except Exception as e:
            print(f"⚠️  Error calculating expense metrics: {e}")
            return self._empty_expense_metrics()
    
    def calculate_gst_metrics(
        self,
        run_id: uuid.UUID,
        sales_data: Optional[pd.DataFrame] = None,
        expense_data: Optional[pd.DataFrame] = None
    ) -> GSTMetrics:
        """
        Calculate GST computation metrics
        
        Args:
            run_id: Processing run identifier
            sales_data: Optional sales data DataFrame
            expense_data: Optional expense data DataFrame
            
        Returns:
            GST metrics summary
        """
        try:
            # Get tax computations from database if not provided
            if sales_data is None:
                sales_data = self._get_tax_computations(run_id)
            if expense_data is None:
                expense_data = self._get_seller_invoices(run_id)
            
            # Calculate output GST (from sales)
            net_gst_output = Decimal('0')
            cgst_amount = Decimal('0')
            sgst_amount = Decimal('0')
            igst_amount = Decimal('0')
            
            if not sales_data.empty:
                net_gst_output = Decimal(str(sales_data['total_gst_amount'].sum()))
                cgst_amount = Decimal(str(sales_data['cgst_amount'].sum())) if 'cgst_amount' in sales_data.columns else Decimal('0')
                sgst_amount = Decimal(str(sales_data['sgst_amount'].sum())) if 'sgst_amount' in sales_data.columns else Decimal('0')
                igst_amount = Decimal(str(sales_data['igst_amount'].sum())) if 'igst_amount' in sales_data.columns else Decimal('0')
            
            # Calculate input GST (from expenses)
            net_gst_input = Decimal('0')
            if not expense_data.empty and 'gst_amount' in expense_data.columns:
                net_gst_input = Decimal(str(expense_data['gst_amount'].sum()))
            
            # Calculate GST liability (output - input)
            gst_liability = net_gst_output - net_gst_input
            
            return GSTMetrics(
                net_gst_output=net_gst_output,
                net_gst_input=net_gst_input,
                gst_liability=gst_liability,
                cgst_amount=cgst_amount,
                sgst_amount=sgst_amount,
                igst_amount=igst_amount
            )
            
        except Exception as e:
            print(f"⚠️  Error calculating GST metrics: {e}")
            return self._empty_gst_metrics()
    
    def calculate_profitability_metrics(
        self,
        sales_metrics: SalesMetrics,
        expense_metrics: ExpenseMetrics
    ) -> ProfitabilityMetrics:
        """
        Calculate profitability analysis metrics
        
        Args:
            sales_metrics: Sales performance metrics
            expense_metrics: Expense breakdown metrics
            
        Returns:
            Profitability metrics summary
        """
        try:
            gross_profit = sales_metrics.net_sales - expense_metrics.total_expenses
            profit_margin = (gross_profit / sales_metrics.net_sales * 100) if sales_metrics.net_sales > 0 else Decimal('0')
            
            revenue_per_transaction = sales_metrics.net_sales / sales_metrics.total_transactions if sales_metrics.total_transactions > 0 else Decimal('0')
            cost_per_transaction = expense_metrics.total_expenses / sales_metrics.total_transactions if sales_metrics.total_transactions > 0 else Decimal('0')
            
            return_rate = (sales_metrics.total_returns / sales_metrics.total_sales * 100) if sales_metrics.total_sales > 0 else Decimal('0')
            
            return ProfitabilityMetrics(
                gross_profit=gross_profit,
                profit_margin=profit_margin,
                revenue_per_transaction=revenue_per_transaction,
                cost_per_transaction=cost_per_transaction,
                return_rate=return_rate
            )
            
        except Exception as e:
            print(f"⚠️  Error calculating profitability metrics: {e}")
            return ProfitabilityMetrics(
                gross_profit=Decimal('0'),
                profit_margin=Decimal('0'),
                revenue_per_transaction=Decimal('0'),
                cost_per_transaction=Decimal('0'),
                return_rate=Decimal('0')
            )
    
    def calculate_data_quality_score(
        self,
        run_id: uuid.UUID,
        total_records: int
    ) -> Tuple[Decimal, int, int]:
        """
        Calculate data quality score and metrics
        
        Args:
            run_id: Processing run identifier
            total_records: Total number of records processed
            
        Returns:
            Tuple of (quality_score, exception_count, approval_count)
        """
        try:
            # Get exception count
            exception_count = self._get_exception_count(run_id)
            
            # Get approval count
            approval_count = self._get_approval_count(run_id)
            
            # Calculate quality score (100% - percentage of issues)
            total_issues = exception_count + approval_count
            quality_score = Decimal('100.0')
            
            if total_records > 0:
                issue_percentage = (total_issues / total_records) * 100
                quality_score = max(Decimal('0'), Decimal('100.0') - Decimal(str(issue_percentage)))
            
            return quality_score, exception_count, approval_count
            
        except Exception as e:
            print(f"⚠️  Error calculating data quality score: {e}")
            return Decimal('100.0'), 0, 0
    
    def generate_mis_report(
        self,
        run_id: uuid.UUID,
        channel: str,
        gstin: str,
        month: str,
        report_type: str = "monthly"
    ) -> MISReport:
        """
        Generate complete MIS report
        
        Args:
            run_id: Processing run identifier
            channel: E-commerce channel
            gstin: Company GSTIN
            month: Processing month
            report_type: Type of report (monthly, quarterly, annual)
            
        Returns:
            Complete MIS report
        """
        try:
            print(f"📊 Generating MIS report for {channel} - {gstin} - {month}")
            
            # Calculate all metrics
            sales_metrics = self.calculate_sales_metrics(run_id)
            expense_metrics = self.calculate_expense_metrics(run_id)
            gst_metrics = self.calculate_gst_metrics(run_id)
            profitability_metrics = self.calculate_profitability_metrics(sales_metrics, expense_metrics)
            
            # Calculate data quality
            quality_score, exception_count, approval_count = self.calculate_data_quality_score(
                run_id, sales_metrics.total_transactions
            )
            
            # Create MIS report
            mis_report = MISReport(
                run_id=run_id,
                channel=channel,
                gstin=gstin,
                month=month,
                report_type=report_type,
                sales_metrics=sales_metrics,
                expense_metrics=expense_metrics,
                gst_metrics=gst_metrics,
                profitability_metrics=profitability_metrics,
                processing_status="completed",
                data_quality_score=quality_score,
                exception_count=exception_count,
                approval_count=approval_count,
                created_at=datetime.now()
            )
            
            print(f"✅ MIS report generated successfully")
            print(f"   📈 Sales: ₹{sales_metrics.net_sales:,.2f}")
            print(f"   💰 Expenses: ₹{expense_metrics.total_expenses:,.2f}")
            print(f"   📊 Profit: ₹{profitability_metrics.gross_profit:,.2f} ({profitability_metrics.profit_margin:.1f}%)")
            print(f"   🏛️  GST Liability: ₹{gst_metrics.gst_liability:,.2f}")
            print(f"   ⭐ Quality Score: {quality_score:.1f}%")
            
            return mis_report
            
        except Exception as e:
            print(f"⚠️  Error generating MIS report: {e}")
            # Return empty report on error
            return self._empty_mis_report(run_id, channel, gstin, month, report_type)
    
    def save_mis_report(self, mis_report: MISReport) -> str:
        """
        Save MIS report to database
        
        Args:
            mis_report: Complete MIS report
            
        Returns:
            Report ID
        """
        try:
            report_data = mis_report.to_dict()
            
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('mis_reports').insert(report_data).execute()
                report_id = result.data[0]['id'] if result.data else str(uuid.uuid4())
                print(f"💾 MIS report saved to database: {report_id}")
            else:
                # Development mode
                report_id = str(uuid.uuid4())
                print(f"💾 MIS report saved (development mode): {report_id}")
            
            return report_id
            
        except Exception as e:
            print(f"⚠️  Error saving MIS report: {e}")
            return str(uuid.uuid4())
    
    def export_mis_report_csv(
        self,
        mis_report: MISReport,
        output_path: str
    ) -> str:
        """
        Export MIS report to CSV format
        
        Args:
            mis_report: Complete MIS report
            output_path: Output file path
            
        Returns:
            Exported file path
        """
        try:
            # Create DataFrame for export
            report_data = {
                'Channel': [mis_report.channel],
                'GSTIN': [mis_report.gstin],
                'Month': [mis_report.month],
                'Total Sales': [float(mis_report.sales_metrics.total_sales)],
                'Total Returns': [float(mis_report.sales_metrics.total_returns)],
                'Net Sales': [float(mis_report.sales_metrics.net_sales)],
                'Total Expenses': [float(mis_report.expense_metrics.total_expenses)],
                'Commission Expenses': [float(mis_report.expense_metrics.commission_expenses)],
                'Shipping Expenses': [float(mis_report.expense_metrics.shipping_expenses)],
                'Fulfillment Expenses': [float(mis_report.expense_metrics.fulfillment_expenses)],
                'Gross Profit': [float(mis_report.profitability_metrics.gross_profit)],
                'Profit Margin %': [float(mis_report.profitability_metrics.profit_margin)],
                'GST Output': [float(mis_report.gst_metrics.net_gst_output)],
                'GST Input': [float(mis_report.gst_metrics.net_gst_input)],
                'GST Liability': [float(mis_report.gst_metrics.gst_liability)],
                'Total Transactions': [mis_report.sales_metrics.total_transactions],
                'Total SKUs': [mis_report.sales_metrics.total_skus],
                'Data Quality Score': [float(mis_report.data_quality_score)],
                'Exception Count': [mis_report.exception_count],
                'Approval Count': [mis_report.approval_count]
            }
            
            df = pd.DataFrame(report_data)
            df.to_csv(output_path, index=False)
            
            print(f"📄 MIS report exported to CSV: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"⚠️  Error exporting MIS report to CSV: {e}")
            return output_path
    
    # Helper methods
    def _get_pivot_summaries(self, run_id: uuid.UUID) -> pd.DataFrame:
        """Get pivot summaries from database"""
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('pivot_summaries').select('*').eq('run_id', str(run_id)).execute()
                return pd.DataFrame(result.data)
            else:
                return pd.DataFrame()
        except Exception:
            return pd.DataFrame()
    
    def _get_seller_invoices(self, run_id: uuid.UUID) -> pd.DataFrame:
        """Get seller invoices from database"""
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('seller_invoices').select('*').eq('run_id', str(run_id)).execute()
                return pd.DataFrame(result.data)
            else:
                return pd.DataFrame()
        except Exception:
            return pd.DataFrame()
    
    def _get_tax_computations(self, run_id: uuid.UUID) -> pd.DataFrame:
        """Get tax computations from database"""
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('tax_computations').select('*').eq('run_id', str(run_id)).execute()
                return pd.DataFrame(result.data)
            else:
                return pd.DataFrame()
        except Exception:
            return pd.DataFrame()
    
    def _get_exception_count(self, run_id: uuid.UUID) -> int:
        """Get exception count for run"""
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('exceptions').select('id', count='exact').eq('run_id', str(run_id)).execute()
                return result.count or 0
            else:
                return 0
        except Exception:
            return 0
    
    def _get_approval_count(self, run_id: uuid.UUID) -> int:
        """Get approval count for run"""
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('approval_queue').select('id', count='exact').eq('run_id', str(run_id)).execute()
                return result.count or 0
            else:
                return 0
        except Exception:
            return 0
    
    def _sum_expense_by_type(self, expense_data: pd.DataFrame, expense_types: List[str]) -> Decimal:
        """Sum expenses by type"""
        if 'expense_type' not in expense_data.columns:
            return Decimal('0')
        
        filtered_data = expense_data[expense_data['expense_type'].isin(expense_types)]
        return Decimal(str(filtered_data['total_amount'].sum())) if not filtered_data.empty else Decimal('0')
    
    def _empty_sales_metrics(self) -> SalesMetrics:
        """Return empty sales metrics"""
        return SalesMetrics(
            total_sales=Decimal('0'),
            total_returns=Decimal('0'),
            total_transfers=Decimal('0'),
            net_sales=Decimal('0'),
            total_transactions=0,
            total_skus=0,
            total_quantity=0,
            average_order_value=Decimal('0')
        )
    
    def _empty_expense_metrics(self) -> ExpenseMetrics:
        """Return empty expense metrics"""
        return ExpenseMetrics(
            total_expenses=Decimal('0'),
            commission_expenses=Decimal('0'),
            shipping_expenses=Decimal('0'),
            fulfillment_expenses=Decimal('0'),
            advertising_expenses=Decimal('0'),
            storage_expenses=Decimal('0'),
            other_expenses=Decimal('0')
        )
    
    def _empty_gst_metrics(self) -> GSTMetrics:
        """Return empty GST metrics"""
        return GSTMetrics(
            net_gst_output=Decimal('0'),
            net_gst_input=Decimal('0'),
            gst_liability=Decimal('0'),
            cgst_amount=Decimal('0'),
            sgst_amount=Decimal('0'),
            igst_amount=Decimal('0')
        )
    
    def _empty_mis_report(
        self,
        run_id: uuid.UUID,
        channel: str,
        gstin: str,
        month: str,
        report_type: str
    ) -> MISReport:
        """Return empty MIS report"""
        return MISReport(
            run_id=run_id,
            channel=channel,
            gstin=gstin,
            month=month,
            report_type=report_type,
            sales_metrics=self._empty_sales_metrics(),
            expense_metrics=self._empty_expense_metrics(),
            gst_metrics=self._empty_gst_metrics(),
            profitability_metrics=ProfitabilityMetrics(
                gross_profit=Decimal('0'),
                profit_margin=Decimal('0'),
                revenue_per_transaction=Decimal('0'),
                cost_per_transaction=Decimal('0'),
                return_rate=Decimal('0')
            ),
            processing_status="error",
            data_quality_score=Decimal('0'),
            exception_count=0,
            approval_count=0,
            created_at=datetime.now()
        )
