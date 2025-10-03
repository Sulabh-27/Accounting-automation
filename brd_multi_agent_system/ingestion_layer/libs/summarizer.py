"""
Summarizer Library
Generates MIS reports and summary statistics for pivot data
"""
import pandas as pd
from typing import Dict, Any, List
from collections import defaultdict


class Summarizer:
    """
    Library for generating summary statistics and MIS reports from pivot data.
    
    Provides various aggregation and reporting functions for accounting and
    management information systems.
    """
    
    def __init__(self):
        """Initialize the Summarizer."""
        pass
    
    def generate_pivot_summary(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate comprehensive summary statistics for pivot data.
        
        Args:
            pivot_df: Pivot DataFrame with aggregated data
            
        Returns:
            Dictionary with summary statistics
        """
        if len(pivot_df) == 0:
            return self._get_empty_summary()
        
        summary = {
            "total_records": len(pivot_df),
            "total_taxable_amount": 0.0,
            "total_tax_amount": 0.0,
            "total_cgst": 0.0,
            "total_sgst": 0.0,
            "total_igst": 0.0,
            "total_quantity": 0.0,
            "unique_ledgers": 0,
            "unique_fgs": 0,
            "unique_gst_rates": 0,
            "gst_rate_breakdown": {},
            "ledger_breakdown": {},
            "fg_breakdown": {},
            "state_breakdown": {}
        }
        
        # Calculate totals
        numeric_columns = {
            "total_taxable": "total_taxable_amount",
            "total_cgst": "total_cgst",
            "total_sgst": "total_sgst", 
            "total_igst": "total_igst",
            "total_quantity": "total_quantity"
        }
        
        for col, summary_key in numeric_columns.items():
            if col in pivot_df.columns:
                summary[summary_key] = float(pivot_df[col].sum())
        
        # Calculate total tax
        summary["total_tax_amount"] = (
            summary["total_cgst"] + 
            summary["total_sgst"] + 
            summary["total_igst"]
        )
        
        # Count unique dimensions
        if "ledger_name" in pivot_df.columns:
            summary["unique_ledgers"] = pivot_df["ledger_name"].nunique()
        elif "ledger" in pivot_df.columns:
            summary["unique_ledgers"] = pivot_df["ledger"].nunique()
        
        if "fg" in pivot_df.columns:
            summary["unique_fgs"] = pivot_df["fg"].nunique()
        
        if "gst_rate" in pivot_df.columns:
            summary["unique_gst_rates"] = pivot_df["gst_rate"].nunique()
        
        # Generate breakdowns
        summary["gst_rate_breakdown"] = self._generate_gst_rate_breakdown(pivot_df)
        summary["ledger_breakdown"] = self._generate_ledger_breakdown(pivot_df)
        summary["fg_breakdown"] = self._generate_fg_breakdown(pivot_df)
        
        if "state_code" in pivot_df.columns:
            summary["state_breakdown"] = self._generate_state_breakdown(pivot_df)
        
        # Round numerical values
        for key in summary:
            if isinstance(summary[key], float):
                summary[key] = round(summary[key], 2)
        
        return summary
    
    def _get_empty_summary(self) -> Dict[str, Any]:
        """Get empty summary template."""
        return {
            "total_records": 0,
            "total_taxable_amount": 0.0,
            "total_tax_amount": 0.0,
            "total_cgst": 0.0,
            "total_sgst": 0.0,
            "total_igst": 0.0,
            "total_quantity": 0.0,
            "unique_ledgers": 0,
            "unique_fgs": 0,
            "unique_gst_rates": 0,
            "gst_rate_breakdown": {},
            "ledger_breakdown": {},
            "fg_breakdown": {},
            "state_breakdown": {}
        }
    
    def _generate_gst_rate_breakdown(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate GST rate wise breakdown."""
        if "gst_rate" not in pivot_df.columns:
            return {}
        
        breakdown = {}
        
        for gst_rate in pivot_df["gst_rate"].unique():
            rate_data = pivot_df[pivot_df["gst_rate"] == gst_rate]
            
            rate_key = f"{int(gst_rate * 100)}%" if gst_rate > 0 else "0%"
            
            breakdown[rate_key] = {
                "records": len(rate_data),
                "taxable_amount": float(rate_data.get("total_taxable", pd.Series([0])).sum()),
                "tax_amount": float(
                    rate_data.get("total_cgst", pd.Series([0])).sum() +
                    rate_data.get("total_sgst", pd.Series([0])).sum() +
                    rate_data.get("total_igst", pd.Series([0])).sum()
                ),
                "quantity": float(rate_data.get("total_quantity", pd.Series([0])).sum())
            }
            
            # Round values
            for key in ["taxable_amount", "tax_amount", "quantity"]:
                breakdown[rate_key][key] = round(breakdown[rate_key][key], 2)
        
        return breakdown
    
    def _generate_ledger_breakdown(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate ledger wise breakdown."""
        ledger_col = None
        if "ledger_name" in pivot_df.columns:
            ledger_col = "ledger_name"
        elif "ledger" in pivot_df.columns:
            ledger_col = "ledger"
        
        if not ledger_col:
            return {}
        
        breakdown = {}
        
        # Get top 10 ledgers by taxable amount
        ledger_totals = pivot_df.groupby(ledger_col).agg({
            "total_taxable": "sum" if "total_taxable" in pivot_df.columns else lambda x: 0
        }).sort_values("total_taxable", ascending=False).head(10)
        
        for ledger in ledger_totals.index:
            ledger_data = pivot_df[pivot_df[ledger_col] == ledger]
            
            breakdown[ledger] = {
                "records": len(ledger_data),
                "taxable_amount": float(ledger_data.get("total_taxable", pd.Series([0])).sum()),
                "tax_amount": float(
                    ledger_data.get("total_cgst", pd.Series([0])).sum() +
                    ledger_data.get("total_sgst", pd.Series([0])).sum() +
                    ledger_data.get("total_igst", pd.Series([0])).sum()
                ),
                "quantity": float(ledger_data.get("total_quantity", pd.Series([0])).sum())
            }
            
            # Round values
            for key in ["taxable_amount", "tax_amount", "quantity"]:
                breakdown[ledger][key] = round(breakdown[ledger][key], 2)
        
        return breakdown
    
    def _generate_fg_breakdown(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate FG (Final Goods) wise breakdown."""
        if "fg" not in pivot_df.columns:
            return {}
        
        breakdown = {}
        
        # Get top 10 FGs by taxable amount
        fg_totals = pivot_df.groupby("fg").agg({
            "total_taxable": "sum" if "total_taxable" in pivot_df.columns else lambda x: 0
        }).sort_values("total_taxable", ascending=False).head(10)
        
        for fg in fg_totals.index:
            fg_data = pivot_df[pivot_df["fg"] == fg]
            
            breakdown[fg] = {
                "records": len(fg_data),
                "taxable_amount": float(fg_data.get("total_taxable", pd.Series([0])).sum()),
                "tax_amount": float(
                    fg_data.get("total_cgst", pd.Series([0])).sum() +
                    fg_data.get("total_sgst", pd.Series([0])).sum() +
                    fg_data.get("total_igst", pd.Series([0])).sum()
                ),
                "quantity": float(fg_data.get("total_quantity", pd.Series([0])).sum())
            }
            
            # Round values
            for key in ["taxable_amount", "tax_amount", "quantity"]:
                breakdown[fg][key] = round(breakdown[fg][key], 2)
        
        return breakdown
    
    def _generate_state_breakdown(self, pivot_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate state wise breakdown."""
        if "state_code" not in pivot_df.columns:
            return {}
        
        breakdown = {}
        
        for state in pivot_df["state_code"].unique():
            state_data = pivot_df[pivot_df["state_code"] == state]
            
            breakdown[state] = {
                "records": len(state_data),
                "taxable_amount": float(state_data.get("total_taxable", pd.Series([0])).sum()),
                "tax_amount": float(
                    state_data.get("total_cgst", pd.Series([0])).sum() +
                    state_data.get("total_sgst", pd.Series([0])).sum() +
                    state_data.get("total_igst", pd.Series([0])).sum()
                ),
                "quantity": float(state_data.get("total_quantity", pd.Series([0])).sum())
            }
            
            # Round values
            for key in ["taxable_amount", "tax_amount", "quantity"]:
                breakdown[state][key] = round(breakdown[state][key], 2)
        
        return breakdown
    
    def generate_mis_report(self, pivot_df: pd.DataFrame, channel: str, month: str) -> Dict[str, Any]:
        """
        Generate Management Information System (MIS) report.
        
        Args:
            pivot_df: Pivot DataFrame
            channel: Channel name
            month: Processing month
            
        Returns:
            MIS report dictionary
        """
        summary = self.generate_pivot_summary(pivot_df)
        
        mis_report = {
            "report_type": "MIS_PIVOT_SUMMARY",
            "channel": channel,
            "month": month,
            "generated_at": pd.Timestamp.now().isoformat(),
            "summary": summary,
            "key_metrics": self._calculate_key_metrics(summary),
            "recommendations": self._generate_recommendations(summary, channel)
        }
        
        return mis_report
    
    def _calculate_key_metrics(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate key business metrics."""
        metrics = {}
        
        # Average transaction value
        if summary["total_records"] > 0:
            metrics["avg_taxable_per_record"] = round(
                summary["total_taxable_amount"] / summary["total_records"], 2
            )
            metrics["avg_tax_per_record"] = round(
                summary["total_tax_amount"] / summary["total_records"], 2
            )
        else:
            metrics["avg_taxable_per_record"] = 0.0
            metrics["avg_tax_per_record"] = 0.0
        
        # Tax efficiency ratio
        if summary["total_taxable_amount"] > 0:
            metrics["tax_to_taxable_ratio"] = round(
                summary["total_tax_amount"] / summary["total_taxable_amount"], 4
            )
        else:
            metrics["tax_to_taxable_ratio"] = 0.0
        
        # Tax distribution
        total_tax = summary["total_tax_amount"]
        if total_tax > 0:
            metrics["cgst_percentage"] = round((summary["total_cgst"] / total_tax) * 100, 2)
            metrics["sgst_percentage"] = round((summary["total_sgst"] / total_tax) * 100, 2)
            metrics["igst_percentage"] = round((summary["total_igst"] / total_tax) * 100, 2)
        else:
            metrics["cgst_percentage"] = 0.0
            metrics["sgst_percentage"] = 0.0
            metrics["igst_percentage"] = 0.0
        
        # Diversity metrics
        metrics["ledger_diversity"] = summary["unique_ledgers"]
        metrics["product_diversity"] = summary["unique_fgs"]
        metrics["gst_rate_diversity"] = summary["unique_gst_rates"]
        
        return metrics
    
    def _generate_recommendations(self, summary: Dict[str, Any], channel: str) -> List[str]:
        """Generate business recommendations based on summary data."""
        recommendations = []
        
        # Tax efficiency recommendations
        if summary.get("total_tax_amount", 0) == 0:
            recommendations.append("Review GST applicability - no tax computed")
        
        # Ledger diversity recommendations
        if summary.get("unique_ledgers", 0) < 3:
            recommendations.append("Consider reviewing ledger mapping for better categorization")
        
        # Product diversity recommendations
        if summary.get("unique_fgs", 0) < 5:
            recommendations.append("Limited product diversity - consider expanding product range")
        
        # Channel-specific recommendations
        if channel == "amazon_mtr":
            igst_ratio = summary.get("total_igst", 0) / max(summary.get("total_tax_amount", 1), 1)
            if igst_ratio > 0.8:
                recommendations.append("High interstate sales - consider local fulfillment centers")
        
        if channel == "flipkart":
            if summary.get("unique_gst_rates", 0) > 3:
                recommendations.append("Multiple GST rates detected - ensure compliance")
        
        return recommendations
    
    def export_summary_report(self, summary: Dict[str, Any], output_path: str) -> bool:
        """
        Export summary report to file.
        
        Args:
            summary: Summary dictionary
            output_path: Output file path
            
        Returns:
            Success status
        """
        try:
            import json
            
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            return True
        except Exception as e:
            print(f"Error exporting summary report: {e}")
            return False
    
    def compare_summaries(self, summary1: Dict[str, Any], summary2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare two summary reports.
        
        Args:
            summary1: First summary (baseline)
            summary2: Second summary (comparison)
            
        Returns:
            Comparison result
        """
        comparison = {
            "baseline": summary1.get("total_taxable_amount", 0),
            "comparison": summary2.get("total_taxable_amount", 0),
            "difference": 0.0,
            "percentage_change": 0.0,
            "metrics_comparison": {}
        }
        
        # Calculate overall difference
        baseline_total = summary1.get("total_taxable_amount", 0)
        comparison_total = summary2.get("total_taxable_amount", 0)
        
        comparison["difference"] = comparison_total - baseline_total
        
        if baseline_total > 0:
            comparison["percentage_change"] = round(
                (comparison["difference"] / baseline_total) * 100, 2
            )
        
        # Compare key metrics
        key_metrics = ["total_records", "total_tax_amount", "unique_ledgers", "unique_fgs"]
        
        for metric in key_metrics:
            baseline_value = summary1.get(metric, 0)
            comparison_value = summary2.get(metric, 0)
            
            comparison["metrics_comparison"][metric] = {
                "baseline": baseline_value,
                "comparison": comparison_value,
                "difference": comparison_value - baseline_value
            }
        
        return comparison
