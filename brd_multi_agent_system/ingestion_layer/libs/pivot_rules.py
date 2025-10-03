"""
Pivot Rules Engine
Defines pivot dimensions and measures for different e-commerce channels
"""
from typing import List, Dict, Any
from enum import Enum


class PivotDimension(Enum):
    """Standard pivot dimensions for accounting systems."""
    GSTIN = "gstin"
    MONTH = "month"
    CHANNEL = "channel"
    GST_RATE = "gst_rate"
    LEDGER = "ledger_name"
    FG = "fg"
    STATE = "state_code"
    INVOICE_PREFIX = "invoice_prefix"


class PivotMeasure(Enum):
    """Standard pivot measures for accounting systems."""
    QUANTITY = "quantity"
    TAXABLE_VALUE = "taxable_value"
    CGST = "cgst"
    SGST = "sgst"
    IGST = "igst"
    TOTAL_TAX = "total_tax"
    TOTAL_AMOUNT = "total_amount"


class PivotRulesEngine:
    """
    Engine that defines pivot rules for different e-commerce channels.
    
    Provides channel-specific configurations for:
    - Grouping dimensions (what to group by)
    - Aggregation measures (what to sum up)
    - Business rules and validations
    """
    
    def __init__(self):
        """Initialize the Pivot Rules Engine."""
        self.channel_configs = self._initialize_channel_configs()
    
    def _initialize_channel_configs(self) -> Dict[str, Dict[str, Any]]:
        """Initialize channel-specific pivot configurations."""
        
        return {
            "amazon_mtr": {
                "dimensions": [
                    PivotDimension.GSTIN.value,
                    PivotDimension.MONTH.value,
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "measures": [
                    PivotMeasure.QUANTITY.value,
                    PivotMeasure.TAXABLE_VALUE.value,
                    PivotMeasure.CGST.value,
                    PivotMeasure.SGST.value,
                    PivotMeasure.IGST.value
                ],
                "sort_order": [
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "business_rules": {
                    "exclude_zero_taxable": True,
                    "round_decimals": 2,
                    "validate_tax_logic": True
                }
            },
            
            "amazon_str": {
                "dimensions": [
                    PivotDimension.GSTIN.value,
                    PivotDimension.MONTH.value,
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "measures": [
                    PivotMeasure.QUANTITY.value,
                    PivotMeasure.TAXABLE_VALUE.value,
                    PivotMeasure.IGST.value  # STR always uses IGST
                ],
                "sort_order": [
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "business_rules": {
                    "exclude_zero_taxable": True,
                    "round_decimals": 2,
                    "force_igst_only": True  # STR specific rule
                }
            },
            
            "flipkart": {
                "dimensions": [
                    PivotDimension.GSTIN.value,
                    PivotDimension.MONTH.value,
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value,
                    PivotDimension.STATE.value  # Include state for Flipkart
                ],
                "measures": [
                    PivotMeasure.QUANTITY.value,
                    PivotMeasure.TAXABLE_VALUE.value,
                    PivotMeasure.CGST.value,
                    PivotMeasure.SGST.value,
                    PivotMeasure.IGST.value
                ],
                "sort_order": [
                    PivotDimension.STATE.value,
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "business_rules": {
                    "exclude_zero_taxable": True,
                    "round_decimals": 2,
                    "include_seller_state": True
                }
            },
            
            "pepperfry": {
                "dimensions": [
                    PivotDimension.GSTIN.value,
                    PivotDimension.MONTH.value,
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "measures": [
                    PivotMeasure.QUANTITY.value,
                    PivotMeasure.TAXABLE_VALUE.value,
                    PivotMeasure.CGST.value,
                    PivotMeasure.SGST.value,
                    PivotMeasure.IGST.value
                ],
                "sort_order": [
                    PivotDimension.GST_RATE.value,
                    PivotDimension.LEDGER.value,
                    PivotDimension.FG.value
                ],
                "business_rules": {
                    "exclude_zero_taxable": True,
                    "round_decimals": 2,
                    "handle_returns": True  # Pepperfry specific rule
                }
            }
        }
    
    def get_pivot_dimensions(self, channel: str) -> List[str]:
        """
        Get pivot dimensions for a specific channel.
        
        Args:
            channel: Channel name (amazon_mtr, flipkart, etc.)
            
        Returns:
            List of dimension column names
        """
        config = self.channel_configs.get(channel, self.channel_configs["amazon_mtr"])
        return config["dimensions"]
    
    def get_pivot_measures(self, channel: str) -> List[str]:
        """
        Get pivot measures for a specific channel.
        
        Args:
            channel: Channel name (amazon_mtr, flipkart, etc.)
            
        Returns:
            List of measure column names
        """
        config = self.channel_configs.get(channel, self.channel_configs["amazon_mtr"])
        return config["measures"]
    
    def get_sort_order(self, channel: str) -> List[str]:
        """
        Get sort order for pivot output.
        
        Args:
            channel: Channel name (amazon_mtr, flipkart, etc.)
            
        Returns:
            List of column names for sorting
        """
        config = self.channel_configs.get(channel, self.channel_configs["amazon_mtr"])
        return config.get("sort_order", [])
    
    def get_business_rules(self, channel: str) -> Dict[str, Any]:
        """
        Get business rules for a specific channel.
        
        Args:
            channel: Channel name (amazon_mtr, flipkart, etc.)
            
        Returns:
            Dictionary of business rules
        """
        config = self.channel_configs.get(channel, self.channel_configs["amazon_mtr"])
        return config.get("business_rules", {})
    
    def get_pivot_template(self, channel: str) -> Dict[str, Any]:
        """
        Get complete pivot template for a channel.
        
        Args:
            channel: Channel name (amazon_mtr, flipkart, etc.)
            
        Returns:
            Complete pivot configuration
        """
        return self.channel_configs.get(channel, self.channel_configs["amazon_mtr"])
    
    def validate_pivot_columns(self, df_columns: List[str], channel: str) -> Dict[str, Any]:
        """
        Validate that DataFrame has required columns for pivoting.
        
        Args:
            df_columns: List of DataFrame column names
            channel: Channel name
            
        Returns:
            Validation result with missing columns
        """
        required_dimensions = self.get_pivot_dimensions(channel)
        required_measures = self.get_pivot_measures(channel)
        required_columns = required_dimensions + required_measures
        
        missing_columns = [col for col in required_columns if col not in df_columns]
        
        return {
            "valid": len(missing_columns) == 0,
            "missing_columns": missing_columns,
            "required_dimensions": required_dimensions,
            "required_measures": required_measures
        }
    
    def get_standard_pivot_output_columns(self) -> List[str]:
        """
        Get standard output column names for pivot CSV files.
        
        Returns:
            List of standard column names
        """
        return [
            "gstin",
            "month", 
            "gst_rate",
            "ledger",
            "fg",
            "total_quantity",
            "total_taxable",
            "total_cgst",
            "total_sgst", 
            "total_igst",
            "total_tax",
            "total_amount"
        ]
    
    def apply_channel_specific_transformations(self, df, channel: str):
        """
        Apply channel-specific transformations to pivot data.
        
        Args:
            df: Pivot DataFrame
            channel: Channel name
            
        Returns:
            Transformed DataFrame
        """
        business_rules = self.get_business_rules(channel)
        
        # Apply exclude zero taxable rule
        if business_rules.get("exclude_zero_taxable", False):
            if "total_taxable" in df.columns:
                df = df[df["total_taxable"] > 0].copy()
        
        # Apply rounding rule
        round_decimals = business_rules.get("round_decimals", 2)
        numeric_columns = ["total_quantity", "total_taxable", "total_cgst", 
                          "total_sgst", "total_igst", "total_tax", "total_amount"]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].round(round_decimals)
        
        # Apply channel-specific rules
        if channel == "amazon_str" and business_rules.get("force_igst_only", False):
            # For STR, ensure CGST and SGST are zero
            if "total_cgst" in df.columns:
                df["total_cgst"] = 0.0
            if "total_sgst" in df.columns:
                df["total_sgst"] = 0.0
        
        return df
    
    def get_supported_channels(self) -> List[str]:
        """
        Get list of supported channels.
        
        Returns:
            List of channel names
        """
        return list(self.channel_configs.keys())
    
    def add_custom_channel_config(self, channel: str, config: Dict[str, Any]) -> None:
        """
        Add custom channel configuration.
        
        Args:
            channel: Channel name
            config: Channel configuration dictionary
        """
        required_keys = ["dimensions", "measures", "business_rules"]
        if not all(key in config for key in required_keys):
            raise ValueError(f"Channel config must contain: {required_keys}")
        
        self.channel_configs[channel] = config
    
    def get_pivot_summary_template(self) -> Dict[str, Any]:
        """
        Get template for pivot summary statistics.
        
        Returns:
            Template dictionary for summary stats
        """
        return {
            "total_records": 0,
            "total_taxable_amount": 0.0,
            "total_tax_amount": 0.0,
            "total_cgst": 0.0,
            "total_sgst": 0.0,
            "total_igst": 0.0,
            "unique_ledgers": 0,
            "unique_fgs": 0,
            "gst_rate_breakdown": {},
            "ledger_breakdown": {},
            "fg_breakdown": {}
        }
