"""
Tax Rules Engine for GST Computation
Handles different e-commerce channel tax rules and GST calculations
"""
from typing import Dict, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP


class TaxRulesEngine:
    """
    Centralized tax rules engine for different e-commerce channels.
    Implements GST computation logic based on channel-specific rules.
    """
    
    # GST rates mapping
    GST_RATES = {
        0.00: {"cgst": 0.00, "sgst": 0.00, "igst": 0.00},
        0.05: {"cgst": 0.025, "sgst": 0.025, "igst": 0.05},
        0.12: {"cgst": 0.06, "sgst": 0.06, "igst": 0.12},
        0.18: {"cgst": 0.09, "sgst": 0.09, "igst": 0.18},
        0.28: {"cgst": 0.14, "sgst": 0.14, "igst": 0.28}
    }
    
    # State code to state name mapping for tax computation
    STATE_MAPPINGS = {
        "ANDHRA PRADESH": "AP", "ARUNACHAL PRADESH": "AR", "ASSAM": "AS",
        "BIHAR": "BR", "CHHATTISGARH": "CG", "GOA": "GA", "GUJARAT": "GJ",
        "HARYANA": "HR", "HIMACHAL PRADESH": "HP", "JHARKHAND": "JH",
        "KARNATAKA": "KA", "KERALA": "KL", "MADHYA PRADESH": "MP",
        "MAHARASHTRA": "MH", "MANIPUR": "MN", "MEGHALAYA": "ML",
        "MIZORAM": "MZ", "NAGALAND": "NL", "DELHI": "DL", "ODISHA": "OR",
        "PUNJAB": "PB", "RAJASTHAN": "RJ", "SIKKIM": "SK", "TAMIL NADU": "TN",
        "TELANGANA": "TG", "TRIPURA": "TR", "UTTAR PRADESH": "UP",
        "UTTARAKHAND": "UK", "WEST BENGAL": "WB", "JAMMU & KASHMIR": "JK",
        "LADAKH": "LA", "CHANDIGARH": "CH", "DADRA & NAGAR HAVELI": "DN",
        "DAMAN & DIU": "DD", "LAKSHADWEEP": "LD", "PUDUCHERRY": "PY"
    }
    
    def __init__(self, company_gstin: str):
        """
        Initialize tax rules engine with company GSTIN.
        
        Args:
            company_gstin: Company's GSTIN to determine home state
        """
        self.company_gstin = company_gstin
        self.company_state_code = self._extract_state_from_gstin(company_gstin)
    
    def _extract_state_from_gstin(self, gstin: str) -> str:
        """Extract state code from GSTIN."""
        if not gstin or len(gstin) < 2:
            return "99"  # Unknown state
        return gstin[:2]
    
    def _round_currency(self, amount: float) -> float:
        """Round currency amount to 2 decimal places using banker's rounding."""
        return float(Decimal(str(amount)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    def _is_intrastate(self, customer_state: str) -> bool:
        """
        Check if transaction is intrastate (same state as company).
        
        Args:
            customer_state: Customer's state name or code
            
        Returns:
            True if intrastate, False if interstate
        """
        # Convert state name to code if needed
        customer_state_code = self.STATE_MAPPINGS.get(customer_state.upper(), customer_state.upper())
        
        # Map GSTIN state code to state abbreviation for comparison
        company_state_abbr = {
            "01": "JK", "02": "HP", "03": "PB", "04": "CH", "05": "UK",
            "06": "HR", "07": "DL", "08": "RJ", "09": "UP", "10": "BR",
            "11": "SK", "12": "AR", "13": "NL", "14": "MN", "15": "MZ",
            "16": "TR", "17": "ML", "18": "AS", "19": "WB", "20": "JH",
            "21": "OR", "22": "CG", "23": "MP", "24": "GJ", "25": "DD",
            "26": "DN", "27": "MH", "28": "AP", "29": "KA", "30": "GA",
            "31": "LD", "32": "KL", "33": "TN", "34": "PY", "35": "AN",
            "36": "TG", "37": "LA"
        }.get(self.company_state_code, "UN")
        
        return customer_state_code == company_state_abbr
    
    def compute_amazon_mtr_tax(self, 
                              taxable_value: float, 
                              gst_rate: float, 
                              customer_state: str,
                              shipping_value: float = 0.0) -> Dict[str, float]:
        """
        Compute GST for Amazon MTR (B2C) transactions.
        
        Amazon MTR Rules:
        - B2C transactions
        - If customer state = company state → CGST + SGST
        - If customer state ≠ company state → IGST
        - Shipping charges follow same tax treatment
        
        Args:
            taxable_value: Taxable amount
            gst_rate: GST rate (0.00, 0.05, 0.12, 0.18, 0.28)
            customer_state: Customer delivery state
            shipping_value: Shipping charges (if any)
            
        Returns:
            Dict with computed tax amounts
        """
        if gst_rate not in self.GST_RATES:
            raise ValueError(f"Invalid GST rate: {gst_rate}")
        
        rates = self.GST_RATES[gst_rate]
        total_taxable = taxable_value + shipping_value
        
        if self._is_intrastate(customer_state):
            # Intrastate: CGST + SGST
            cgst = self._round_currency(total_taxable * rates["cgst"])
            sgst = self._round_currency(total_taxable * rates["sgst"])
            igst = 0.0
        else:
            # Interstate: IGST
            cgst = 0.0
            sgst = 0.0
            igst = self._round_currency(total_taxable * rates["igst"])
        
        return {
            "taxable_value": self._round_currency(taxable_value),
            "shipping_value": self._round_currency(shipping_value),
            "cgst": cgst,
            "sgst": sgst,
            "igst": igst,
            "gst_rate": gst_rate,
            "total_tax": self._round_currency(cgst + sgst + igst),
            "total_amount": self._round_currency(total_taxable + cgst + sgst + igst)
        }
    
    def compute_amazon_str_tax(self, 
                              taxable_value: float, 
                              gst_rate: float, 
                              customer_state: str,
                              shipping_value: float = 0.0) -> Dict[str, float]:
        """
        Compute GST for Amazon STR (Stock Transfer) transactions.
        
        Amazon STR Rules:
        - Stock transfers between Amazon warehouses
        - Always IGST (interstate transfers)
        - No CGST/SGST even if same state
        
        Args:
            taxable_value: Taxable amount
            gst_rate: GST rate
            customer_state: Destination state
            shipping_value: Shipping charges
            
        Returns:
            Dict with computed tax amounts
        """
        if gst_rate not in self.GST_RATES:
            raise ValueError(f"Invalid GST rate: {gst_rate}")
        
        rates = self.GST_RATES[gst_rate]
        total_taxable = taxable_value + shipping_value
        
        # STR is always IGST (stock transfer)
        cgst = 0.0
        sgst = 0.0
        igst = self._round_currency(total_taxable * rates["igst"])
        
        return {
            "taxable_value": self._round_currency(taxable_value),
            "shipping_value": self._round_currency(shipping_value),
            "cgst": cgst,
            "sgst": sgst,
            "igst": igst,
            "gst_rate": gst_rate,
            "total_tax": igst,
            "total_amount": self._round_currency(total_taxable + igst)
        }
    
    def compute_flipkart_tax(self, 
                            taxable_value: float, 
                            gst_rate: float, 
                            customer_state: str,
                            seller_state: Optional[str] = None,
                            shipping_value: float = 0.0) -> Dict[str, float]:
        """
        Compute GST for Flipkart transactions.
        
        Flipkart Rules:
        - B2C marketplace transactions
        - Tax based on seller state vs customer state
        - If seller_state not provided, use company state
        
        Args:
            taxable_value: Taxable amount
            gst_rate: GST rate
            customer_state: Customer delivery state
            seller_state: Seller's state (optional, defaults to company state)
            shipping_value: Shipping charges
            
        Returns:
            Dict with computed tax amounts
        """
        if gst_rate not in self.GST_RATES:
            raise ValueError(f"Invalid GST rate: {gst_rate}")
        
        # Use company state if seller state not provided
        effective_seller_state = seller_state or self._extract_state_from_gstin(self.company_gstin)
        
        rates = self.GST_RATES[gst_rate]
        total_taxable = taxable_value + shipping_value
        
        # Compare seller state with customer state
        seller_state_code = self.STATE_MAPPINGS.get(effective_seller_state.upper(), effective_seller_state)
        customer_state_code = self.STATE_MAPPINGS.get(customer_state.upper(), customer_state)
        
        if seller_state_code == customer_state_code:
            # Intrastate: CGST + SGST
            cgst = self._round_currency(total_taxable * rates["cgst"])
            sgst = self._round_currency(total_taxable * rates["sgst"])
            igst = 0.0
        else:
            # Interstate: IGST
            cgst = 0.0
            sgst = 0.0
            igst = self._round_currency(total_taxable * rates["igst"])
        
        return {
            "taxable_value": self._round_currency(taxable_value),
            "shipping_value": self._round_currency(shipping_value),
            "cgst": cgst,
            "sgst": sgst,
            "igst": igst,
            "gst_rate": gst_rate,
            "total_tax": self._round_currency(cgst + sgst + igst),
            "total_amount": self._round_currency(total_taxable + cgst + sgst + igst)
        }
    
    def compute_pepperfry_tax(self, 
                             taxable_value: float, 
                             gst_rate: float, 
                             customer_state: str,
                             returned_qty: int = 0,
                             total_qty: int = 1,
                             shipping_value: float = 0.0) -> Dict[str, float]:
        """
        Compute GST for Pepperfry transactions.
        
        Pepperfry Rules:
        - B2C transactions with returns handling
        - Adjust taxable value for returned quantities
        - Tax based on company state vs customer state
        
        Args:
            taxable_value: Original taxable amount
            gst_rate: GST rate
            customer_state: Customer delivery state
            returned_qty: Quantity returned
            total_qty: Total quantity ordered
            shipping_value: Shipping charges
            
        Returns:
            Dict with computed tax amounts
        """
        if gst_rate not in self.GST_RATES:
            raise ValueError(f"Invalid GST rate: {gst_rate}")
        
        # Adjust taxable value for returns
        if returned_qty > 0 and total_qty > 0:
            net_qty = max(0, total_qty - returned_qty)
            adjusted_taxable = taxable_value * (net_qty / total_qty)
        else:
            adjusted_taxable = taxable_value
        
        rates = self.GST_RATES[gst_rate]
        total_taxable = adjusted_taxable + shipping_value
        
        if self._is_intrastate(customer_state):
            # Intrastate: CGST + SGST
            cgst = self._round_currency(total_taxable * rates["cgst"])
            sgst = self._round_currency(total_taxable * rates["sgst"])
            igst = 0.0
        else:
            # Interstate: IGST
            cgst = 0.0
            sgst = 0.0
            igst = self._round_currency(total_taxable * rates["igst"])
        
        return {
            "taxable_value": self._round_currency(adjusted_taxable),
            "shipping_value": self._round_currency(shipping_value),
            "cgst": cgst,
            "sgst": sgst,
            "igst": igst,
            "gst_rate": gst_rate,
            "total_tax": self._round_currency(cgst + sgst + igst),
            "total_amount": self._round_currency(total_taxable + cgst + sgst + igst),
            "returned_qty": returned_qty,
            "net_qty": total_qty - returned_qty if returned_qty > 0 else total_qty
        }
    
    def validate_tax_computation(self, computation: Dict[str, float]) -> bool:
        """
        Validate tax computation for correctness.
        
        Args:
            computation: Tax computation result
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check if CGST + SGST = IGST (when applicable)
            if computation["cgst"] > 0 or computation["sgst"] > 0:
                # Intrastate transaction
                if computation["igst"] != 0:
                    return False
                expected_total = computation["cgst"] + computation["sgst"]
            else:
                # Interstate transaction
                if computation["cgst"] != 0 or computation["sgst"] != 0:
                    return False
                expected_total = computation["igst"]
            
            # Verify total tax calculation
            actual_total = computation["total_tax"]
            return abs(expected_total - actual_total) < 0.01
            
        except (KeyError, TypeError):
            return False
