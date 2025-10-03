"""
Expense mapping rules and GST computation for seller invoices
Maps expense types to appropriate Tally ledger accounts
"""
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging


@dataclass
class ExpenseRule:
    """Rule for mapping expense type to ledger and GST treatment."""
    channel: str
    expense_type: str
    ledger_name: str
    gst_rate: float
    is_input_gst: bool = True  # Whether this qualifies for input GST credit
    hsn_code: Optional[str] = None
    description: Optional[str] = None


class ExpenseRulesEngine:
    """Engine for expense mapping and GST computation rules."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rules = self._initialize_default_rules()
    
    def _initialize_default_rules(self) -> List[ExpenseRule]:
        """Initialize default expense mapping rules for all channels."""
        
        rules = []
        
        # Amazon expense rules
        amazon_rules = [
            ExpenseRule("amazon", "Closing Fee", "Amazon Closing Fee", 0.18, True, "998314", "Marketplace closing fee"),
            ExpenseRule("amazon", "Shipping Fee", "Amazon Shipping Fee", 0.18, True, "996511", "Shipping and logistics fee"),
            ExpenseRule("amazon", "Commission", "Amazon Commission", 0.18, True, "998314", "Marketplace commission"),
            ExpenseRule("amazon", "Fulfillment Fee", "Amazon Fulfillment Fee", 0.18, True, "996511", "FBA fulfillment fee"),
            ExpenseRule("amazon", "Storage Fee", "Amazon Storage Fee", 0.18, True, "996419", "Warehouse storage fee"),
            ExpenseRule("amazon", "Advertising Fee", "Amazon Advertising Fee", 0.18, True, "998399", "Sponsored products advertising"),
            ExpenseRule("amazon", "Refund Processing Fee", "Amazon Refund Processing Fee", 0.18, True, "998314", "Refund processing charges"),
            ExpenseRule("amazon", "Return Processing Fee", "Amazon Return Processing Fee", 0.18, True, "998314", "Return processing charges"),
            ExpenseRule("amazon", "Payment Gateway Fee", "Amazon Payment Gateway Fee", 0.18, True, "998399", "Payment processing fee"),
            ExpenseRule("amazon", "Subscription Fee", "Amazon Subscription Fee", 0.18, True, "998399", "Seller subscription fee"),
            ExpenseRule("amazon", "Other Fee", "Amazon Other Charges", 0.18, True, "998399", "Other marketplace charges")
        ]
        rules.extend(amazon_rules)
        
        # Flipkart expense rules
        flipkart_rules = [
            ExpenseRule("flipkart", "Commission", "Flipkart Commission", 0.18, True, "998314", "Marketplace commission"),
            ExpenseRule("flipkart", "Collection Fee", "Flipkart Collection Fee", 0.18, True, "996511", "Cash collection fee"),
            ExpenseRule("flipkart", "Fixed Fee", "Flipkart Fixed Fee", 0.18, True, "998314", "Fixed marketplace fee"),
            ExpenseRule("flipkart", "Shipping Fee", "Flipkart Shipping Fee", 0.18, True, "996511", "Shipping and logistics"),
            ExpenseRule("flipkart", "Payment Gateway Fee", "Flipkart Payment Gateway Fee", 0.18, True, "998399", "Payment processing fee"),
            ExpenseRule("flipkart", "Storage Fee", "Flipkart Storage Fee", 0.18, True, "996419", "Warehouse storage fee"),
            ExpenseRule("flipkart", "Advertising Fee", "Flipkart Advertising Fee", 0.18, True, "998399", "Sponsored listings"),
            ExpenseRule("flipkart", "Other Fee", "Flipkart Other Charges", 0.18, True, "998399", "Other marketplace charges")
        ]
        rules.extend(flipkart_rules)
        
        # Pepperfry expense rules
        pepperfry_rules = [
            ExpenseRule("pepperfry", "Commission", "Pepperfry Commission", 0.18, True, "998314", "Marketplace commission"),
            ExpenseRule("pepperfry", "Shipping Fee", "Pepperfry Shipping Fee", 0.18, True, "996511", "Shipping charges"),
            ExpenseRule("pepperfry", "Payment Gateway Fee", "Pepperfry Payment Gateway Fee", 0.18, True, "998399", "Payment processing"),
            ExpenseRule("pepperfry", "Other Fee", "Pepperfry Other Charges", 0.18, True, "998399", "Other charges")
        ]
        rules.extend(pepperfry_rules)
        
        return rules
    
    def get_expense_rule(self, channel: str, expense_type: str) -> Optional[ExpenseRule]:
        """Get expense rule for given channel and expense type."""
        
        # First try exact match
        for rule in self.rules:
            if rule.channel.lower() == channel.lower() and rule.expense_type.lower() == expense_type.lower():
                return rule
        
        # Try partial match on expense type
        for rule in self.rules:
            if rule.channel.lower() == channel.lower():
                if expense_type.lower() in rule.expense_type.lower() or rule.expense_type.lower() in expense_type.lower():
                    return rule
        
        # Fallback to "Other Fee" for the channel
        for rule in self.rules:
            if rule.channel.lower() == channel.lower() and rule.expense_type == "Other Fee":
                return rule
        
        return None
    
    def compute_gst_split(self, taxable_amount: float, gst_rate: float, company_gstin: str, 
                         vendor_gstin: Optional[str] = None) -> Dict[str, float]:
        """Compute GST split (CGST/SGST vs IGST) for expense."""
        
        if gst_rate == 0:
            return {
                'cgst_rate': 0.0,
                'sgst_rate': 0.0,
                'igst_rate': 0.0,
                'cgst_amount': 0.0,
                'sgst_amount': 0.0,
                'igst_amount': 0.0,
                'total_gst': 0.0
            }
        
        # Extract state codes
        company_state_code = company_gstin[:2] if company_gstin else None
        vendor_state_code = vendor_gstin[:2] if vendor_gstin else None
        
        # For expenses, if vendor GSTIN is not available, assume interstate
        # This is common for marketplace fees where vendor GSTIN might not be provided
        if not vendor_gstin or company_state_code != vendor_state_code:
            # Interstate transaction - IGST
            igst_amount = taxable_amount * gst_rate
            return {
                'cgst_rate': 0.0,
                'sgst_rate': 0.0,
                'igst_rate': gst_rate,
                'cgst_amount': 0.0,
                'sgst_amount': 0.0,
                'igst_amount': igst_amount,
                'total_gst': igst_amount
            }
        else:
            # Intrastate transaction - CGST + SGST
            cgst_rate = gst_rate / 2
            sgst_rate = gst_rate / 2
            cgst_amount = taxable_amount * cgst_rate
            sgst_amount = taxable_amount * sgst_rate
            
            return {
                'cgst_rate': cgst_rate,
                'sgst_rate': sgst_rate,
                'igst_rate': 0.0,
                'cgst_amount': cgst_amount,
                'sgst_amount': sgst_amount,
                'igst_amount': 0.0,
                'total_gst': cgst_amount + sgst_amount
            }
    
    def get_gst_ledger_names(self, gst_split: Dict[str, float], is_input_gst: bool = True) -> Dict[str, str]:
        """Get appropriate GST ledger names for Tally."""
        
        prefix = "Input" if is_input_gst else "Output"
        
        ledgers = {}
        
        if gst_split['cgst_amount'] > 0:
            ledgers['cgst_ledger'] = f"{prefix} CGST @ {gst_split['cgst_rate']*100:.0f}%"
        
        if gst_split['sgst_amount'] > 0:
            ledgers['sgst_ledger'] = f"{prefix} SGST @ {gst_split['sgst_rate']*100:.0f}%"
        
        if gst_split['igst_amount'] > 0:
            ledgers['igst_ledger'] = f"{prefix} IGST @ {gst_split['igst_rate']*100:.0f}%"
        
        return ledgers
    
    def validate_expense_data(self, expense_data: Dict) -> Tuple[bool, List[str]]:
        """Validate expense data for completeness and accuracy."""
        
        errors = []
        
        # Required fields
        required_fields = ['expense_type', 'taxable_value', 'gst_rate', 'total_value']
        for field in required_fields:
            if field not in expense_data or expense_data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate amounts
        if 'taxable_value' in expense_data and 'total_value' in expense_data:
            taxable = expense_data['taxable_value']
            total = expense_data['total_value']
            
            if taxable < 0:
                errors.append("Taxable value cannot be negative")
            
            if total < taxable:
                errors.append("Total value cannot be less than taxable value")
        
        # Validate GST rate
        if 'gst_rate' in expense_data:
            gst_rate = expense_data['gst_rate']
            valid_rates = [0.0, 0.05, 0.12, 0.18, 0.28]
            if gst_rate not in valid_rates:
                errors.append(f"Invalid GST rate: {gst_rate}. Must be one of {valid_rates}")
        
        return len(errors) == 0, errors
    
    def normalize_expense_type(self, expense_type: str) -> str:
        """Normalize expense type to standard format."""
        
        expense_type = expense_type.strip()
        
        # Common variations and their standard forms
        normalizations = {
            'closing fee': 'Closing Fee',
            'closure fee': 'Closing Fee',
            'shipping fee': 'Shipping Fee',
            'delivery fee': 'Shipping Fee',
            'freight': 'Shipping Fee',
            'commission': 'Commission',
            'referral fee': 'Commission',
            'fulfillment fee': 'Fulfillment Fee',
            'fba fee': 'Fulfillment Fee',
            'storage fee': 'Storage Fee',
            'warehouse fee': 'Storage Fee',
            'advertising fee': 'Advertising Fee',
            'ads fee': 'Advertising Fee',
            'promotion fee': 'Advertising Fee',
            'payment gateway fee': 'Payment Gateway Fee',
            'payment processing fee': 'Payment Gateway Fee',
            'refund processing fee': 'Refund Processing Fee',
            'return processing fee': 'Return Processing Fee'
        }
        
        expense_lower = expense_type.lower()
        
        for variation, standard in normalizations.items():
            if variation in expense_lower:
                return standard
        
        # If no match found, return title case version
        return expense_type.title()
    
    def get_all_rules_for_channel(self, channel: str) -> List[ExpenseRule]:
        """Get all expense rules for a specific channel."""
        
        return [rule for rule in self.rules if rule.channel.lower() == channel.lower()]
    
    def add_custom_rule(self, rule: ExpenseRule) -> None:
        """Add a custom expense rule."""
        
        # Remove existing rule if it exists
        self.rules = [r for r in self.rules if not (r.channel == rule.channel and r.expense_type == rule.expense_type)]
        
        # Add new rule
        self.rules.append(rule)
        self.logger.info(f"Added custom expense rule: {rule.channel} - {rule.expense_type}")
    
    def get_expense_summary(self, expenses: List[Dict]) -> Dict:
        """Get summary statistics for a list of expenses."""
        
        if not expenses:
            return {
                'total_expenses': 0,
                'total_taxable': 0.0,
                'total_gst': 0.0,
                'total_amount': 0.0,
                'expense_types': {},
                'gst_rates': {}
            }
        
        total_taxable = sum(exp.get('taxable_value', 0) for exp in expenses)
        total_gst = sum(exp.get('tax_amount', 0) for exp in expenses)
        total_amount = sum(exp.get('total_value', 0) for exp in expenses)
        
        # Group by expense type
        expense_types = {}
        for exp in expenses:
            exp_type = exp.get('expense_type', 'Unknown')
            if exp_type not in expense_types:
                expense_types[exp_type] = {'count': 0, 'amount': 0.0}
            expense_types[exp_type]['count'] += 1
            expense_types[exp_type]['amount'] += exp.get('total_value', 0)
        
        # Group by GST rate
        gst_rates = {}
        for exp in expenses:
            rate = exp.get('gst_rate', 0)
            rate_key = f"{rate*100:.0f}%"
            if rate_key not in gst_rates:
                gst_rates[rate_key] = {'count': 0, 'amount': 0.0}
            gst_rates[rate_key]['count'] += 1
            gst_rates[rate_key]['amount'] += exp.get('total_value', 0)
        
        return {
            'total_expenses': len(expenses),
            'total_taxable': total_taxable,
            'total_gst': total_gst,
            'total_amount': total_amount,
            'expense_types': expense_types,
            'gst_rates': gst_rates
        }
