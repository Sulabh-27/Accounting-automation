"""
Error Codes Library for Part 7: Exception Handling & Approval Workflows
Centralized catalog of standardized error codes for consistent exception handling
"""
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, Any


@dataclass
class ErrorDefinition:
    """Definition of a standardized error code."""
    code: str
    category: str
    name: str
    description: str
    severity: str  # 'info', 'warning', 'error', 'critical'
    auto_resolve: bool = False
    requires_approval: bool = False


class ErrorSeverity(Enum):
    """Error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error category codes."""
    MAPPING = "MAP"
    LEDGER = "LED"
    GST = "GST"
    INVOICE = "INV"
    SCHEMA = "SCH"
    EXPORT = "EXP"
    DATA = "DAT"
    SYSTEM = "SYS"


class ErrorCodes:
    """Centralized error code definitions."""
    
    # Mapping Issues (MAP-xxx)
    MAP_001 = ErrorDefinition(
        code="MAP-001",
        category="MAP",
        name="Missing SKU Mapping",
        description="SKU not found in item_master table",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=True,
        requires_approval=True
    )
    
    MAP_002 = ErrorDefinition(
        code="MAP-002",
        category="MAP",
        name="Missing ASIN Mapping",
        description="ASIN not found in item_master table",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=True,
        requires_approval=True
    )
    
    MAP_003 = ErrorDefinition(
        code="MAP-003",
        category="MAP",
        name="Ambiguous SKU Mapping",
        description="Multiple Final Goods found for single SKU",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    MAP_004 = ErrorDefinition(
        code="MAP-004",
        category="MAP",
        name="Invalid Final Goods Name",
        description="Final Goods name contains invalid characters or format",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    # Ledger Issues (LED-xxx)
    LED_001 = ErrorDefinition(
        code="LED-001",
        category="LED",
        name="Missing Ledger Mapping",
        description="Channel and state combination not found in ledger_master",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=True,
        requires_approval=True
    )
    
    LED_002 = ErrorDefinition(
        code="LED-002",
        category="LED",
        name="Invalid State Code",
        description="State code not recognized or invalid format",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    LED_003 = ErrorDefinition(
        code="LED-003",
        category="LED",
        name="Invalid Channel Name",
        description="Channel name not supported or invalid format",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    LED_004 = ErrorDefinition(
        code="LED-004",
        category="LED",
        name="Duplicate Ledger Mapping",
        description="Multiple ledger names found for same channel-state combination",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    # GST Issues (GST-xxx)
    GST_001 = ErrorDefinition(
        code="GST-001",
        category="GST",
        name="Invalid GST Rate",
        description="GST rate not in allowed values (0%, 5%, 12%, 18%, 28%)",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    GST_002 = ErrorDefinition(
        code="GST-002",
        category="GST",
        name="GST Calculation Mismatch",
        description="Computed GST amount doesn't match expected calculation",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    GST_003 = ErrorDefinition(
        code="GST-003",
        category="GST",
        name="Missing GST Rate",
        description="GST rate not specified for taxable transaction",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    GST_004 = ErrorDefinition(
        code="GST-004",
        category="GST",
        name="Interstate Detection Error",
        description="Unable to determine if transaction is interstate or intrastate",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    # Invoice Issues (INV-xxx)
    INV_001 = ErrorDefinition(
        code="INV-001",
        category="INV",
        name="Duplicate Invoice Number",
        description="Invoice number already exists in system",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    INV_002 = ErrorDefinition(
        code="INV-002",
        category="INV",
        name="Invalid Invoice Format",
        description="Invoice number doesn't match expected pattern",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=True,
        requires_approval=False
    )
    
    INV_003 = ErrorDefinition(
        code="INV-003",
        category="INV",
        name="Invoice Date Invalid",
        description="Invoice date is invalid or outside acceptable range",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    INV_004 = ErrorDefinition(
        code="INV-004",
        category="INV",
        name="Invoice Sequence Gap",
        description="Gap detected in invoice number sequence",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    # Schema Issues (SCH-xxx)
    SCH_001 = ErrorDefinition(
        code="SCH-001",
        category="SCH",
        name="Missing Required Column",
        description="Required column missing from input data",
        severity=ErrorSeverity.CRITICAL.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    SCH_002 = ErrorDefinition(
        code="SCH-002",
        category="SCH",
        name="Invalid Data Type",
        description="Column contains data of incorrect type",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    SCH_003 = ErrorDefinition(
        code="SCH-003",
        category="SCH",
        name="Data Out of Range",
        description="Numeric value outside acceptable range",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    SCH_004 = ErrorDefinition(
        code="SCH-004",
        category="SCH",
        name="Invalid Date Format",
        description="Date column contains invalid date format",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    # Export Issues (EXP-xxx)
    EXP_001 = ErrorDefinition(
        code="EXP-001",
        category="EXP",
        name="Template Not Found",
        description="X2Beta template file not found for GSTIN",
        severity=ErrorSeverity.CRITICAL.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    EXP_002 = ErrorDefinition(
        code="EXP-002",
        category="EXP",
        name="Template Validation Failed",
        description="X2Beta template structure validation failed",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    EXP_003 = ErrorDefinition(
        code="EXP-003",
        category="EXP",
        name="Export File Creation Failed",
        description="Unable to create X2Beta export file",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    EXP_004 = ErrorDefinition(
        code="EXP-004",
        category="EXP",
        name="Data Mapping Error",
        description="Error mapping batch data to X2Beta template",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    # Data Quality Issues (DAT-xxx)
    DAT_001 = ErrorDefinition(
        code="DAT-001",
        category="DAT",
        name="Negative Amount",
        description="Negative value found in amount field",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=True,
        requires_approval=True
    )
    
    DAT_002 = ErrorDefinition(
        code="DAT-002",
        category="DAT",
        name="Zero Quantity",
        description="Zero or negative quantity in transaction",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=True,
        requires_approval=True
    )
    
    DAT_003 = ErrorDefinition(
        code="DAT-003",
        category="DAT",
        name="Missing Transaction Data",
        description="Required transaction data is missing or empty",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    DAT_004 = ErrorDefinition(
        code="DAT-004",
        category="DAT",
        name="Data Inconsistency",
        description="Inconsistent data detected across related fields",
        severity=ErrorSeverity.WARNING.value,
        auto_resolve=False,
        requires_approval=True
    )
    
    # System Issues (SYS-xxx)
    SYS_001 = ErrorDefinition(
        code="SYS-001",
        category="SYS",
        name="Database Connection Error",
        description="Unable to connect to database",
        severity=ErrorSeverity.CRITICAL.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    SYS_002 = ErrorDefinition(
        code="SYS-002",
        category="SYS",
        name="File Access Error",
        description="Unable to read or write file",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    SYS_003 = ErrorDefinition(
        code="SYS-003",
        category="SYS",
        name="Memory Limit Exceeded",
        description="Processing exceeded available memory limits",
        severity=ErrorSeverity.CRITICAL.value,
        auto_resolve=False,
        requires_approval=False
    )
    
    SYS_004 = ErrorDefinition(
        code="SYS-004",
        category="SYS",
        name="Processing Timeout",
        description="Operation timed out during processing",
        severity=ErrorSeverity.ERROR.value,
        auto_resolve=False,
        requires_approval=False
    )


class ErrorCodeRegistry:
    """Registry for looking up error codes and their definitions."""
    
    def __init__(self):
        self._codes: Dict[str, ErrorDefinition] = {}
        self._load_error_codes()
    
    def _load_error_codes(self):
        """Load all error code definitions from ErrorCodes class."""
        for attr_name in dir(ErrorCodes):
            if not attr_name.startswith('_'):
                attr_value = getattr(ErrorCodes, attr_name)
                if isinstance(attr_value, ErrorDefinition):
                    self._codes[attr_value.code] = attr_value
    
    def get_error_definition(self, error_code: str) -> Optional[ErrorDefinition]:
        """Get error definition by code."""
        return self._codes.get(error_code)
    
    def get_errors_by_category(self, category: str) -> Dict[str, ErrorDefinition]:
        """Get all errors in a specific category."""
        return {code: defn for code, defn in self._codes.items() 
                if defn.category == category}
    
    def get_errors_by_severity(self, severity: str) -> Dict[str, ErrorDefinition]:
        """Get all errors of a specific severity."""
        return {code: defn for code, defn in self._codes.items() 
                if defn.severity == severity}
    
    def get_auto_resolvable_errors(self) -> Dict[str, ErrorDefinition]:
        """Get all errors that can be auto-resolved."""
        return {code: defn for code, defn in self._codes.items() 
                if defn.auto_resolve}
    
    def get_approval_required_errors(self) -> Dict[str, ErrorDefinition]:
        """Get all errors that require approval."""
        return {code: defn for code, defn in self._codes.items() 
                if defn.requires_approval}
    
    def list_all_codes(self) -> Dict[str, ErrorDefinition]:
        """Get all error codes."""
        return self._codes.copy()


# Global registry instance
ERROR_REGISTRY = ErrorCodeRegistry()


def get_error_definition(error_code: str) -> Optional[ErrorDefinition]:
    """Convenience function to get error definition."""
    return ERROR_REGISTRY.get_error_definition(error_code)


def create_exception_record(
    error_code: str,
    record_type: str,
    record_id: Optional[str] = None,
    error_details: Optional[Dict[str, Any]] = None,
    custom_message: Optional[str] = None
) -> Dict[str, Any]:
    """Create a standardized exception record."""
    
    error_def = get_error_definition(error_code)
    if not error_def:
        raise ValueError(f"Unknown error code: {error_code}")
    
    return {
        'record_type': record_type,
        'record_id': record_id,
        'error_code': error_code,
        'error_message': custom_message or error_def.description,
        'error_details': error_details or {},
        'severity': error_def.severity
    }


def should_auto_resolve(error_code: str) -> bool:
    """Check if error code can be auto-resolved."""
    error_def = get_error_definition(error_code)
    return error_def.auto_resolve if error_def else False


def requires_approval(error_code: str) -> bool:
    """Check if error code requires human approval."""
    error_def = get_error_definition(error_code)
    return error_def.requires_approval if error_def else False
