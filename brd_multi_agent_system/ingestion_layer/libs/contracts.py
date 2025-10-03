from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime
from uuid import UUID, uuid4


class IngestionRequest(BaseModel):
    """Contract for initiating an ingestion for a given channel and report type."""
    run_id: UUID = Field(default_factory=uuid4)
    channel: str
    gstin: str
    month: str  # e.g., "2025-08"
    report_type: str  # e.g., "amazon_mtr", "amazon_str", "flipkart_sales", "pepperfry_sales_returns"
    file_path: str  # local path to input CSV (or folder)


class ValidationResult(BaseModel):
    """Schema validation result returned by the schema validator agent."""

    success: bool
    errors: list[str] = Field(default_factory=list)
    schema_version: str = "1.0"


class ReportMetadata(BaseModel):
    id: UUID
    run_id: UUID
    report_type: str
    file_path: str
    hash: str
    created_at: Optional[str]


# Part-2: Item & Ledger Master Mapping Contracts

class ItemMappingRequest(BaseModel):
    """Request for item master mapping (SKU/ASIN to FG)."""
    sku: str
    asin: Optional[str] = None
    item_code: Optional[str] = None


class LedgerMappingRequest(BaseModel):
    channel: str
    state_code: str


class ApprovalRequest(BaseModel):
    """Request for human approval of mapping."""
    approval_type: str  # 'item' or 'ledger'
    payload: Dict[str, Any]
    suggested_value: str
    confidence_score: Optional[float] = None


# Part-3: Tax Engine & Invoice Numbering Contracts

class TaxComputationRequest(BaseModel):
    """Request for tax computation processing."""
    run_id: UUID
    channel: str
    gstin: str
    dataset_path: Optional[str] = None
    recompute: bool = False


class TaxComputationResult(BaseModel):
    """Result of tax computation processing."""
    success: bool
    processed_records: int = 0
    successful_computations: int = 0
    failed_computations: int = 0
    total_tax_amount: float = 0.0
    total_taxable_amount: float = 0.0
    error_message: Optional[str] = None
    computation_summary: Optional[Dict[str, Any]] = None


class InvoiceNumberingRequest(BaseModel):
    """Request for invoice numbering processing."""
    run_id: UUID
    channel: str
    gstin: str
    dataset_path: Optional[str] = None
    force_regenerate: bool = False


class InvoiceNumberingResult(BaseModel):
    """Result of invoice numbering operation."""
    success: bool
    processed_records: int = 0
    successful_generations: int = 0
    failed_generations: int = 0
    unique_invoice_numbers: int = 0
    error_message: Optional[str] = None


class PivotResult(BaseModel):
    """Result of pivot generation operation."""
    success: bool
    processed_records: int = 0
    pivot_records: int = 0
    total_taxable_amount: float = 0.0
    total_tax_amount: float = 0.0
    unique_ledgers: int = 0
    unique_fgs: int = 0
    gst_rate_breakdown: Dict[str, Any] = {}
    error_message: Optional[str] = None


class BatchSplitResult(BaseModel):
    """Result of batch splitting operation."""
    success: bool
    processed_records: int = 0
    batch_files_created: int = 0
    total_records_split: int = 0
    gst_rates_processed: int = 0
    batch_summaries: List[Dict[str, Any]] = []
    validation_passed: bool = True
    error_message: Optional[str] = None


class TaxComputationRecord(BaseModel):
    """Individual tax computation record."""
    run_id: UUID
    channel: str
    gstin: str
    state_code: str
    sku: str
    taxable_value: float
    shipping_value: float = 0.0
    cgst: float = 0.0
    sgst: float = 0.0
    igst: float = 0.0
    gst_rate: float
    created_at: Optional[datetime] = None


class InvoiceRecord(BaseModel):
    """Individual invoice registry record."""
    run_id: UUID
    channel: str
    gstin: str
    state_code: str
    invoice_no: str
    month: str
    created_at: Optional[datetime] = None


class ItemMasterRecord(BaseModel):
    """Item master record from database."""
    id: UUID
    sku: Optional[str]
    asin: Optional[str]
    item_code: Optional[str]
    fg: str
    gst_rate: float
    approved_by: Optional[str]
    approved_at: Optional[str]


class LedgerMasterRecord(BaseModel):
    """Ledger master record from database."""
    id: UUID
    channel: str
    state_code: str
    ledger_name: str
    approved_by: Optional[str]
    approved_at: Optional[str]


class MappingResult(BaseModel):
    """Result of mapping operation."""
    success: bool
    mapped_count: int
    pending_approvals: int
    errors: list[str] = Field(default_factory=list)


class ProcessingResult(BaseModel):
    """Generic result class for processing operations."""
    success: bool
    processed_records: int = 0
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
