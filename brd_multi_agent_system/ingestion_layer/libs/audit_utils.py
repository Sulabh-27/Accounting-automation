"""
Audit Utilities Library for Part-8: MIS & Audit Trail

Provides structured logging and audit trail functionality for enterprise-grade
compliance and traceability across all pipeline operations.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import dataclass
from enum import Enum

from .supabase_client import SupabaseClientWrapper


class AuditActor(Enum):
    """Standardized audit actors"""
    SYSTEM = "system"
    USER = "user" 
    AGENT = "agent"
    FINANCE = "finance"
    ADMIN = "admin"


class AuditAction(Enum):
    """Standardized audit actions"""
    # Ingestion actions
    INGEST_START = "ingest_start"
    INGEST_COMPLETE = "ingest_complete"
    INGEST_ERROR = "ingest_error"
    FILE_UPLOADED = "file_uploaded"
    FILE_VALIDATED = "file_validated"
    
    # Processing actions
    MAPPING_START = "mapping_start"
    MAPPING_COMPLETE = "mapping_complete"
    TAX_COMPUTE_START = "tax_compute_start"
    TAX_COMPUTE_COMPLETE = "tax_compute_complete"
    INVOICE_GENERATED = "invoice_generated"
    PIVOT_GENERATED = "pivot_generated"
    BATCH_CREATED = "batch_created"
    
    # Export actions
    EXPORT_START = "export_start"
    EXPORT_COMPLETE = "export_complete"
    EXPORT_ERROR = "export_error"
    TALLY_EXPORT = "tally_export"
    
    # Approval actions
    APPROVAL_REQUESTED = "approval_requested"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_REJECTED = "approval_rejected"
    AUTO_APPROVAL = "auto_approval"
    
    # Exception actions
    EXCEPTION_DETECTED = "exception_detected"
    EXCEPTION_RESOLVED = "exception_resolved"
    CRITICAL_ERROR = "critical_error"
    
    # MIS actions
    MIS_GENERATED = "mis_generated"
    AUDIT_LOG_CREATED = "audit_log_created"


class EntityType(Enum):
    """Entity types for audit logging"""
    FILE = "file"
    RECORD = "record"
    APPROVAL = "approval"
    EXPORT = "export"
    EXCEPTION = "exception"
    BATCH = "batch"
    INVOICE = "invoice"
    TAX_COMPUTATION = "tax_computation"
    MIS_REPORT = "mis_report"


@dataclass
class AuditLogEntry:
    """Structured audit log entry"""
    run_id: uuid.UUID
    actor: AuditActor
    action: AuditAction
    entity_type: Optional[EntityType] = None
    entity_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            'run_id': str(self.run_id),
            'actor': self.actor.value,
            'action': self.action.value,
            'entity_type': self.entity_type.value if self.entity_type else None,
            'entity_id': self.entity_id,
            'details': self.details,
            'metadata': self.metadata,
            'timestamp': self.timestamp or datetime.now()
        }


class AuditLogger:
    """
    Enterprise audit logger for immutable event tracking
    
    Features:
    - Structured logging with standardized event types
    - Immutable audit trail storage
    - Performance optimized batch logging
    - Integration with Supabase for persistence
    - Support for custom metadata and context
    """
    
    def __init__(self, supabase_client: Optional[SupabaseClientWrapper] = None):
        self.supabase = supabase_client or SupabaseClientWrapper()
        self.log_buffer: List[AuditLogEntry] = []
        self.buffer_size = 100  # Batch size for performance
        
    def log_event(
        self,
        run_id: uuid.UUID,
        actor: AuditActor,
        action: AuditAction,
        entity_type: Optional[EntityType] = None,
        entity_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        immediate_flush: bool = False
    ) -> str:
        """
        Log an audit event
        
        Args:
            run_id: Processing run identifier
            actor: Who performed the action
            action: What action was performed
            entity_type: Type of entity affected
            entity_id: Specific entity identifier
            details: Structured event details
            metadata: Additional context metadata
            immediate_flush: Force immediate database write
            
        Returns:
            Audit log entry ID
        """
        entry = AuditLogEntry(
            run_id=run_id,
            actor=actor,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details or {},
            metadata=metadata or {},
            timestamp=datetime.now()
        )
        
        self.log_buffer.append(entry)
        
        # Auto-flush if buffer is full or immediate flush requested
        if len(self.log_buffer) >= self.buffer_size or immediate_flush:
            self.flush_logs()
            
        return f"audit_{uuid.uuid4().hex[:8]}"
    
    def log_ingestion_start(
        self,
        run_id: uuid.UUID,
        file_path: str,
        channel: str,
        gstin: str,
        month: str
    ) -> str:
        """Log ingestion process start"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.INGEST_START,
            entity_type=EntityType.FILE,
            entity_id=file_path,
            details={
                'file_path': file_path,
                'channel': channel,
                'gstin': gstin,
                'month': month,
                'process_stage': 'ingestion'
            }
        )
    
    def log_ingestion_complete(
        self,
        run_id: uuid.UUID,
        file_path: str,
        records_processed: int,
        storage_path: str
    ) -> str:
        """Log successful ingestion completion"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.INGEST_COMPLETE,
            entity_type=EntityType.FILE,
            entity_id=file_path,
            details={
                'file_path': file_path,
                'records_processed': records_processed,
                'storage_path': storage_path,
                'process_stage': 'ingestion'
            }
        )
    
    def log_tax_computation(
        self,
        run_id: uuid.UUID,
        records_processed: int,
        total_tax_amount: float,
        channel: str
    ) -> str:
        """Log tax computation completion"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.TAX_COMPUTE_COMPLETE,
            entity_type=EntityType.TAX_COMPUTATION,
            details={
                'records_processed': records_processed,
                'total_tax_amount': total_tax_amount,
                'channel': channel,
                'process_stage': 'tax_computation'
            }
        )
    
    def log_approval_request(
        self,
        run_id: uuid.UUID,
        approval_type: str,
        entity_details: Dict[str, Any],
        priority: str = "medium"
    ) -> str:
        """Log approval request"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.APPROVAL_REQUESTED,
            entity_type=EntityType.APPROVAL,
            details={
                'approval_type': approval_type,
                'entity_details': entity_details,
                'priority': priority,
                'process_stage': 'approval'
            }
        )
    
    def log_approval_decision(
        self,
        run_id: uuid.UUID,
        approval_id: str,
        decision: str,
        approver: str,
        notes: Optional[str] = None
    ) -> str:
        """Log approval decision"""
        action = AuditAction.APPROVAL_GRANTED if decision == 'approved' else AuditAction.APPROVAL_REJECTED
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.FINANCE,
            action=action,
            entity_type=EntityType.APPROVAL,
            entity_id=approval_id,
            details={
                'decision': decision,
                'approver': approver,
                'notes': notes,
                'process_stage': 'approval'
            }
        )
    
    def log_export_completion(
        self,
        run_id: uuid.UUID,
        export_type: str,
        file_path: str,
        records_exported: int
    ) -> str:
        """Log export completion"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.EXPORT_COMPLETE,
            entity_type=EntityType.EXPORT,
            entity_id=file_path,
            details={
                'export_type': export_type,
                'file_path': file_path,
                'records_exported': records_exported,
                'process_stage': 'export'
            }
        )
    
    def log_exception(
        self,
        run_id: uuid.UUID,
        exception_code: str,
        exception_message: str,
        severity: str,
        entity_details: Optional[Dict[str, Any]] = None
    ) -> str:
        """Log exception occurrence"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.EXCEPTION_DETECTED,
            entity_type=EntityType.EXCEPTION,
            entity_id=exception_code,
            details={
                'exception_code': exception_code,
                'exception_message': exception_message,
                'severity': severity,
                'entity_details': entity_details or {},
                'process_stage': 'exception_handling'
            }
        )
    
    def log_mis_generation(
        self,
        run_id: uuid.UUID,
        channel: str,
        gstin: str,
        month: str,
        metrics: Dict[str, Any]
    ) -> str:
        """Log MIS report generation"""
        return self.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.MIS_GENERATED,
            entity_type=EntityType.MIS_REPORT,
            details={
                'channel': channel,
                'gstin': gstin,
                'month': month,
                'metrics': metrics,
                'process_stage': 'mis_generation'
            }
        )
    
    def flush_logs(self) -> int:
        """
        Flush buffered logs to database
        
        Returns:
            Number of logs flushed
        """
        if not self.log_buffer:
            return 0
            
        try:
            # Convert entries to database format
            log_data = [entry.to_dict() for entry in self.log_buffer]
            
            # Batch insert to database
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.table('audit_logs').insert(log_data).execute()
                flushed_count = len(self.log_buffer)
            else:
                # Development mode - log to console
                print(f"🔍 AUDIT LOG: Flushing {len(self.log_buffer)} entries to database")
                for entry in self.log_buffer:
                    print(f"   📝 {entry.action.value}: {entry.details}")
                flushed_count = len(self.log_buffer)
            
            # Clear buffer
            self.log_buffer.clear()
            return flushed_count
            
        except Exception as e:
            print(f"⚠️  Failed to flush audit logs: {e}")
            # Keep logs in buffer for retry
            return 0
    
    def get_audit_trail(
        self,
        run_id: uuid.UUID,
        actor_filter: Optional[AuditActor] = None,
        action_filter: Optional[AuditAction] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve audit trail for a specific run
        
        Args:
            run_id: Processing run identifier
            actor_filter: Filter by specific actor
            action_filter: Filter by specific action
            
        Returns:
            List of audit log entries
        """
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                query = self.supabase.client.table('audit_logs').select('*').eq('run_id', str(run_id))
                
                if actor_filter:
                    query = query.eq('actor', actor_filter.value)
                if action_filter:
                    query = query.eq('action', action_filter.value)
                    
                result = query.order('timestamp').execute()
                return result.data
            else:
                print(f"🔍 AUDIT TRAIL: Retrieving logs for run {run_id}")
                return []
                
        except Exception as e:
            print(f"⚠️  Failed to retrieve audit trail: {e}")
            return []
    
    def get_audit_summary(self, run_id: uuid.UUID) -> Dict[str, Any]:
        """
        Get audit summary for a processing run
        
        Args:
            run_id: Processing run identifier
            
        Returns:
            Summary statistics and timeline
        """
        try:
            if hasattr(self.supabase, 'client') and self.supabase.client:
                result = self.supabase.client.rpc('get_audit_summary', {'p_run_id': str(run_id)}).execute()
                return result.data[0] if result.data else {}
            else:
                print(f"🔍 AUDIT SUMMARY: Generating summary for run {run_id}")
                return {
                    'run_id': str(run_id),
                    'total_events': len(self.log_buffer),
                    'error_count': 0,
                    'approval_count': 0,
                    'duration_seconds': 0
                }
                
        except Exception as e:
            print(f"⚠️  Failed to generate audit summary: {e}")
            return {}
    
    def __del__(self):
        """Ensure logs are flushed on cleanup"""
        if self.log_buffer:
            self.flush_logs()


# Convenience functions for common audit operations
def create_audit_logger(supabase_client: Optional[SupabaseClientWrapper] = None) -> AuditLogger:
    """Create a new audit logger instance"""
    return AuditLogger(supabase_client)


def log_pipeline_start(
    logger: AuditLogger,
    run_id: uuid.UUID,
    channel: str,
    gstin: str,
    month: str,
    input_file: str
) -> str:
    """Log pipeline processing start"""
    return logger.log_event(
        run_id=run_id,
        actor=AuditActor.SYSTEM,
        action=AuditAction.INGEST_START,
        entity_type=EntityType.FILE,
        entity_id=input_file,
        details={
            'pipeline_stage': 'start',
            'channel': channel,
            'gstin': gstin,
            'month': month,
            'input_file': input_file
        }
    )


def log_pipeline_complete(
    logger: AuditLogger,
    run_id: uuid.UUID,
    status: str,
    duration_seconds: float,
    metrics: Dict[str, Any]
) -> str:
    """Log pipeline processing completion"""
    return logger.log_event(
        run_id=run_id,
        actor=AuditActor.SYSTEM,
        action=AuditAction.EXPORT_COMPLETE,
        details={
            'pipeline_stage': 'complete',
            'status': status,
            'duration_seconds': duration_seconds,
            'metrics': metrics
        }
    )
