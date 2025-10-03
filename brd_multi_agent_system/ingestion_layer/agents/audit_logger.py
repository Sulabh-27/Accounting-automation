"""
Audit Logger Agent for Part-8: MIS & Audit Trail

Provides enterprise-grade audit logging with immutable event tracking
across all pipeline operations for compliance and traceability.
"""

import uuid
import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass
from contextlib import contextmanager

from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.audit_utils import (
    AuditLogger, AuditActor, AuditAction, EntityType,
    create_audit_logger, log_pipeline_start, log_pipeline_complete
)


@dataclass
class AuditContext:
    """Audit context for tracking operation scope"""
    run_id: uuid.UUID
    operation: str
    actor: AuditActor
    start_time: datetime
    metadata: Dict[str, Any]


class AuditLoggerAgent:
    """
    Enterprise Audit Logger Agent
    
    Features:
    - Immutable audit trail for all system operations
    - Structured event logging with standardized codes
    - Performance monitoring and timing
    - Context-aware logging with operation scoping
    - Integration with all pipeline stages
    - Compliance-ready audit reports
    - Real-time event streaming and notifications
    """
    
    def __init__(self, supabase_client: Optional[SupabaseClientWrapper] = None):
        self.supabase = supabase_client or SupabaseClientWrapper()
        self.audit_logger = create_audit_logger(self.supabase)
        self.active_contexts: Dict[str, AuditContext] = {}
        
        # Performance tracking
        self.operation_timings: Dict[str, List[float]] = {}
        
        # Event handlers for custom processing
        self.event_handlers: Dict[AuditAction, List[Callable]] = {}
    
    def start_audit_session(
        self,
        run_id: uuid.UUID,
        channel: str,
        gstin: str,
        month: str,
        input_file: str,
        actor: AuditActor = AuditActor.SYSTEM
    ) -> str:
        """
        Start a new audit session for pipeline processing
        
        Args:
            run_id: Processing run identifier
            channel: E-commerce channel
            gstin: Company GSTIN
            month: Processing month
            input_file: Input file path
            actor: Actor initiating the session
            
        Returns:
            Session ID for tracking
        """
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        
        try:
            print(f"🔍 Starting audit session: {session_id}")
            print(f"   📋 Run ID: {run_id}")
            print(f"   🏢 Channel: {channel}")
            print(f"   🏛️  GSTIN: {gstin}")
            print(f"   📅 Month: {month}")
            print(f"   📄 Input File: {input_file}")
            
            # Create audit context
            context = AuditContext(
                run_id=run_id,
                operation="pipeline_session",
                actor=actor,
                start_time=datetime.now(),
                metadata={
                    'session_id': session_id,
                    'channel': channel,
                    'gstin': gstin,
                    'month': month,
                    'input_file': input_file
                }
            )
            
            self.active_contexts[session_id] = context
            
            # Log session start
            log_pipeline_start(
                self.audit_logger,
                run_id=run_id,
                channel=channel,
                gstin=gstin,
                month=month,
                input_file=input_file
            )
            
            # Log detailed session initialization
            self.audit_logger.log_event(
                run_id=run_id,
                actor=actor,
                action=AuditAction.INGEST_START,
                entity_type=EntityType.FILE,
                entity_id=session_id,
                details={
                    'session_id': session_id,
                    'operation': 'audit_session_start',
                    'channel': channel,
                    'gstin': gstin,
                    'month': month,
                    'input_file': input_file,
                    'pipeline_stage': 'initialization'
                },
                metadata={
                    'user_agent': 'audit_logger_agent',
                    'session_start': context.start_time.isoformat()
                }
            )
            
            print(f"✅ Audit session started successfully")
            return session_id
            
        except Exception as e:
            print(f"❌ Failed to start audit session: {e}")
            return session_id
    
    def end_audit_session(
        self,
        session_id: str,
        status: str = "completed",
        final_metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        End an audit session and generate summary
        
        Args:
            session_id: Session identifier
            status: Final status (completed, failed, partial)
            final_metrics: Final processing metrics
            
        Returns:
            Session summary with timing and metrics
        """
        try:
            if session_id not in self.active_contexts:
                print(f"⚠️  Session not found: {session_id}")
                return {'error': 'Session not found'}
            
            context = self.active_contexts[session_id]
            end_time = datetime.now()
            duration = (end_time - context.start_time).total_seconds()
            
            print(f"🔍 Ending audit session: {session_id}")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            print(f"   📊 Status: {status}")
            
            # Log session completion
            log_pipeline_complete(
                self.audit_logger,
                run_id=context.run_id,
                status=status,
                duration_seconds=duration,
                metrics=final_metrics or {}
            )
            
            # Generate session summary
            session_summary = {
                'session_id': session_id,
                'run_id': str(context.run_id),
                'operation': context.operation,
                'actor': context.actor.value,
                'start_time': context.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'status': status,
                'metadata': context.metadata,
                'final_metrics': final_metrics or {}
            }
            
            # Log session end
            self.audit_logger.log_event(
                run_id=context.run_id,
                actor=context.actor,
                action=AuditAction.EXPORT_COMPLETE,
                entity_type=EntityType.FILE,
                entity_id=session_id,
                details={
                    'session_id': session_id,
                    'operation': 'audit_session_end',
                    'status': status,
                    'duration_seconds': duration,
                    'final_metrics': final_metrics or {},
                    'pipeline_stage': 'completion'
                },
                metadata={
                    'session_summary': session_summary
                }
            )
            
            # Flush all pending logs
            self.audit_logger.flush_logs()
            
            # Clean up context
            del self.active_contexts[session_id]
            
            print(f"✅ Audit session ended successfully")
            return session_summary
            
        except Exception as e:
            print(f"❌ Failed to end audit session: {e}")
            return {'error': str(e)}
    
    @contextmanager
    def audit_operation(
        self,
        run_id: uuid.UUID,
        operation: str,
        actor: AuditActor = AuditActor.SYSTEM,
        entity_type: Optional[EntityType] = None,
        entity_id: Optional[str] = None,
        **kwargs
    ):
        """
        Context manager for auditing operations with automatic timing
        
        Args:
            run_id: Processing run identifier
            operation: Operation name
            actor: Actor performing the operation
            entity_type: Type of entity being operated on
            entity_id: Specific entity identifier
            **kwargs: Additional context data
            
        Usage:
            with audit_agent.audit_operation(run_id, "tax_computation") as audit_ctx:
                # Perform tax computation
                result = compute_taxes(data)
                audit_ctx.add_metric("records_processed", len(data))
        """
        operation_id = f"op_{uuid.uuid4().hex[:8]}"
        start_time = datetime.now()
        
        # Create operation context
        operation_context = {
            'operation_id': operation_id,
            'metrics': {},
            'events': [],
            'errors': []
        }
        
        try:
            print(f"🔍 Starting operation audit: {operation} ({operation_id})")
            
            # Log operation start
            self.audit_logger.log_event(
                run_id=run_id,
                actor=actor,
                action=self._get_start_action(operation),
                entity_type=entity_type,
                entity_id=entity_id or operation_id,
                details={
                    'operation': operation,
                    'operation_id': operation_id,
                    'stage': 'start',
                    **kwargs
                }
            )
            
            # Provide context object for operation
            class OperationAuditContext:
                def __init__(self, audit_agent, run_id, operation_id):
                    self.audit_agent = audit_agent
                    self.run_id = run_id
                    self.operation_id = operation_id
                    self.metrics = {}
                    self.events = []
                
                def add_metric(self, key: str, value: Any):
                    """Add a metric to the operation context"""
                    self.metrics[key] = value
                
                def log_event(self, action: AuditAction, details: Dict[str, Any]):
                    """Log an event within the operation"""
                    self.audit_agent.audit_logger.log_event(
                        run_id=self.run_id,
                        actor=actor,
                        action=action,
                        entity_type=entity_type,
                        entity_id=self.operation_id,
                        details={
                            'operation': operation,
                            'operation_id': self.operation_id,
                            **details
                        }
                    )
                    self.events.append({'action': action.value, 'details': details})
                
                def log_error(self, error_message: str, error_details: Optional[Dict[str, Any]] = None):
                    """Log an error within the operation"""
                    self.audit_agent.audit_logger.log_exception(
                        run_id=self.run_id,
                        exception_code=f"OP_{operation.upper()}_ERROR",
                        exception_message=error_message,
                        severity="error",
                        entity_details=error_details or {}
                    )
            
            audit_ctx = OperationAuditContext(self, run_id, operation_id)
            operation_context['audit_ctx'] = audit_ctx
            
            yield audit_ctx
            
            # Operation completed successfully
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Track timing
            if operation not in self.operation_timings:
                self.operation_timings[operation] = []
            self.operation_timings[operation].append(duration)
            
            print(f"✅ Operation completed: {operation} ({duration:.2f}s)")
            
            # Log operation completion
            self.audit_logger.log_event(
                run_id=run_id,
                actor=actor,
                action=self._get_complete_action(operation),
                entity_type=entity_type,
                entity_id=entity_id or operation_id,
                details={
                    'operation': operation,
                    'operation_id': operation_id,
                    'stage': 'complete',
                    'duration_seconds': duration,
                    'metrics': audit_ctx.metrics,
                    'events_count': len(audit_ctx.events),
                    **kwargs
                }
            )
            
        except Exception as e:
            # Operation failed
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_message = str(e)
            
            print(f"❌ Operation failed: {operation} ({duration:.2f}s) - {error_message}")
            
            # Log operation failure
            self.audit_logger.log_event(
                run_id=run_id,
                actor=actor,
                action=AuditAction.CRITICAL_ERROR,
                entity_type=entity_type,
                entity_id=entity_id or operation_id,
                details={
                    'operation': operation,
                    'operation_id': operation_id,
                    'stage': 'error',
                    'duration_seconds': duration,
                    'error_message': error_message,
                    'metrics': operation_context.get('audit_ctx', {}).metrics if 'audit_ctx' in operation_context else {},
                    **kwargs
                }
            )
            
            raise
    
    def log_ingestion_event(
        self,
        run_id: uuid.UUID,
        file_path: str,
        records_processed: int,
        storage_path: str,
        channel: str
    ) -> str:
        """Log file ingestion completion"""
        return self.audit_logger.log_ingestion_complete(
            run_id=run_id,
            file_path=file_path,
            records_processed=records_processed,
            storage_path=storage_path
        )
    
    def log_mapping_event(
        self,
        run_id: uuid.UUID,
        mapping_type: str,
        records_processed: int,
        mappings_resolved: int,
        mappings_pending: int
    ) -> str:
        """Log mapping process completion"""
        return self.audit_logger.log_event(
            run_id=run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.MAPPING_COMPLETE,
            details={
                'mapping_type': mapping_type,
                'records_processed': records_processed,
                'mappings_resolved': mappings_resolved,
                'mappings_pending': mappings_pending,
                'resolution_rate': (mappings_resolved / records_processed * 100) if records_processed > 0 else 0
            }
        )
    
    def log_tax_computation_event(
        self,
        run_id: uuid.UUID,
        records_processed: int,
        total_tax_amount: float,
        channel: str
    ) -> str:
        """Log tax computation completion"""
        return self.audit_logger.log_tax_computation(
            run_id=run_id,
            records_processed=records_processed,
            total_tax_amount=total_tax_amount,
            channel=channel
        )
    
    def log_export_event(
        self,
        run_id: uuid.UUID,
        export_type: str,
        file_path: str,
        records_exported: int,
        export_format: str = "excel"
    ) -> str:
        """Log export completion"""
        return self.audit_logger.log_export_completion(
            run_id=run_id,
            export_type=export_type,
            file_path=file_path,
            records_exported=records_exported
        )
    
    def log_approval_event(
        self,
        run_id: uuid.UUID,
        approval_type: str,
        entity_details: Dict[str, Any],
        decision: str,
        approver: str,
        notes: Optional[str] = None
    ) -> str:
        """Log approval decision"""
        if decision in ['requested', 'pending']:
            return self.audit_logger.log_approval_request(
                run_id=run_id,
                approval_type=approval_type,
                entity_details=entity_details
            )
        else:
            approval_id = entity_details.get('approval_id', str(uuid.uuid4()))
            return self.audit_logger.log_approval_decision(
                run_id=run_id,
                approval_id=approval_id,
                decision=decision,
                approver=approver,
                notes=notes
            )
    
    def log_exception_event(
        self,
        run_id: uuid.UUID,
        exception_code: str,
        exception_message: str,
        severity: str,
        entity_details: Optional[Dict[str, Any]] = None
    ) -> str:
        """Log exception occurrence"""
        return self.audit_logger.log_exception(
            run_id=run_id,
            exception_code=exception_code,
            exception_message=exception_message,
            severity=severity,
            entity_details=entity_details
        )
    
    def get_audit_trail(
        self,
        run_id: uuid.UUID,
        actor_filter: Optional[AuditActor] = None,
        action_filter: Optional[AuditAction] = None
    ) -> List[Dict[str, Any]]:
        """Get complete audit trail for a run"""
        return self.audit_logger.get_audit_trail(
            run_id=run_id,
            actor_filter=actor_filter,
            action_filter=action_filter
        )
    
    def get_audit_summary(self, run_id: uuid.UUID) -> Dict[str, Any]:
        """Get audit summary for a run"""
        return self.audit_logger.get_audit_summary(run_id)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for all operations"""
        try:
            metrics = {}
            
            for operation, timings in self.operation_timings.items():
                if timings:
                    metrics[operation] = {
                        'count': len(timings),
                        'total_time': sum(timings),
                        'average_time': sum(timings) / len(timings),
                        'min_time': min(timings),
                        'max_time': max(timings)
                    }
            
            return {
                'operation_metrics': metrics,
                'total_operations': sum(len(timings) for timings in self.operation_timings.values()),
                'active_contexts': len(self.active_contexts),
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {'error': f'Performance metrics calculation failed: {str(e)}'}
    
    def register_event_handler(
        self,
        action: AuditAction,
        handler: Callable[[Dict[str, Any]], None]
    ):
        """Register a custom event handler for specific audit actions"""
        if action not in self.event_handlers:
            self.event_handlers[action] = []
        self.event_handlers[action].append(handler)
    
    def flush_audit_logs(self) -> int:
        """Flush all pending audit logs to database"""
        return self.audit_logger.flush_logs()
    
    def _get_start_action(self, operation: str) -> AuditAction:
        """Get appropriate start action for operation"""
        action_mapping = {
            'ingestion': AuditAction.INGEST_START,
            'mapping': AuditAction.MAPPING_START,
            'tax_computation': AuditAction.TAX_COMPUTE_START,
            'export': AuditAction.EXPORT_START,
            'mis_generation': AuditAction.MIS_GENERATED
        }
        return action_mapping.get(operation, AuditAction.INGEST_START)
    
    def _get_complete_action(self, operation: str) -> AuditAction:
        """Get appropriate completion action for operation"""
        action_mapping = {
            'ingestion': AuditAction.INGEST_COMPLETE,
            'mapping': AuditAction.MAPPING_COMPLETE,
            'tax_computation': AuditAction.TAX_COMPUTE_COMPLETE,
            'export': AuditAction.EXPORT_COMPLETE,
            'mis_generation': AuditAction.MIS_GENERATED
        }
        return action_mapping.get(operation, AuditAction.INGEST_COMPLETE)


# Convenience functions for common audit operations
def create_audit_session(
    supabase_client: Optional[SupabaseClientWrapper] = None
) -> AuditLoggerAgent:
    """Create a new audit logger agent"""
    return AuditLoggerAgent(supabase_client)


def audit_pipeline_operation(
    audit_agent: AuditLoggerAgent,
    run_id: uuid.UUID,
    operation: str,
    func: Callable,
    *args,
    **kwargs
):
    """Decorator-style function to audit any pipeline operation"""
    with audit_agent.audit_operation(run_id, operation) as audit_ctx:
        result = func(*args, **kwargs)
        
        # Add result metrics if available
        if hasattr(result, '__dict__'):
            for key, value in result.__dict__.items():
                if isinstance(value, (int, float, str, bool)):
                    audit_ctx.add_metric(key, value)
        
        return result
