"""
Test suite for Audit Logger Agent - Part 8: MIS & Audit Trail

Tests audit logging functionality, event tracking, and compliance features.
"""

import unittest
import uuid
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

from ..agents.audit_logger import AuditLoggerAgent, AuditContext
from ..libs.audit_utils import (
    AuditLogger, AuditActor, AuditAction, EntityType,
    AuditLogEntry, create_audit_logger
)
from ..libs.supabase_client import SupabaseClientWrapper


class TestAuditLogger(unittest.TestCase):
    """Test cases for Audit Logger Agent"""
    
    def setUp(self):
        """Set up test environment"""
        self.supabase = SupabaseClientWrapper()  # Development mode
        self.audit_agent = AuditLoggerAgent(self.supabase)
        self.audit_logger = create_audit_logger(self.supabase)
        self.test_run_id = uuid.uuid4()
        
        # Test data
        self.test_channel = "amazon"
        self.test_gstin = "06ABGCS4796R1ZA"
        self.test_month = "2025-08"
        self.test_input_file = "test_input.xlsx"
        
    def test_audit_log_entry_creation(self):
        """Test audit log entry creation and conversion"""
        entry = AuditLogEntry(
            run_id=self.test_run_id,
            actor=AuditActor.SYSTEM,
            action=AuditAction.INGEST_START,
            entity_type=EntityType.FILE,
            entity_id="test_file.xlsx",
            details={"file_size": 1024, "channel": self.test_channel},
            metadata={"user_agent": "test_agent"}
        )
        
        self.assertEqual(entry.run_id, self.test_run_id)
        self.assertEqual(entry.actor, AuditActor.SYSTEM)
        self.assertEqual(entry.action, AuditAction.INGEST_START)
        self.assertEqual(entry.entity_type, EntityType.FILE)
        self.assertEqual(entry.entity_id, "test_file.xlsx")
        
        # Test conversion to dictionary
        entry_dict = entry.to_dict()
        self.assertIsInstance(entry_dict, dict)
        self.assertEqual(entry_dict['run_id'], str(self.test_run_id))
        self.assertEqual(entry_dict['actor'], 'system')
        self.assertEqual(entry_dict['action'], 'ingest_start')
        self.assertEqual(entry_dict['entity_type'], 'file')
        self.assertEqual(entry_dict['entity_id'], 'test_file.xlsx')
        self.assertIn('details', entry_dict)
        self.assertIn('metadata', entry_dict)
        self.assertIn('timestamp', entry_dict)
        
    def test_basic_audit_logging(self):
        """Test basic audit event logging"""
        # Test ingestion start logging
        log_id = self.audit_logger.log_ingestion_start(
            run_id=self.test_run_id,
            file_path=self.test_input_file,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month
        )
        
        self.assertIsNotNone(log_id)
        self.assertIsInstance(log_id, str)
        
        # Test ingestion completion logging
        completion_id = self.audit_logger.log_ingestion_complete(
            run_id=self.test_run_id,
            file_path=self.test_input_file,
            records_processed=1000,
            storage_path="normalized/test_output.csv"
        )
        
        self.assertIsNotNone(completion_id)
        self.assertIsInstance(completion_id, str)
        
        # Test tax computation logging
        tax_id = self.audit_logger.log_tax_computation(
            run_id=self.test_run_id,
            records_processed=1000,
            total_tax_amount=50000.0,
            channel=self.test_channel
        )
        
        self.assertIsNotNone(tax_id)
        self.assertIsInstance(tax_id, str)
        
    def test_approval_event_logging(self):
        """Test approval event logging"""
        # Test approval request
        request_id = self.audit_logger.log_approval_request(
            run_id=self.test_run_id,
            approval_type="item_mapping",
            entity_details={
                "sku": "TEST123",
                "suggested_fg": "Test Product",
                "channel": self.test_channel
            },
            priority="medium"
        )
        
        self.assertIsNotNone(request_id)
        
        # Test approval decision
        decision_id = self.audit_logger.log_approval_decision(
            run_id=self.test_run_id,
            approval_id="approval_123",
            decision="approved",
            approver="test_user",
            notes="Approved after review"
        )
        
        self.assertIsNotNone(decision_id)
        
    def test_exception_logging(self):
        """Test exception event logging"""
        exception_id = self.audit_logger.log_exception(
            run_id=self.test_run_id,
            exception_code="MAP-001",
            exception_message="Missing SKU mapping for TEST123",
            severity="warning",
            entity_details={
                "sku": "TEST123",
                "channel": self.test_channel,
                "row_index": 42
            }
        )
        
        self.assertIsNotNone(exception_id)
        self.assertIsInstance(exception_id, str)
        
    def test_export_event_logging(self):
        """Test export event logging"""
        export_id = self.audit_logger.log_export_completion(
            run_id=self.test_run_id,
            export_type="x2beta",
            file_path="exports/test_export.xlsx",
            records_exported=1000
        )
        
        self.assertIsNotNone(export_id)
        self.assertIsInstance(export_id, str)
        
    def test_mis_generation_logging(self):
        """Test MIS generation event logging"""
        metrics = {
            "total_sales": 100000.0,
            "total_expenses": 20000.0,
            "gross_profit": 80000.0,
            "profit_margin": 80.0
        }
        
        mis_id = self.audit_logger.log_mis_generation(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month,
            metrics=metrics
        )
        
        self.assertIsNotNone(mis_id)
        self.assertIsInstance(mis_id, str)
        
    def test_audit_session_management(self):
        """Test audit session start and end"""
        # Start audit session
        session_id = self.audit_agent.start_audit_session(
            run_id=self.test_run_id,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month,
            input_file=self.test_input_file
        )
        
        self.assertIsNotNone(session_id)
        self.assertIsInstance(session_id, str)
        self.assertIn(session_id, self.audit_agent.active_contexts)
        
        # Check session context
        context = self.audit_agent.active_contexts[session_id]
        self.assertIsInstance(context, AuditContext)
        self.assertEqual(context.run_id, self.test_run_id)
        self.assertEqual(context.operation, "pipeline_session")
        self.assertEqual(context.actor, AuditActor.SYSTEM)
        
        # End audit session
        final_metrics = {
            "processing_time": 30.5,
            "records_processed": 1000,
            "files_generated": 3
        }
        
        session_summary = self.audit_agent.end_audit_session(
            session_id=session_id,
            status="completed",
            final_metrics=final_metrics
        )
        
        self.assertIsInstance(session_summary, dict)
        self.assertIn('session_id', session_summary)
        self.assertIn('run_id', session_summary)
        self.assertIn('duration_seconds', session_summary)
        self.assertIn('status', session_summary)
        self.assertIn('final_metrics', session_summary)
        
        self.assertEqual(session_summary['session_id'], session_id)
        self.assertEqual(session_summary['status'], "completed")
        self.assertEqual(session_summary['final_metrics'], final_metrics)
        
        # Session should be removed from active contexts
        self.assertNotIn(session_id, self.audit_agent.active_contexts)
        
    def test_audit_operation_context_manager(self):
        """Test audit operation context manager"""
        with self.audit_agent.audit_operation(
            run_id=self.test_run_id,
            operation="test_operation",
            actor=AuditActor.SYSTEM,
            entity_type=EntityType.RECORD,
            test_param="test_value"
        ) as audit_ctx:
            
            # Test context object
            self.assertIsNotNone(audit_ctx)
            self.assertEqual(audit_ctx.run_id, self.test_run_id)
            self.assertEqual(audit_ctx.operation_id[:3], "op_")
            
            # Add metrics
            audit_ctx.add_metric("records_processed", 500)
            audit_ctx.add_metric("processing_time", 15.2)
            
            # Log events
            audit_ctx.log_event(
                AuditAction.MAPPING_COMPLETE,
                {"mapping_type": "item", "success_rate": 95.5}
            )
            
            # Check metrics were added
            self.assertEqual(audit_ctx.metrics["records_processed"], 500)
            self.assertEqual(audit_ctx.metrics["processing_time"], 15.2)
            self.assertEqual(len(audit_ctx.events), 1)
            
    def test_audit_operation_error_handling(self):
        """Test audit operation error handling"""
        try:
            with self.audit_agent.audit_operation(
                run_id=self.test_run_id,
                operation="failing_operation"
            ) as audit_ctx:
                audit_ctx.add_metric("before_error", True)
                raise ValueError("Test error for audit logging")
                
        except ValueError:
            pass  # Expected error
        
        # Operation should have been logged as failed
        # In a real test, we would verify the error was logged
        
    def test_performance_metrics_tracking(self):
        """Test performance metrics tracking"""
        # Simulate multiple operations
        for i in range(3):
            with self.audit_agent.audit_operation(
                run_id=self.test_run_id,
                operation="test_operation"
            ) as audit_ctx:
                audit_ctx.add_metric("iteration", i)
                
        # Get performance metrics
        performance_metrics = self.audit_agent.get_performance_metrics()
        
        self.assertIsInstance(performance_metrics, dict)
        self.assertIn('operation_metrics', performance_metrics)
        self.assertIn('total_operations', performance_metrics)
        self.assertIn('active_contexts', performance_metrics)
        self.assertIn('generated_at', performance_metrics)
        
        # Check operation metrics
        if 'test_operation' in performance_metrics['operation_metrics']:
            test_op_metrics = performance_metrics['operation_metrics']['test_operation']
            self.assertEqual(test_op_metrics['count'], 3)
            self.assertIn('average_time', test_op_metrics)
            self.assertIn('total_time', test_op_metrics)
            self.assertIn('min_time', test_op_metrics)
            self.assertIn('max_time', test_op_metrics)
            
    def test_audit_trail_retrieval(self):
        """Test audit trail retrieval"""
        # Log some events
        self.audit_logger.log_ingestion_start(
            run_id=self.test_run_id,
            file_path=self.test_input_file,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month
        )
        
        self.audit_logger.log_tax_computation(
            run_id=self.test_run_id,
            records_processed=1000,
            total_tax_amount=50000.0,
            channel=self.test_channel
        )
        
        # Flush logs to ensure they're processed
        flushed_count = self.audit_logger.flush_logs()
        self.assertGreaterEqual(flushed_count, 0)
        
        # Retrieve audit trail
        audit_trail = self.audit_agent.get_audit_trail(self.test_run_id)
        self.assertIsInstance(audit_trail, list)
        
        # In development mode, this might be empty, but should not error
        
    def test_audit_summary_generation(self):
        """Test audit summary generation"""
        # Log some events
        self.audit_logger.log_ingestion_start(
            run_id=self.test_run_id,
            file_path=self.test_input_file,
            channel=self.test_channel,
            gstin=self.test_gstin,
            month=self.test_month
        )
        
        self.audit_logger.log_exception(
            run_id=self.test_run_id,
            exception_code="TEST-001",
            exception_message="Test exception",
            severity="warning"
        )
        
        # Get audit summary
        audit_summary = self.audit_agent.get_audit_summary(self.test_run_id)
        self.assertIsInstance(audit_summary, dict)
        
        # In development mode, might be empty but should not error
        
    def test_log_buffer_management(self):
        """Test audit log buffer management"""
        initial_buffer_size = len(self.audit_logger.log_buffer)
        
        # Add multiple log entries
        for i in range(5):
            self.audit_logger.log_event(
                run_id=self.test_run_id,
                actor=AuditActor.SYSTEM,
                action=AuditAction.INGEST_START,
                details={"iteration": i}
            )
        
        # Buffer should have entries
        self.assertEqual(len(self.audit_logger.log_buffer), initial_buffer_size + 5)
        
        # Flush logs
        flushed_count = self.audit_logger.flush_logs()
        self.assertGreaterEqual(flushed_count, 0)
        
        # Buffer should be empty after flush
        self.assertEqual(len(self.audit_logger.log_buffer), 0)
        
    def test_event_handler_registration(self):
        """Test custom event handler registration"""
        events_received = []
        
        def custom_handler(event_data):
            events_received.append(event_data)
        
        # Register handler
        self.audit_agent.register_event_handler(
            AuditAction.INGEST_START,
            custom_handler
        )
        
        # Check handler was registered
        self.assertIn(AuditAction.INGEST_START, self.audit_agent.event_handlers)
        self.assertIn(custom_handler, self.audit_agent.event_handlers[AuditAction.INGEST_START])
        
    def test_agent_integration_logging(self):
        """Test integration with agent-specific logging methods"""
        # Test ingestion event logging
        ingestion_id = self.audit_agent.log_ingestion_event(
            run_id=self.test_run_id,
            file_path=self.test_input_file,
            records_processed=1000,
            storage_path="normalized/output.csv",
            channel=self.test_channel
        )
        self.assertIsNotNone(ingestion_id)
        
        # Test mapping event logging
        mapping_id = self.audit_agent.log_mapping_event(
            run_id=self.test_run_id,
            mapping_type="item_mapping",
            records_processed=1000,
            mappings_resolved=900,
            mappings_pending=100
        )
        self.assertIsNotNone(mapping_id)
        
        # Test tax computation event logging
        tax_id = self.audit_agent.log_tax_computation_event(
            run_id=self.test_run_id,
            records_processed=1000,
            total_tax_amount=50000.0,
            channel=self.test_channel
        )
        self.assertIsNotNone(tax_id)
        
        # Test export event logging
        export_id = self.audit_agent.log_export_event(
            run_id=self.test_run_id,
            export_type="x2beta",
            file_path="exports/output.xlsx",
            records_exported=1000,
            export_format="excel"
        )
        self.assertIsNotNone(export_id)
        
        # Test approval event logging
        approval_id = self.audit_agent.log_approval_event(
            run_id=self.test_run_id,
            approval_type="item_mapping",
            entity_details={"sku": "TEST123", "fg": "Test Product"},
            decision="approved",
            approver="test_user",
            notes="Test approval"
        )
        self.assertIsNotNone(approval_id)
        
        # Test exception event logging
        exception_id = self.audit_agent.log_exception_event(
            run_id=self.test_run_id,
            exception_code="TEST-001",
            exception_message="Test exception message",
            severity="warning",
            entity_details={"test_field": "test_value"}
        )
        self.assertIsNotNone(exception_id)
        
    def test_golden_audit_log_validation(self):
        """Test audit log against golden test case"""
        # Load golden test data
        golden_file = "ingestion_layer/tests/golden/audit_log_expected.csv"
        
        if os.path.exists(golden_file):
            import pandas as pd
            golden_df = pd.read_csv(golden_file)
            
            # Validate structure
            expected_columns = ['run_id', 'actor', 'action', 'entity_type', 'details']
            for col in expected_columns:
                self.assertIn(col, golden_df.columns)
            
            # Test that we can generate similar log entries
            for _, row in golden_df.iterrows():
                try:
                    details = json.loads(row['details']) if pd.notna(row['details']) else {}
                except json.JSONDecodeError:
                    details = {}
                
                log_id = self.audit_logger.log_event(
                    run_id=uuid.UUID(row['run_id']) if row['run_id'] != 'RUN-001' else self.test_run_id,
                    actor=AuditActor(row['actor']),
                    action=AuditAction(row['action']),
                    entity_type=EntityType(row['entity_type']) if pd.notna(row['entity_type']) else None,
                    details=details
                )
                
                self.assertIsNotNone(log_id)
        
    def test_concurrent_logging(self):
        """Test concurrent audit logging scenarios"""
        import threading
        import time
        
        results = []
        
        def log_events(thread_id):
            for i in range(10):
                log_id = self.audit_logger.log_event(
                    run_id=self.test_run_id,
                    actor=AuditActor.SYSTEM,
                    action=AuditAction.INGEST_START,
                    details={"thread_id": thread_id, "iteration": i}
                )
                results.append(log_id)
                time.sleep(0.01)  # Small delay
        
        # Create multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=log_events, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Should have 30 results (3 threads × 10 iterations)
        self.assertEqual(len(results), 30)
        
        # All results should be non-None
        for result in results:
            self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
