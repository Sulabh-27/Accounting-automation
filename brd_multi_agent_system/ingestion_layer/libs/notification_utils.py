"""
Notification Utilities for Part 7: Exception Handling & Approval Workflows
Handles alerts, notifications, and communication for exception management
"""
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


class NotificationType(Enum):
    """Types of notifications."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    APPROVAL_REQUIRED = "approval_required"
    APPROVAL_COMPLETED = "approval_completed"


class NotificationChannel(Enum):
    """Notification delivery channels."""
    CONSOLE = "console"
    EMAIL = "email"
    SLACK = "slack"
    DATABASE = "database"
    FILE = "file"


@dataclass
class NotificationMessage:
    """Structured notification message."""
    type: NotificationType
    title: str
    message: str
    details: Optional[Dict[str, Any]] = None
    recipient: Optional[str] = None
    channel: NotificationChannel = NotificationChannel.CONSOLE
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class NotificationManager:
    """Manages notification delivery across different channels."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_level = self.config.get('log_level', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def send_notification(self, notification: NotificationMessage) -> bool:
        """Send notification through specified channel."""
        try:
            if notification.channel == NotificationChannel.CONSOLE:
                return self._send_console_notification(notification)
            elif notification.channel == NotificationChannel.EMAIL:
                return self._send_email_notification(notification)
            elif notification.channel == NotificationChannel.SLACK:
                return self._send_slack_notification(notification)
            elif notification.channel == NotificationChannel.DATABASE:
                return self._send_database_notification(notification)
            elif notification.channel == NotificationChannel.FILE:
                return self._send_file_notification(notification)
            else:
                self.logger.error(f"Unknown notification channel: {notification.channel}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to send notification: {e}")
            return False
    
    def _send_console_notification(self, notification: NotificationMessage) -> bool:
        """Send notification to console/terminal."""
        
        # Color codes for different notification types
        colors = {
            NotificationType.INFO: '\033[94m',      # Blue
            NotificationType.WARNING: '\033[93m',   # Yellow
            NotificationType.ERROR: '\033[91m',     # Red
            NotificationType.CRITICAL: '\033[95m',  # Magenta
            NotificationType.APPROVAL_REQUIRED: '\033[96m',  # Cyan
            NotificationType.APPROVAL_COMPLETED: '\033[92m'  # Green
        }
        
        reset_color = '\033[0m'
        color = colors.get(notification.type, '')
        
        # Format message
        timestamp_str = notification.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        header = f"{color}[{notification.type.value.upper()}] {timestamp_str}{reset_color}"
        title = f"{color}{notification.title}{reset_color}"
        message = notification.message
        
        print(f"{header}")
        print(f"{title}")
        print(f"{message}")
        
        if notification.details:
            print(f"Details: {json.dumps(notification.details, indent=2)}")
        
        print("-" * 80)
        
        # Also log to logger
        log_level = {
            NotificationType.INFO: logging.INFO,
            NotificationType.WARNING: logging.WARNING,
            NotificationType.ERROR: logging.ERROR,
            NotificationType.CRITICAL: logging.CRITICAL,
            NotificationType.APPROVAL_REQUIRED: logging.WARNING,
            NotificationType.APPROVAL_COMPLETED: logging.INFO
        }.get(notification.type, logging.INFO)
        
        self.logger.log(log_level, f"{notification.title}: {notification.message}")
        
        return True
    
    def _send_email_notification(self, notification: NotificationMessage) -> bool:
        """Send email notification (stub implementation)."""
        # TODO: Implement actual email sending
        self.logger.info(f"EMAIL NOTIFICATION (stub): {notification.title}")
        self.logger.info(f"To: {notification.recipient}")
        self.logger.info(f"Message: {notification.message}")
        return True
    
    def _send_slack_notification(self, notification: NotificationMessage) -> bool:
        """Send Slack notification (stub implementation)."""
        # TODO: Implement actual Slack webhook
        self.logger.info(f"SLACK NOTIFICATION (stub): {notification.title}")
        self.logger.info(f"Channel: {notification.recipient}")
        self.logger.info(f"Message: {notification.message}")
        return True
    
    def _send_database_notification(self, notification: NotificationMessage) -> bool:
        """Store notification in database (stub implementation)."""
        # TODO: Implement database storage
        self.logger.info(f"DATABASE NOTIFICATION (stub): {notification.title}")
        return True
    
    def _send_file_notification(self, notification: NotificationMessage) -> bool:
        """Write notification to file."""
        try:
            log_file = self.config.get('notification_log_file', 'notifications.log')
            
            log_entry = {
                'timestamp': notification.timestamp.isoformat(),
                'type': notification.type.value,
                'title': notification.title,
                'message': notification.message,
                'details': notification.details,
                'recipient': notification.recipient
            }
            
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry) + '\n')
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to write notification to file: {e}")
            return False
    
    def send_exception_alert(
        self,
        error_code: str,
        error_message: str,
        record_type: str,
        record_id: Optional[str] = None,
        severity: str = "warning",
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send alert for system exception."""
        
        notification_type = {
            'info': NotificationType.INFO,
            'warning': NotificationType.WARNING,
            'error': NotificationType.ERROR,
            'critical': NotificationType.CRITICAL
        }.get(severity, NotificationType.WARNING)
        
        title = f"Exception {error_code}: {record_type}"
        if record_id:
            title += f" ({record_id})"
        
        notification = NotificationMessage(
            type=notification_type,
            title=title,
            message=error_message,
            details=details,
            channel=NotificationChannel.CONSOLE
        )
        
        return self.send_notification(notification)
    
    def send_approval_request(
        self,
        request_type: str,
        payload: Dict[str, Any],
        approver: Optional[str] = None,
        priority: str = "medium"
    ) -> bool:
        """Send approval request notification."""
        
        title = f"Approval Required: {request_type}"
        message = f"New {request_type} approval request requires attention"
        
        if priority == "high":
            message += " (HIGH PRIORITY)"
        elif priority == "critical":
            message += " (CRITICAL)"
        
        details = {
            'request_type': request_type,
            'payload': payload,
            'priority': priority,
            'approver': approver
        }
        
        notification = NotificationMessage(
            type=NotificationType.APPROVAL_REQUIRED,
            title=title,
            message=message,
            details=details,
            recipient=approver,
            channel=NotificationChannel.CONSOLE
        )
        
        return self.send_notification(notification)
    
    def send_approval_completion(
        self,
        request_type: str,
        decision: str,
        approver: str,
        notes: Optional[str] = None
    ) -> bool:
        """Send approval completion notification."""
        
        title = f"Approval {decision.title()}: {request_type}"
        message = f"{request_type} request has been {decision} by {approver}"
        
        if notes:
            message += f"\nNotes: {notes}"
        
        details = {
            'request_type': request_type,
            'decision': decision,
            'approver': approver,
            'notes': notes
        }
        
        notification = NotificationMessage(
            type=NotificationType.APPROVAL_COMPLETED,
            title=title,
            message=message,
            details=details,
            channel=NotificationChannel.CONSOLE
        )
        
        return self.send_notification(notification)
    
    def send_batch_summary(
        self,
        total_records: int,
        exceptions_count: int,
        approvals_pending: int,
        processing_time: float
    ) -> bool:
        """Send batch processing summary notification."""
        
        title = "Batch Processing Summary"
        message = f"""
Processing completed:
- Total records processed: {total_records:,}
- Exceptions detected: {exceptions_count}
- Approvals pending: {approvals_pending}
- Processing time: {processing_time:.2f} seconds
        """.strip()
        
        notification_type = NotificationType.INFO
        if exceptions_count > 0 or approvals_pending > 0:
            notification_type = NotificationType.WARNING
        
        details = {
            'total_records': total_records,
            'exceptions_count': exceptions_count,
            'approvals_pending': approvals_pending,
            'processing_time': processing_time
        }
        
        notification = NotificationMessage(
            type=notification_type,
            title=title,
            message=message,
            details=details,
            channel=NotificationChannel.CONSOLE
        )
        
        return self.send_notification(notification)


# Global notification manager instance
_notification_manager = None


def get_notification_manager(config: Optional[Dict[str, Any]] = None) -> NotificationManager:
    """Get global notification manager instance."""
    global _notification_manager
    if _notification_manager is None:
        _notification_manager = NotificationManager(config)
    return _notification_manager


def notify_exception(
    error_code: str,
    error_message: str,
    record_type: str,
    record_id: Optional[str] = None,
    severity: str = "warning",
    details: Optional[Dict[str, Any]] = None
) -> bool:
    """Convenience function to send exception notification."""
    manager = get_notification_manager()
    return manager.send_exception_alert(
        error_code=error_code,
        error_message=error_message,
        record_type=record_type,
        record_id=record_id,
        severity=severity,
        details=details
    )


def notify_approval_request(
    request_type: str,
    payload: Dict[str, Any],
    approver: Optional[str] = None,
    priority: str = "medium"
) -> bool:
    """Convenience function to send approval request notification."""
    manager = get_notification_manager()
    return manager.send_approval_request(
        request_type=request_type,
        payload=payload,
        approver=approver,
        priority=priority
    )


def notify_approval_completion(
    request_type: str,
    decision: str,
    approver: str,
    notes: Optional[str] = None
) -> bool:
    """Convenience function to send approval completion notification."""
    manager = get_notification_manager()
    return manager.send_approval_completion(
        request_type=request_type,
        decision=decision,
        approver=approver,
        notes=notes
    )


def notify_batch_summary(
    total_records: int,
    exceptions_count: int,
    approvals_pending: int,
    processing_time: float
) -> bool:
    """Convenience function to send batch processing summary."""
    manager = get_notification_manager()
    return manager.send_batch_summary(
        total_records=total_records,
        exceptions_count=exceptions_count,
        approvals_pending=approvals_pending,
        processing_time=processing_time
    )
