"""
Sample data for Streamlit dashboard
Based on real system outputs and metrics
"""

import pandas as pd
from datetime import datetime, timedelta
import random

def get_dashboard_metrics():
    """Get key dashboard metrics"""
    return {
        "total_transactions": 698,
        "value_processed": 310901.85,
        "success_rate": 98.2,
        "exception_count": 23
    }

def get_recent_activity():
    """Get recent activity timeline"""
    return [
        {
            "icon": "📤",
            "title": "Amazon MTR file processed",
            "time": "2 minutes ago"
        },
        {
            "icon": "✅",
            "title": "X2Beta export completed",
            "time": "5 minutes ago"
        },
        {
            "icon": "📊",
            "title": "MIS report generated",
            "time": "8 minutes ago"
        },
        {
            "icon": "⚠️",
            "title": "3 exceptions resolved",
            "time": "12 minutes ago"
        },
        {
            "icon": "🔍",
            "title": "Audit trail updated",
            "time": "15 minutes ago"
        }
    ]

def get_processing_status():
    """Get current processing status"""
    return {
        "current_file": "Amazon MTR B2C Report - Sample.xlsx",
        "progress": 0.75,
        "step": "Tally Export",
        "estimated_completion": "2 minutes",
        "records_processed": 698,
        "errors": 0,
        "warnings": 3
    }

def get_exception_data():
    """Get exception statistics"""
    return {
        "categories": {
            "MAP": {"count": 8, "severity": "warning", "description": "Mapping issues"},
            "LED": {"count": 5, "severity": "warning", "description": "Ledger mapping"},
            "GST": {"count": 3, "severity": "error", "description": "Tax computation"},
            "INV": {"count": 2, "severity": "info", "description": "Invoice numbering"},
            "SCH": {"count": 1, "severity": "critical", "description": "Schema validation"},
            "EXP": {"count": 2, "severity": "warning", "description": "Export issues"},
            "DAT": {"count": 1, "severity": "error", "description": "Data quality"},
            "SYS": {"count": 1, "severity": "info", "description": "System errors"}
        },
        "recent_exceptions": [
            {
                "id": "EXC-001",
                "category": "MAP",
                "severity": "warning",
                "message": "SKU 'NEW-PRODUCT-123' not found in item master",
                "timestamp": datetime.now() - timedelta(minutes=5),
                "status": "pending"
            },
            {
                "id": "EXC-002", 
                "category": "GST",
                "severity": "error",
                "message": "Invalid GST rate 28% for category 'Electronics'",
                "timestamp": datetime.now() - timedelta(minutes=12),
                "status": "resolved"
            },
            {
                "id": "EXC-003",
                "category": "LED",
                "severity": "warning", 
                "message": "State 'ANDAMAN & NICOBAR' not mapped to ledger",
                "timestamp": datetime.now() - timedelta(minutes=18),
                "status": "pending"
            }
        ]
    }

def get_mis_data():
    """Get MIS report data"""
    return {
        "sales_metrics": {
            "total_sales": 450000.00,
            "total_returns": 15000.00,
            "net_sales": 435000.00,
            "total_transactions": 698,
            "total_skus": 53,
            "average_order_value": 623.21
        },
        "expense_metrics": {
            "total_expenses": 125000.00,
            "commission_expenses": 45000.00,
            "shipping_expenses": 35000.00,
            "fulfillment_expenses": 25000.00,
            "advertising_expenses": 20000.00
        },
        "gst_metrics": {
            "gst_output": 78300.00,
            "gst_input": 22500.00,
            "gst_liability": 55800.00,
            "cgst": 25000.00,
            "sgst": 25000.00,
            "igst": 28300.00
        },
        "profitability_metrics": {
            "gross_profit": 310000.00,
            "profit_margin": 71.26,
            "revenue_per_transaction": 623.21,
            "cost_per_transaction": 179.08
        },
        "quality_metrics": {
            "data_quality_score": 96.8,
            "exception_count": 23,
            "approval_count": 5,
            "processing_time": 28.5
        }
    }

def get_audit_logs():
    """Get audit log entries"""
    return [
        {
            "timestamp": datetime.now() - timedelta(minutes=2),
            "event_type": "TALLY_EXPORT",
            "actor": "system",
            "action": "export_completed",
            "entity": "x2beta_file",
            "details": "Generated X2Beta file for GSTIN 06ABGCS4796R1ZA",
            "metadata": {"file_size": "5.2KB", "records": 698}
        },
        {
            "timestamp": datetime.now() - timedelta(minutes=5),
            "event_type": "MIS_GENERATED", 
            "actor": "system",
            "action": "report_created",
            "entity": "mis_report",
            "details": "Monthly MIS report generated for August 2025",
            "metadata": {"report_id": "MIS-2025-08-001", "metrics_count": 25}
        },
        {
            "timestamp": datetime.now() - timedelta(minutes=8),
            "event_type": "EXCEPTION_RESOLVED",
            "actor": "admin",
            "action": "exception_approved",
            "entity": "approval_queue",
            "details": "Approved new SKU mapping for 'NEW-PRODUCT-123'",
            "metadata": {"exception_id": "EXC-001", "resolution": "approved"}
        },
        {
            "timestamp": datetime.now() - timedelta(minutes=12),
            "event_type": "PIVOT_COMPLETE",
            "actor": "system", 
            "action": "pivot_generated",
            "entity": "pivot_summary",
            "details": "Pivot summaries created for 2 GST rates",
            "metadata": {"gst_rates": ["0%", "18%"], "total_amount": 450000}
        },
        {
            "timestamp": datetime.now() - timedelta(minutes=15),
            "event_type": "INGEST_COMPLETE",
            "actor": "system",
            "action": "file_processed", 
            "entity": "amazon_mtr",
            "details": "Successfully processed Amazon MTR file",
            "metadata": {"records": 698, "file_size": "762KB"}
        }
    ]

def get_master_data():
    """Get master data statistics"""
    return {
        "item_master": {
            "total_skus": 1247,
            "mapped_skus": 1189,
            "unmapped_skus": 58,
            "mapping_rate": 95.3,
            "last_updated": datetime.now() - timedelta(hours=2)
        },
        "ledger_master": {
            "total_ledgers": 156,
            "active_ledgers": 142,
            "inactive_ledgers": 14,
            "states_covered": 28,
            "channels_covered": 4
        },
        "tax_rates": {
            "gst_rates": [0, 5, 12, 18, 28],
            "active_rates": 3,
            "default_rate": 18,
            "last_updated": datetime.now() - timedelta(days=15)
        }
    }

def get_tally_integration_data():
    """Get Tally integration statistics"""
    return {
        "x2beta_files": [
            {
                "filename": "amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_x2beta.xlsx",
                "gstin": "06ABGCS4796R1ZA",
                "gst_rate": "0%",
                "records": 125,
                "amount": 85000.00,
                "created": datetime.now() - timedelta(minutes=5),
                "status": "ready"
            },
            {
                "filename": "amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_x2beta.xlsx", 
                "gstin": "06ABGCS4796R1ZA",
                "gst_rate": "18%",
                "records": 573,
                "amount": 365000.00,
                "created": datetime.now() - timedelta(minutes=5),
                "status": "ready"
            }
        ],
        "templates": {
            "total_templates": 5,
            "active_templates": 5,
            "gstins": ["06ABGCS4796R1ZA", "07ABGCS4796R1Z8", "09ABGCS4796R1Z4", "24ABGCS4796R1ZC", "29ABGCS4796R1Z2"]
        },
        "import_status": {
            "successful_imports": 89,
            "failed_imports": 3,
            "pending_imports": 2,
            "success_rate": 96.7
        }
    }

def get_system_health():
    """Get system health metrics"""
    return {
        "database": {
            "status": "online",
            "connections": 12,
            "query_time": 45,  # ms
            "storage_used": 2.3,  # GB
            "last_backup": datetime.now() - timedelta(hours=6)
        },
        "processing": {
            "cpu_usage": 23.5,  # %
            "memory_usage": 67.2,  # %
            "disk_usage": 45.8,  # %
            "active_jobs": 2,
            "queue_size": 0
        },
        "integrations": {
            "supabase": "online",
            "file_storage": "online", 
            "email_service": "online",
            "audit_logger": "online"
        }
    }

def get_channel_data():
    """Get channel-wise performance data"""
    return {
        "amazon_mtr": {
            "transactions": 450,
            "success_rate": 98.5,
            "avg_processing_time": 25.3,
            "total_value": 285000.00,
            "exceptions": 8
        },
        "amazon_str": {
            "transactions": 123,
            "success_rate": 97.2,
            "avg_processing_time": 18.7,
            "total_value": 78000.00,
            "exceptions": 3
        },
        "flipkart": {
            "transactions": 89,
            "success_rate": 96.8,
            "avg_processing_time": 22.1,
            "total_value": 56000.00,
            "exceptions": 5
        },
        "pepperfry": {
            "transactions": 36,
            "success_rate": 95.1,
            "avg_processing_time": 31.2,
            "total_value": 23000.00,
            "exceptions": 7
        }
    }

def get_sample_transactions():
    """Get sample transaction data"""
    return pd.DataFrame([
        {
            "invoice_date": "2025-08-15",
            "order_id": "408-2877618-7539541",
            "sku": "LLQ-LAV-3L-FBA",
            "asin": "B0CZXQMSR5",
            "quantity": 1,
            "taxable_value": 449.0,
            "gst_rate": 0.18,
            "state_code": "ANDHRA PRADESH",
            "channel": "amazon",
            "gstin": "06ABGCS4796R1ZA"
        },
        {
            "invoice_date": "2025-08-16", 
            "order_id": "406-7575378-4478758",
            "sku": "FABCON-5L-FBA",
            "asin": "B09MZ2LBXB",
            "quantity": 1,
            "taxable_value": 1059.0,
            "gst_rate": 0.18,
            "state_code": "KARNATAKA",
            "channel": "amazon",
            "gstin": "06ABGCS4796R1ZA"
        }
    ])

def get_approval_queue():
    """Get pending approvals"""
    return [
        {
            "id": "APP-001",
            "type": "sku_mapping",
            "priority": "high",
            "description": "New SKU 'PREMIUM-CLEANER-5L' needs Final Goods mapping",
            "context": {
                "sku": "PREMIUM-CLEANER-5L",
                "asin": "B0D4NGD87J",
                "suggested_fg": "Toilet Cleaner",
                "channel": "amazon"
            },
            "created": datetime.now() - timedelta(hours=2),
            "status": "pending"
        },
        {
            "id": "APP-002",
            "type": "ledger_mapping",
            "priority": "medium",
            "description": "State 'LADAKH' needs ledger mapping for Amazon channel",
            "context": {
                "state": "LADAKH",
                "channel": "amazon",
                "suggested_ledger": "Amazon LADAKH"
            },
            "created": datetime.now() - timedelta(hours=4),
            "status": "pending"
        }
    ]
