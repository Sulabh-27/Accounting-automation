-- Part-8: MIS & Audit Trail Database Schema
-- Creates tables for management information system reports and audit logging

-- =====================================================
-- AUDIT LOGS TABLE
-- =====================================================
-- Immutable audit trail for all system events
CREATE TABLE IF NOT EXISTS public.audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES public.runs(run_id) ON DELETE CASCADE,
    actor TEXT NOT NULL,              -- 'system', 'user', 'agent', 'finance'
    action TEXT NOT NULL,             -- 'ingest', 'approve', 'export', 'error', 'tax_compute', 'mapping'
    entity_type TEXT,                 -- 'file', 'record', 'approval', 'export', 'exception'
    entity_id TEXT,                   -- reference to specific entity (file name, record id, etc.)
    details JSONB,                    -- structured event details
    metadata JSONB,                   -- additional context (user agent, IP, etc.)
    timestamp TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for audit logs performance
CREATE INDEX IF NOT EXISTS idx_audit_logs_run_id ON public.audit_logs(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_actor ON public.audit_logs(actor);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON public.audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp ON public.audit_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON public.audit_logs(entity_type, entity_id);

-- =====================================================
-- MIS REPORTS TABLE
-- =====================================================
-- Management Information System reports for business intelligence
CREATE TABLE IF NOT EXISTS public.mis_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES public.runs(run_id) ON DELETE CASCADE,
    channel TEXT NOT NULL,            -- 'amazon', 'flipkart', 'pepperfry'
    gstin TEXT NOT NULL,              -- company GSTIN
    month TEXT NOT NULL,              -- 'YYYY-MM' format
    report_type TEXT DEFAULT 'monthly', -- 'monthly', 'quarterly', 'annual'
    
    -- Sales Metrics
    total_sales NUMERIC(15,2) DEFAULT 0,
    total_returns NUMERIC(15,2) DEFAULT 0,
    total_transfers NUMERIC(15,2) DEFAULT 0,
    net_sales NUMERIC(15,2) DEFAULT 0,
    
    -- Expense Metrics
    total_expenses NUMERIC(15,2) DEFAULT 0,
    commission_expenses NUMERIC(15,2) DEFAULT 0,
    shipping_expenses NUMERIC(15,2) DEFAULT 0,
    fulfillment_expenses NUMERIC(15,2) DEFAULT 0,
    advertising_expenses NUMERIC(15,2) DEFAULT 0,
    other_expenses NUMERIC(15,2) DEFAULT 0,
    
    -- GST Metrics
    net_gst_output NUMERIC(15,2) DEFAULT 0,    -- GST collected on sales
    net_gst_input NUMERIC(15,2) DEFAULT 0,     -- GST paid on expenses
    gst_liability NUMERIC(15,2) DEFAULT 0,     -- net GST payable
    
    -- Volume Metrics
    total_transactions INTEGER DEFAULT 0,
    total_skus INTEGER DEFAULT 0,
    total_quantity INTEGER DEFAULT 0,
    
    -- Profitability Metrics
    gross_profit NUMERIC(15,2) DEFAULT 0,     -- sales - expenses
    profit_margin NUMERIC(5,2) DEFAULT 0,     -- (gross_profit / sales) * 100
    
    -- Metadata
    processing_status TEXT DEFAULT 'completed',
    data_quality_score NUMERIC(3,2) DEFAULT 100.00,
    exception_count INTEGER DEFAULT 0,
    approval_count INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for MIS reports performance
CREATE INDEX IF NOT EXISTS idx_mis_reports_run_id ON public.mis_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_mis_reports_channel ON public.mis_reports(channel);
CREATE INDEX IF NOT EXISTS idx_mis_reports_gstin ON public.mis_reports(gstin);
CREATE INDEX IF NOT EXISTS idx_mis_reports_month ON public.mis_reports(month);
CREATE INDEX IF NOT EXISTS idx_mis_reports_composite ON public.mis_reports(channel, gstin, month);

-- =====================================================
-- AUDIT EVENT TYPES REFERENCE
-- =====================================================
-- Reference table for standardized audit event types
CREATE TABLE IF NOT EXISTS public.audit_event_types (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_code TEXT UNIQUE NOT NULL,  -- 'INGEST_START', 'TAX_COMPUTE', etc.
    event_name TEXT NOT NULL,
    event_category TEXT NOT NULL,     -- 'ingestion', 'processing', 'export', 'approval'
    description TEXT,
    severity_level TEXT DEFAULT 'info', -- 'info', 'warning', 'error', 'critical'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert standard audit event types
INSERT INTO public.audit_event_types (event_code, event_name, event_category, description, severity_level) VALUES
-- Ingestion Events
('INGEST_START', 'Ingestion Started', 'ingestion', 'Data ingestion process initiated', 'info'),
('INGEST_COMPLETE', 'Ingestion Completed', 'ingestion', 'Data ingestion process completed successfully', 'info'),
('INGEST_ERROR', 'Ingestion Error', 'ingestion', 'Error occurred during data ingestion', 'error'),
('FILE_UPLOADED', 'File Uploaded', 'ingestion', 'File successfully uploaded to storage', 'info'),
('FILE_VALIDATED', 'File Validated', 'ingestion', 'File structure and content validated', 'info'),

-- Processing Events
('MAPPING_START', 'Mapping Started', 'processing', 'Item and ledger mapping initiated', 'info'),
('MAPPING_COMPLETE', 'Mapping Completed', 'processing', 'Mapping process completed', 'info'),
('TAX_COMPUTE_START', 'Tax Computation Started', 'processing', 'GST computation process initiated', 'info'),
('TAX_COMPUTE_COMPLETE', 'Tax Computation Completed', 'processing', 'GST computation completed', 'info'),
('INVOICE_GENERATED', 'Invoice Generated', 'processing', 'Invoice numbers generated', 'info'),
('PIVOT_GENERATED', 'Pivot Generated', 'processing', 'Pivot summaries created', 'info'),
('BATCH_CREATED', 'Batch Created', 'processing', 'GST rate-wise batch files created', 'info'),

-- Export Events
('EXPORT_START', 'Export Started', 'export', 'Data export process initiated', 'info'),
('EXPORT_COMPLETE', 'Export Completed', 'export', 'Export process completed successfully', 'info'),
('EXPORT_ERROR', 'Export Error', 'export', 'Error occurred during export', 'error'),
('TALLY_EXPORT', 'Tally Export', 'export', 'X2Beta files generated for Tally import', 'info'),

-- Approval Events
('APPROVAL_REQUESTED', 'Approval Requested', 'approval', 'Human approval requested', 'warning'),
('APPROVAL_GRANTED', 'Approval Granted', 'approval', 'Approval granted by user', 'info'),
('APPROVAL_REJECTED', 'Approval Rejected', 'approval', 'Approval rejected by user', 'warning'),
('AUTO_APPROVAL', 'Auto Approval', 'approval', 'Automatic approval based on rules', 'info'),

-- Exception Events
('EXCEPTION_DETECTED', 'Exception Detected', 'exception', 'System exception detected', 'warning'),
('EXCEPTION_RESOLVED', 'Exception Resolved', 'exception', 'Exception resolved successfully', 'info'),
('CRITICAL_ERROR', 'Critical Error', 'exception', 'Critical system error occurred', 'critical'),

-- MIS Events
('MIS_GENERATED', 'MIS Report Generated', 'reporting', 'Management information report generated', 'info'),
('AUDIT_LOG_CREATED', 'Audit Log Created', 'audit', 'Audit log entry created', 'info')

ON CONFLICT (event_code) DO NOTHING;

-- =====================================================
-- VIEWS FOR REPORTING
-- =====================================================

-- View for audit trail summary
CREATE OR REPLACE VIEW public.audit_trail_summary AS
SELECT 
    al.run_id,
    r.channel,
    r.gstin,
    r.month,
    COUNT(*) as total_events,
    COUNT(CASE WHEN al.action = 'error' THEN 1 END) as error_count,
    COUNT(CASE WHEN al.action = 'approve' THEN 1 END) as approval_count,
    MIN(al.timestamp) as process_start,
    MAX(al.timestamp) as process_end,
    EXTRACT(EPOCH FROM (MAX(al.timestamp) - MIN(al.timestamp))) as duration_seconds
FROM public.audit_logs al
JOIN public.runs r ON al.run_id = r.run_id
GROUP BY al.run_id, r.channel, r.gstin, r.month;

-- View for MIS dashboard
CREATE OR REPLACE VIEW public.mis_dashboard AS
SELECT 
    mr.channel,
    mr.gstin,
    mr.month,
    mr.total_sales,
    mr.total_expenses,
    mr.gross_profit,
    mr.profit_margin,
    mr.net_gst_output,
    mr.net_gst_input,
    mr.gst_liability,
    mr.total_transactions,
    mr.exception_count,
    mr.data_quality_score,
    mr.created_at
FROM public.mis_reports mr
ORDER BY mr.created_at DESC;

-- =====================================================
-- FUNCTIONS FOR AUDIT LOGGING
-- =====================================================

-- Function to log audit events
CREATE OR REPLACE FUNCTION public.log_audit_event(
    p_run_id UUID,
    p_actor TEXT,
    p_action TEXT,
    p_entity_type TEXT DEFAULT NULL,
    p_entity_id TEXT DEFAULT NULL,
    p_details JSONB DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    audit_id UUID;
BEGIN
    INSERT INTO public.audit_logs (
        run_id, actor, action, entity_type, entity_id, details, metadata
    ) VALUES (
        p_run_id, p_actor, p_action, p_entity_type, p_entity_id, p_details, p_metadata
    ) RETURNING id INTO audit_id;
    
    RETURN audit_id;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate MIS metrics
CREATE OR REPLACE FUNCTION public.calculate_mis_metrics(
    p_run_id UUID,
    p_channel TEXT,
    p_gstin TEXT,
    p_month TEXT
) RETURNS UUID AS $$
DECLARE
    mis_id UUID;
    sales_total NUMERIC := 0;
    expenses_total NUMERIC := 0;
    gst_output NUMERIC := 0;
    gst_input NUMERIC := 0;
    transaction_count INTEGER := 0;
BEGIN
    -- Calculate sales metrics from pivot summaries
    SELECT 
        COALESCE(SUM(total_taxable_value), 0),
        COALESCE(SUM(total_gst_amount), 0),
        COALESCE(SUM(total_records), 0)
    INTO sales_total, gst_output, transaction_count
    FROM public.pivot_summaries 
    WHERE run_id = p_run_id;
    
    -- Calculate expense metrics from seller invoices
    SELECT 
        COALESCE(SUM(total_amount), 0),
        COALESCE(SUM(gst_amount), 0)
    INTO expenses_total, gst_input
    FROM public.seller_invoices 
    WHERE run_id = p_run_id;
    
    -- Insert MIS report
    INSERT INTO public.mis_reports (
        run_id, channel, gstin, month,
        total_sales, total_expenses, net_sales, gross_profit,
        net_gst_output, net_gst_input, gst_liability,
        total_transactions, profit_margin
    ) VALUES (
        p_run_id, p_channel, p_gstin, p_month,
        sales_total, expenses_total, sales_total, sales_total - expenses_total,
        gst_output, gst_input, gst_output - gst_input,
        transaction_count, 
        CASE WHEN sales_total > 0 THEN ((sales_total - expenses_total) / sales_total) * 100 ELSE 0 END
    ) RETURNING id INTO mis_id;
    
    RETURN mis_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- TRIGGERS FOR AUDIT LOGGING
-- =====================================================

-- Trigger function for automatic audit logging
CREATE OR REPLACE FUNCTION public.trigger_audit_log() RETURNS TRIGGER AS $$
BEGIN
    -- Log the operation
    PERFORM public.log_audit_event(
        COALESCE(NEW.run_id, OLD.run_id),
        'system',
        CASE TG_OP 
            WHEN 'INSERT' THEN 'create'
            WHEN 'UPDATE' THEN 'update'
            WHEN 'DELETE' THEN 'delete'
        END,
        TG_TABLE_NAME,
        COALESCE(NEW.id::TEXT, OLD.id::TEXT),
        jsonb_build_object(
            'operation', TG_OP,
            'table', TG_TABLE_NAME,
            'timestamp', NOW()
        )
    );
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Apply audit triggers to key tables (optional - can be enabled for full audit trail)
-- CREATE TRIGGER audit_runs AFTER INSERT OR UPDATE OR DELETE ON public.runs 
--     FOR EACH ROW EXECUTE FUNCTION public.trigger_audit_log();

-- =====================================================
-- PERMISSIONS AND SECURITY
-- =====================================================

-- Grant appropriate permissions
GRANT SELECT, INSERT ON public.audit_logs TO authenticated;
GRANT SELECT, INSERT, UPDATE ON public.mis_reports TO authenticated;
GRANT SELECT ON public.audit_event_types TO authenticated;
GRANT SELECT ON public.audit_trail_summary TO authenticated;
GRANT SELECT ON public.mis_dashboard TO authenticated;
GRANT EXECUTE ON FUNCTION public.log_audit_event TO authenticated;
GRANT EXECUTE ON FUNCTION public.calculate_mis_metrics TO authenticated;

-- Create row level security policies if needed
-- ALTER TABLE public.audit_logs ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE public.mis_reports ENABLE ROW LEVEL SECURITY;

COMMENT ON TABLE public.audit_logs IS 'Immutable audit trail for all system events and user actions';
COMMENT ON TABLE public.mis_reports IS 'Management Information System reports for business intelligence';
COMMENT ON TABLE public.audit_event_types IS 'Reference table for standardized audit event types';
COMMENT ON VIEW public.audit_trail_summary IS 'Summary view of audit events per processing run';
COMMENT ON VIEW public.mis_dashboard IS 'Dashboard view for MIS reporting and analytics';
COMMENT ON FUNCTION public.log_audit_event IS 'Function to create standardized audit log entries';
COMMENT ON FUNCTION public.calculate_mis_metrics IS 'Function to calculate and store MIS metrics for a processing run';
