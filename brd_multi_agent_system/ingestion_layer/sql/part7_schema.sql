-- Part 7: Exception Handling & Approval Workflows
-- Database schema for exception tracking and approval management

-- Exceptions table for tracking all system errors and issues
CREATE TABLE IF NOT EXISTS public.exceptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES public.runs(run_id) ON DELETE CASCADE,
    record_type TEXT NOT NULL,        -- 'sales', 'expense', 'mapping', 'validation', 'export'
    record_id TEXT,                   -- Reference to specific record (SKU, invoice_no, etc.)
    error_code TEXT NOT NULL,         -- Standardized error code (MAP-001, GST-003, etc.)
    error_message TEXT NOT NULL,      -- Human-readable error description
    error_details JSONB,              -- Additional error context and data
    severity TEXT DEFAULT 'warning',  -- 'info', 'warning', 'error', 'critical'
    status TEXT DEFAULT 'pending',    -- 'pending', 'resolved', 'ignored', 'escalated'
    resolution_notes TEXT,            -- Notes on how the exception was resolved
    created_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP,
    resolved_by TEXT
);

-- Approval queue for human-in-the-loop decision making
CREATE TABLE IF NOT EXISTS public.approval_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES public.runs(run_id) ON DELETE CASCADE,
    request_type TEXT NOT NULL,       -- 'item_mapping', 'ledger_mapping', 'invoice_override', 'gst_rate_override'
    payload JSONB NOT NULL,           -- Request data and context
    context_data JSONB,               -- Additional context for decision making
    priority TEXT DEFAULT 'medium',   -- 'low', 'medium', 'high', 'critical'
    status TEXT DEFAULT 'pending',    -- 'pending', 'approved', 'rejected', 'escalated'
    approver TEXT,                    -- Who approved/rejected the request
    approval_notes TEXT,              -- Notes from the approver
    auto_approve_eligible BOOLEAN DEFAULT FALSE, -- Can this be auto-approved based on rules
    created_at TIMESTAMP DEFAULT NOW(),
    decided_at TIMESTAMP
);

-- Exception categories for grouping and reporting
CREATE TABLE IF NOT EXISTS public.exception_categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_code TEXT UNIQUE NOT NULL,
    category_name TEXT NOT NULL,
    description TEXT,
    default_severity TEXT DEFAULT 'warning',
    auto_resolve_eligible BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Approval rules for automated decision making
CREATE TABLE IF NOT EXISTS public.approval_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_name TEXT UNIQUE NOT NULL,
    request_type TEXT NOT NULL,
    conditions JSONB NOT NULL,        -- Conditions for auto-approval
    action TEXT NOT NULL,             -- 'approve', 'reject', 'escalate'
    is_active BOOLEAN DEFAULT TRUE,
    created_by TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_exceptions_run_id ON public.exceptions(run_id);
CREATE INDEX IF NOT EXISTS idx_exceptions_status ON public.exceptions(status);
CREATE INDEX IF NOT EXISTS idx_exceptions_error_code ON public.exceptions(error_code);
CREATE INDEX IF NOT EXISTS idx_exceptions_created_at ON public.exceptions(created_at);

CREATE INDEX IF NOT EXISTS idx_approval_queue_run_id ON public.approval_queue(run_id);
CREATE INDEX IF NOT EXISTS idx_approval_queue_status ON public.approval_queue(status);
CREATE INDEX IF NOT EXISTS idx_approval_queue_request_type ON public.approval_queue(request_type);
CREATE INDEX IF NOT EXISTS idx_approval_queue_created_at ON public.approval_queue(created_at);

-- Insert default exception categories
INSERT INTO public.exception_categories (category_code, category_name, description, default_severity, auto_resolve_eligible) VALUES
('MAP', 'Mapping Issues', 'SKU/ASIN to Final Goods mapping problems', 'warning', TRUE),
('LED', 'Ledger Issues', 'Channel and state to ledger mapping problems', 'warning', TRUE),
('GST', 'GST Validation', 'GST rate and tax calculation issues', 'error', FALSE),
('INV', 'Invoice Issues', 'Invoice numbering and validation problems', 'error', FALSE),
('SCH', 'Schema Issues', 'Data schema and validation problems', 'error', FALSE),
('EXP', 'Export Issues', 'X2Beta export and template problems', 'warning', FALSE),
('DAT', 'Data Quality', 'Data quality and integrity issues', 'warning', TRUE),
('SYS', 'System Issues', 'System and processing errors', 'critical', FALSE)
ON CONFLICT (category_code) DO NOTHING;

-- Insert default approval rules
INSERT INTO public.approval_rules (rule_name, request_type, conditions, action, created_by) VALUES
('Auto Approve Similar SKUs', 'item_mapping', '{"similarity_threshold": 0.9, "existing_mappings": true}', 'approve', 'system'),
('Auto Approve Standard Ledgers', 'ledger_mapping', '{"channel": ["amazon", "flipkart"], "standard_pattern": true}', 'approve', 'system'),
('Escalate High Value Items', 'item_mapping', '{"value_threshold": 10000}', 'escalate', 'system'),
('Reject Invalid GST Rates', 'gst_rate_override', '{"gst_rate": {"not_in": [0, 0.05, 0.12, 0.18, 0.28]}}', 'reject', 'system')
ON CONFLICT (rule_name) DO NOTHING;

-- Comments for documentation
COMMENT ON TABLE public.exceptions IS 'Tracks all system exceptions and errors for resolution';
COMMENT ON TABLE public.approval_queue IS 'Queue for human approval of ambiguous or critical decisions';
COMMENT ON TABLE public.exception_categories IS 'Categorization and configuration for exception types';
COMMENT ON TABLE public.approval_rules IS 'Automated rules for approval workflow decisions';

COMMENT ON COLUMN public.exceptions.record_type IS 'Type of record that caused the exception';
COMMENT ON COLUMN public.exceptions.error_code IS 'Standardized error code for categorization';
COMMENT ON COLUMN public.exceptions.severity IS 'Severity level: info, warning, error, critical';
COMMENT ON COLUMN public.approval_queue.request_type IS 'Type of approval request';
COMMENT ON COLUMN public.approval_queue.payload IS 'JSON data containing the approval request details';
COMMENT ON COLUMN public.approval_queue.auto_approve_eligible IS 'Whether this request can be auto-approved by rules';
