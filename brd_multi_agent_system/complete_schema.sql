-- Complete Database Schema for Multi-Agent Accounting System
-- Copy and paste this entire content into Supabase SQL Editor

-- Part-1: Core tables for runs and reports
CREATE TABLE IF NOT EXISTS public.runs (
    run_id uuid PRIMARY KEY,
    channel text,
    gstin text,
    month text,
    status text,
    started_at timestamp,
    finished_at timestamp
);

CREATE TABLE IF NOT EXISTS public.reports (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE SET NULL,
    report_type text,
    file_path text,
    hash text,
    created_at timestamp
);

-- Part-2: Item Master table for SKU/ASIN to Tally FG mapping
CREATE TABLE IF NOT EXISTS public.item_master (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sku text,
    asin text,
    item_code text,
    fg text,             -- Final Goods (Tally name)
    gst_rate numeric,
    created_at timestamp DEFAULT now(),
    approved_by text,
    approved_at timestamp,
    UNIQUE(sku, asin)
);

-- Part-2: Ledger Master table for channel + state to ledger mapping
CREATE TABLE IF NOT EXISTS public.ledger_master (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel text,
    state_code text,
    ledger_name text,
    created_at timestamp DEFAULT now(),
    approved_by text,
    approved_at timestamp,
    UNIQUE(channel, state_code)
);

-- Part-2: Approvals table for pending human approvals
CREATE TABLE IF NOT EXISTS public.approvals (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type text,            -- 'item' or 'ledger'
    payload jsonb,
    status text DEFAULT 'pending',
    approver text,
    decided_at timestamp,
    created_at timestamp DEFAULT now()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_runs_channel ON public.runs(channel);
CREATE INDEX IF NOT EXISTS idx_runs_gstin ON public.runs(gstin);
CREATE INDEX IF NOT EXISTS idx_reports_run_id ON public.reports(run_id);
CREATE INDEX IF NOT EXISTS idx_item_master_sku ON public.item_master(sku);
CREATE INDEX IF NOT EXISTS idx_item_master_asin ON public.item_master(asin);
CREATE INDEX IF NOT EXISTS idx_ledger_master_channel_state ON public.ledger_master(channel, state_code);
CREATE INDEX IF NOT EXISTS idx_approvals_status ON public.approvals(status);
CREATE INDEX IF NOT EXISTS idx_approvals_type ON public.approvals(type);

-- Insert sample master data
INSERT INTO public.item_master (sku, asin, item_code, fg, gst_rate, approved_by, approved_at) VALUES
('LLQ-LAV-3L-FBA', 'B0CZXQMSR5', 'LLQ001', 'Liquid Lavender 3L', 0.18, 'system', now()),
('FABCON-5L-FBA', 'B09MZ2LBXB', 'FAB001', 'Fabric Conditioner 5L', 0.18, 'system', now()),
('DW-5L', 'B09P7P7P32', 'DW001', 'Dishwash Liquid 5L', 0.18, 'system', now()),
('90-X8YV-Q3DM', 'B0CZXQMSR5', 'LLQ002', 'Liquid Lavender 3L (0% GST)', 0.00, 'system', now()),
('FABCON-5L', 'B09MZ2LBXB', 'FAB002', 'Fabric Conditioner 5L (0% GST)', 0.00, 'system', now())
ON CONFLICT (sku, asin) DO NOTHING;

INSERT INTO public.ledger_master (channel, state_code, ledger_name, approved_by, approved_at) VALUES
('amazon', 'ANDHRA PRADESH', 'Amazon Sales - AP', 'system', now()),
('amazon', 'KARNATAKA', 'Amazon Sales - KA', 'system', now()),
('amazon', 'DELHI', 'Amazon Sales - DL', 'system', now()),
('amazon', 'JAMMU & KASHMIR', 'Amazon Sales - JK', 'system', now()),
('amazon', 'TELANGANA', 'Amazon Sales - TG', 'system', now()),
('amazon', 'MAHARASHTRA', 'Amazon Sales - MH', 'system', now()),
('amazon', 'PUNJAB', 'Amazon Sales - PB', 'system', now()),
('amazon', 'WEST BENGAL', 'Amazon Sales - WB', 'system', now()),
('amazon', 'UTTAR PRADESH', 'Amazon Sales - UP', 'system', now()),
('amazon', 'RAJASTHAN', 'Amazon Sales - RJ', 'system', now()),
('flipkart', 'ANDHRA PRADESH', 'Flipkart Sales - AP', 'system', now()),
('flipkart', 'KARNATAKA', 'Flipkart Sales - KA', 'system', now()),
('flipkart', 'DELHI', 'Flipkart Sales - DL', 'system', now()),
('pepperfry', 'ANDHRA PRADESH', 'Pepperfry Sales - AP', 'system', now()),
('pepperfry', 'KARNATAKA', 'Pepperfry Sales - KA', 'system', now())
ON CONFLICT (channel, state_code) DO NOTHING;

-- Verify the setup
SELECT 'Tables created successfully!' as status;
SELECT 'item_master' as table_name, count(*) as sample_records FROM public.item_master
UNION ALL
SELECT 'ledger_master' as table_name, count(*) as sample_records FROM public.ledger_master;
