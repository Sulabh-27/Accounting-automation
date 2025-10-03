-- Part-2: Item & Ledger Master Mapping Database Schema
-- Run this in your Supabase SQL Editor

-- Item Master table for SKU/ASIN to Tally FG mapping
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

-- Ledger Master table for channel + state to ledger mapping
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

-- Approvals table for pending human approvals
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
CREATE INDEX IF NOT EXISTS idx_item_master_sku ON public.item_master(sku);
CREATE INDEX IF NOT EXISTS idx_item_master_asin ON public.item_master(asin);
CREATE INDEX IF NOT EXISTS idx_ledger_master_channel_state ON public.ledger_master(channel, state_code);
CREATE INDEX IF NOT EXISTS idx_approvals_status ON public.approvals(status);
CREATE INDEX IF NOT EXISTS idx_approvals_type ON public.approvals(type);

-- Insert some sample data (optional)
INSERT INTO public.item_master (sku, asin, item_code, fg, gst_rate, approved_by, approved_at) VALUES
('LLQ-LAV-3L-FBA', 'B0CZXQMSR5', 'LLQ001', 'Liquid Lavender 3L', 0.18, 'system', now()),
('FABCON-5L-FBA', 'B09MZ2LBXB', 'FAB001', 'Fabric Conditioner 5L', 0.18, 'system', now()),
('DW-5L', 'B09P7P7P32', 'DW001', 'Dishwash Liquid 5L', 0.18, 'system', now())
ON CONFLICT (sku, asin) DO NOTHING;

INSERT INTO public.ledger_master (channel, state_code, ledger_name, approved_by, approved_at) VALUES
('amazon', 'ANDHRA PRADESH', 'Amazon Sales - AP', 'system', now()),
('amazon', 'KARNATAKA', 'Amazon Sales - KA', 'system', now()),
('amazon', 'DELHI', 'Amazon Sales - DL', 'system', now()),
('amazon', 'MAHARASHTRA', 'Amazon Sales - MH', 'system', now()),
('flipkart', 'ANDHRA PRADESH', 'Flipkart Sales - AP', 'system', now()),
('flipkart', 'KARNATAKA', 'Flipkart Sales - KA', 'system', now())
ON CONFLICT (channel, state_code) DO NOTHING;
