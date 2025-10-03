-- Part-3: Tax Engine & Invoice Numbering Database Schema
-- Run this in your Supabase SQL Editor

-- Tax Computations table for storing GST calculations
CREATE TABLE IF NOT EXISTS public.tax_computations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    state_code text NOT NULL,
    sku text,
    taxable_value numeric NOT NULL DEFAULT 0,
    shipping_value numeric NOT NULL DEFAULT 0,
    cgst numeric NOT NULL DEFAULT 0,
    sgst numeric NOT NULL DEFAULT 0,
    igst numeric NOT NULL DEFAULT 0,
    gst_rate numeric NOT NULL DEFAULT 0,
    created_at timestamp DEFAULT now()
);

-- Invoice Registry table for tracking unique invoice numbers
CREATE TABLE IF NOT EXISTS public.invoice_registry (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    state_code text NOT NULL,
    invoice_no text UNIQUE NOT NULL,
    month text NOT NULL,
    created_at timestamp DEFAULT now()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_tax_computations_run_id ON public.tax_computations(run_id);
CREATE INDEX IF NOT EXISTS idx_tax_computations_channel ON public.tax_computations(channel);
CREATE INDEX IF NOT EXISTS idx_tax_computations_gstin ON public.tax_computations(gstin);
CREATE INDEX IF NOT EXISTS idx_tax_computations_state ON public.tax_computations(state_code);
CREATE INDEX IF NOT EXISTS idx_tax_computations_sku ON public.tax_computations(sku);

CREATE INDEX IF NOT EXISTS idx_invoice_registry_run_id ON public.invoice_registry(run_id);
CREATE INDEX IF NOT EXISTS idx_invoice_registry_channel ON public.invoice_registry(channel);
CREATE INDEX IF NOT EXISTS idx_invoice_registry_gstin ON public.invoice_registry(gstin);
CREATE INDEX IF NOT EXISTS idx_invoice_registry_month ON public.invoice_registry(month);
CREATE INDEX IF NOT EXISTS idx_invoice_registry_invoice_no ON public.invoice_registry(invoice_no);

-- Create composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_tax_computations_channel_gstin_state ON public.tax_computations(channel, gstin, state_code);
CREATE INDEX IF NOT EXISTS idx_invoice_registry_channel_gstin_month ON public.invoice_registry(channel, gstin, month);

-- Add constraints for data integrity
ALTER TABLE public.tax_computations 
ADD CONSTRAINT chk_tax_computations_positive_values 
CHECK (taxable_value >= 0 AND shipping_value >= 0 AND cgst >= 0 AND sgst >= 0 AND igst >= 0);

ALTER TABLE public.tax_computations 
ADD CONSTRAINT chk_tax_computations_valid_gst_rate 
CHECK (gst_rate IN (0.00, 0.05, 0.12, 0.18, 0.28));

-- Add constraint to ensure either (CGST+SGST) OR IGST, not both
ALTER TABLE public.tax_computations 
ADD CONSTRAINT chk_tax_computations_gst_logic 
CHECK (
    (cgst > 0 AND sgst > 0 AND igst = 0) OR 
    (cgst = 0 AND sgst = 0 AND igst >= 0)
);

-- Create views for common queries
CREATE OR REPLACE VIEW public.tax_summary_by_run AS
SELECT 
    run_id,
    channel,
    gstin,
    COUNT(*) as total_records,
    SUM(taxable_value) as total_taxable_value,
    SUM(shipping_value) as total_shipping_value,
    SUM(cgst) as total_cgst,
    SUM(sgst) as total_sgst,
    SUM(igst) as total_igst,
    SUM(cgst + sgst + igst) as total_tax,
    SUM(taxable_value + shipping_value + cgst + sgst + igst) as total_amount,
    COUNT(CASE WHEN cgst > 0 THEN 1 END) as intrastate_transactions,
    COUNT(CASE WHEN igst > 0 THEN 1 END) as interstate_transactions
FROM public.tax_computations
GROUP BY run_id, channel, gstin;

CREATE OR REPLACE VIEW public.invoice_summary_by_month AS
SELECT 
    channel,
    gstin,
    month,
    COUNT(*) as total_invoices,
    COUNT(DISTINCT state_code) as unique_states,
    MIN(created_at) as first_invoice_date,
    MAX(created_at) as last_invoice_date
FROM public.invoice_registry
GROUP BY channel, gstin, month;

-- Insert sample data for testing (optional)
-- This will be populated by the actual system during processing

-- Verify the setup
SELECT 'Tax Computations table created successfully!' as status
UNION ALL
SELECT 'Invoice Registry table created successfully!' as status
UNION ALL
SELECT 'Part-3 schema setup complete!' as status;
