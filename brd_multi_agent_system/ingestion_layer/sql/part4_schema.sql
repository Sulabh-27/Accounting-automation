-- Part-4 Schema: Pivoting & Summarization
-- Tables for pivot summaries and batch registry

-- Pivot Summaries Table
-- Stores aggregated data grouped by key dimensions
CREATE TABLE IF NOT EXISTS public.pivot_summaries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    month text NOT NULL,
    gst_rate numeric NOT NULL DEFAULT 0,
    ledger text NOT NULL,
    fg text NOT NULL,
    total_quantity numeric NOT NULL DEFAULT 0,
    total_taxable numeric NOT NULL DEFAULT 0,
    total_cgst numeric NOT NULL DEFAULT 0,
    total_sgst numeric NOT NULL DEFAULT 0,
    total_igst numeric NOT NULL DEFAULT 0,
    created_at timestamp DEFAULT now()
);

-- Batch Registry Table
-- Tracks batch files created by splitting pivot data by GST rate
CREATE TABLE IF NOT EXISTS public.batch_registry (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    month text NOT NULL,
    gst_rate numeric NOT NULL DEFAULT 0,
    file_path text NOT NULL,
    record_count integer NOT NULL DEFAULT 0,
    total_taxable numeric NOT NULL DEFAULT 0,
    total_tax numeric NOT NULL DEFAULT 0,
    created_at timestamp DEFAULT now()
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_pivot_summaries_run_id ON public.pivot_summaries (run_id);
CREATE INDEX IF NOT EXISTS idx_pivot_summaries_channel_gstin ON public.pivot_summaries (channel, gstin);
CREATE INDEX IF NOT EXISTS idx_pivot_summaries_month_gst_rate ON public.pivot_summaries (month, gst_rate);
CREATE INDEX IF NOT EXISTS idx_pivot_summaries_ledger_fg ON public.pivot_summaries (ledger, fg);

CREATE INDEX IF NOT EXISTS idx_batch_registry_run_id ON public.batch_registry (run_id);
CREATE INDEX IF NOT EXISTS idx_batch_registry_channel_gstin ON public.batch_registry (channel, gstin);
CREATE INDEX IF NOT EXISTS idx_batch_registry_month_gst_rate ON public.batch_registry (month, gst_rate);

-- Row Level Security (RLS) policies
ALTER TABLE public.pivot_summaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.batch_registry ENABLE ROW LEVEL SECURITY;

-- Policy to allow authenticated users to read/write their own data
CREATE POLICY IF NOT EXISTS "Users can manage their pivot summaries" ON public.pivot_summaries
    FOR ALL USING (auth.uid() IS NOT NULL);

CREATE POLICY IF NOT EXISTS "Users can manage their batch registry" ON public.batch_registry
    FOR ALL USING (auth.uid() IS NOT NULL);

-- Comments for documentation
COMMENT ON TABLE public.pivot_summaries IS 'Aggregated transaction data grouped by key accounting dimensions';
COMMENT ON TABLE public.batch_registry IS 'Registry of batch files created by splitting pivot data by GST rate';

COMMENT ON COLUMN public.pivot_summaries.run_id IS 'Reference to the ingestion run that created this pivot';
COMMENT ON COLUMN public.pivot_summaries.channel IS 'E-commerce channel (amazon_mtr, flipkart, etc.)';
COMMENT ON COLUMN public.pivot_summaries.gstin IS 'Company GSTIN';
COMMENT ON COLUMN public.pivot_summaries.month IS 'Processing month in YYYY-MM format';
COMMENT ON COLUMN public.pivot_summaries.gst_rate IS 'GST rate as decimal (0.18 for 18%)';
COMMENT ON COLUMN public.pivot_summaries.ledger IS 'Tally ledger name for accounting';
COMMENT ON COLUMN public.pivot_summaries.fg IS 'Final goods (FG) name';
COMMENT ON COLUMN public.pivot_summaries.total_quantity IS 'Sum of quantities for this group';
COMMENT ON COLUMN public.pivot_summaries.total_taxable IS 'Sum of taxable amounts for this group';
COMMENT ON COLUMN public.pivot_summaries.total_cgst IS 'Sum of CGST amounts for this group';
COMMENT ON COLUMN public.pivot_summaries.total_sgst IS 'Sum of SGST amounts for this group';
COMMENT ON COLUMN public.pivot_summaries.total_igst IS 'Sum of IGST amounts for this group';

COMMENT ON COLUMN public.batch_registry.run_id IS 'Reference to the ingestion run that created this batch';
COMMENT ON COLUMN public.batch_registry.channel IS 'E-commerce channel (amazon_mtr, flipkart, etc.)';
COMMENT ON COLUMN public.batch_registry.gstin IS 'Company GSTIN';
COMMENT ON COLUMN public.batch_registry.month IS 'Processing month in YYYY-MM format';
COMMENT ON COLUMN public.batch_registry.gst_rate IS 'GST rate as decimal (0.18 for 18%)';
COMMENT ON COLUMN public.batch_registry.file_path IS 'Path/filename of the batch file';
COMMENT ON COLUMN public.batch_registry.record_count IS 'Number of records in this batch file';
COMMENT ON COLUMN public.batch_registry.total_taxable IS 'Total taxable amount in this batch';
COMMENT ON COLUMN public.batch_registry.total_tax IS 'Total tax amount in this batch';
