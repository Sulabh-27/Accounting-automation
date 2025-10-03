-- Part-5: Tally Export Schema
-- Database schema for X2Beta template exports and Tally integration

-- Table to track Tally export files
CREATE TABLE IF NOT EXISTS public.tally_exports (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    month text NOT NULL,
    gst_rate numeric NOT NULL DEFAULT 0,
    template_name text NOT NULL,
    file_path text NOT NULL,
    file_size integer DEFAULT 0,
    record_count integer DEFAULT 0,
    total_taxable numeric DEFAULT 0,
    total_tax numeric DEFAULT 0,
    export_status text DEFAULT 'pending',
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Table to track X2Beta template configurations
CREATE TABLE IF NOT EXISTS public.x2beta_templates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gstin text NOT NULL UNIQUE,
    template_name text NOT NULL,
    template_path text NOT NULL,
    company_name text,
    state_code text,
    is_active boolean DEFAULT true,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Table to track Tally import status (optional for future use)
CREATE TABLE IF NOT EXISTS public.tally_imports (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    export_id uuid REFERENCES public.tally_exports (id) ON DELETE CASCADE,
    import_method text, -- 'webtel', 'manual', 'api'
    import_status text DEFAULT 'pending', -- 'pending', 'success', 'failed'
    import_date timestamp,
    error_message text,
    created_at timestamp DEFAULT now()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tally_exports_run_id ON public.tally_exports (run_id);
CREATE INDEX IF NOT EXISTS idx_tally_exports_gstin ON public.tally_exports (gstin);
CREATE INDEX IF NOT EXISTS idx_tally_exports_channel ON public.tally_exports (channel);
CREATE INDEX IF NOT EXISTS idx_tally_exports_gst_rate ON public.tally_exports (gst_rate);
CREATE INDEX IF NOT EXISTS idx_x2beta_templates_gstin ON public.x2beta_templates (gstin);

-- Insert default X2Beta template configurations
INSERT INTO public.x2beta_templates (gstin, template_name, template_path, company_name, state_code) VALUES
('06ABGCS4796R1ZA', 'X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx', 'ingestion_layer/templates/X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx', 'Zaggle Haryana', 'HARYANA'),
('07ABGCS4796R1Z8', 'X2Beta Sales Template - 07ABGCS4796R1Z8.xlsx', 'ingestion_layer/templates/X2Beta Sales Template - 07ABGCS4796R1Z8.xlsx', 'Zaggle Delhi', 'DELHI'),
('09ABGCS4796R1Z4', 'X2Beta Sales Template - 09ABGCS4796R1Z4.xlsx', 'ingestion_layer/templates/X2Beta Sales Template - 09ABGCS4796R1Z4.xlsx', 'Zaggle Uttar Pradesh', 'UTTAR PRADESH'),
('24ABGCS4796R1ZC', 'X2Beta Sales Template - 24ABGCS4796R1ZC.xlsx', 'ingestion_layer/templates/X2Beta Sales Template - 24ABGCS4796R1ZC.xlsx', 'Zaggle Gujarat', 'GUJARAT'),
('29ABGCS4796R1Z2', 'X2Beta Sales Template - 29ABGCS4796R1Z2.xlsx', 'ingestion_layer/templates/X2Beta Sales Template - 29ABGCS4796R1Z2.xlsx', 'Zaggle Karnataka', 'KARNATAKA')
ON CONFLICT (gstin) DO UPDATE SET
    template_name = EXCLUDED.template_name,
    template_path = EXCLUDED.template_path,
    company_name = EXCLUDED.company_name,
    state_code = EXCLUDED.state_code,
    updated_at = now();

-- Comments for documentation
COMMENT ON TABLE public.tally_exports IS 'Tracks X2Beta export files generated for Tally import';
COMMENT ON TABLE public.x2beta_templates IS 'Configuration for X2Beta templates per GSTIN';
COMMENT ON TABLE public.tally_imports IS 'Tracks import status of X2Beta files into Tally';

COMMENT ON COLUMN public.tally_exports.export_status IS 'Status: pending, success, failed';
COMMENT ON COLUMN public.tally_exports.gst_rate IS 'GST rate for this export file (0.0, 0.05, 0.12, 0.18, 0.28)';
COMMENT ON COLUMN public.tally_exports.file_path IS 'Path to generated X2Beta Excel file';
COMMENT ON COLUMN public.x2beta_templates.template_path IS 'Path to X2Beta template Excel file';
