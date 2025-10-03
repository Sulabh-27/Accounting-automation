-- Part-6: Seller Invoices & Credit Notes Schema
-- Database schema for expense-side automation and Tally integration

-- Table to track seller invoices and credit notes
CREATE TABLE IF NOT EXISTS public.seller_invoices (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    invoice_no text NOT NULL,
    invoice_date date NOT NULL,
    expense_type text NOT NULL,       -- closing fee, shipping, commission, etc.
    taxable_value numeric DEFAULT 0,
    gst_rate numeric DEFAULT 0,
    cgst numeric DEFAULT 0,
    sgst numeric DEFAULT 0,
    igst numeric DEFAULT 0,
    total_value numeric DEFAULT 0,
    ledger_name text,
    file_path text,
    processing_status text DEFAULT 'pending', -- pending, processed, failed
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Table to track expense mapping rules
CREATE TABLE IF NOT EXISTS public.expense_mapping (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel text NOT NULL,
    expense_type text NOT NULL,
    ledger_name text NOT NULL,
    gst_rate numeric DEFAULT 0,
    is_input_gst boolean DEFAULT true,
    is_active boolean DEFAULT true,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now(),
    UNIQUE(channel, expense_type)
);

-- Table to track expense tally exports (extends tally_exports for expenses)
CREATE TABLE IF NOT EXISTS public.expense_exports (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES public.runs (run_id) ON DELETE CASCADE,
    channel text NOT NULL,
    gstin text NOT NULL,
    month text NOT NULL,
    file_path text NOT NULL,
    total_records integer DEFAULT 0,
    total_taxable numeric DEFAULT 0,
    total_tax numeric DEFAULT 0,
    export_status text DEFAULT 'pending',
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_seller_invoices_run_id ON public.seller_invoices (run_id);
CREATE INDEX IF NOT EXISTS idx_seller_invoices_gstin ON public.seller_invoices (gstin);
CREATE INDEX IF NOT EXISTS idx_seller_invoices_channel ON public.seller_invoices (channel);
CREATE INDEX IF NOT EXISTS idx_seller_invoices_expense_type ON public.seller_invoices (expense_type);
CREATE INDEX IF NOT EXISTS idx_expense_mapping_channel ON public.expense_mapping (channel);
CREATE INDEX IF NOT EXISTS idx_expense_exports_run_id ON public.expense_exports (run_id);
CREATE INDEX IF NOT EXISTS idx_expense_exports_gstin ON public.expense_exports (gstin);

-- Insert default expense mapping rules for Amazon
INSERT INTO public.expense_mapping (channel, expense_type, ledger_name, gst_rate, is_input_gst) VALUES
('amazon', 'Closing Fee', 'Amazon Closing Fee', 0.18, true),
('amazon', 'Shipping Fee', 'Amazon Shipping Fee', 0.18, true),
('amazon', 'Commission', 'Amazon Commission', 0.18, true),
('amazon', 'Advertising Fee', 'Amazon Advertising Fee', 0.18, true),
('amazon', 'Storage Fee', 'Amazon Storage Fee', 0.18, true),
('amazon', 'Fulfillment Fee', 'Amazon Fulfillment Fee', 0.18, true),
('amazon', 'Refund Processing Fee', 'Amazon Refund Processing Fee', 0.18, true),
('amazon', 'Return Processing Fee', 'Amazon Return Processing Fee', 0.18, true),
('flipkart', 'Commission', 'Flipkart Commission', 0.18, true),
('flipkart', 'Collection Fee', 'Flipkart Collection Fee', 0.18, true),
('flipkart', 'Fixed Fee', 'Flipkart Fixed Fee', 0.18, true),
('flipkart', 'Shipping Fee', 'Flipkart Shipping Fee', 0.18, true),
('flipkart', 'Payment Gateway Fee', 'Flipkart Payment Gateway Fee', 0.18, true)
ON CONFLICT (channel, expense_type) DO UPDATE SET
    ledger_name = EXCLUDED.ledger_name,
    gst_rate = EXCLUDED.gst_rate,
    is_input_gst = EXCLUDED.is_input_gst,
    updated_at = now();

-- Comments for documentation
COMMENT ON TABLE public.seller_invoices IS 'Tracks parsed seller invoices and credit notes for expense processing';
COMMENT ON TABLE public.expense_mapping IS 'Maps expense types to Tally ledger names per channel';
COMMENT ON TABLE public.expense_exports IS 'Tracks X2Beta expense export files generated for Tally import';

COMMENT ON COLUMN public.seller_invoices.expense_type IS 'Type of expense: closing fee, shipping, commission, etc.';
COMMENT ON COLUMN public.seller_invoices.processing_status IS 'Status: pending, processed, failed';
COMMENT ON COLUMN public.expense_mapping.gst_rate IS 'GST rate for this expense type (0.0, 0.05, 0.12, 0.18, 0.28)';
COMMENT ON COLUMN public.expense_exports.file_path IS 'Path to generated X2Beta Expense Excel file';
