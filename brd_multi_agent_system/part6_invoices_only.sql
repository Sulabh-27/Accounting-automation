-- Part-6 Sample Invoices Only (Simplified)
-- Run this in your Supabase SQL Editor to see sample seller invoices

-- First, create a sample run record with a fixed UUID
INSERT INTO public.runs (run_id, channel, gstin, month, status, started_at, finished_at)
VALUES ('12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA', '2025-08', 'completed', now() - interval '1 hour', now())
ON CONFLICT (run_id) DO NOTHING;

-- Insert sample seller invoices (the main thing you want to see!)
INSERT INTO public.seller_invoices (
    id, run_id, channel, gstin, invoice_no, invoice_date, expense_type,
    taxable_value, gst_rate, cgst, sgst, igst, total_value, ledger_name,
    file_path, processing_status, created_at, updated_at
) VALUES 
(
    gen_random_uuid(), '12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA',
    'AMZ-FEE-2025-001', '2025-08-15', 'Closing Fee',
    1000.00, 0.18, 90.00, 90.00, 0.00, 1180.00, 'Amazon Closing Fee',
    'sample_amazon_fee_invoice.pdf', 'processed', now(), now()
),
(
    gen_random_uuid(), '12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA',
    'AMZ-SHIP-2025-002', '2025-08-16', 'Shipping Fee',
    500.00, 0.18, 45.00, 45.00, 0.00, 590.00, 'Amazon Shipping Fee',
    'sample_amazon_shipping_invoice.pdf', 'processed', now(), now()
),
(
    gen_random_uuid(), '12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA',
    'AMZ-COMM-2025-003', '2025-08-17', 'Commission',
    2000.00, 0.18, 180.00, 180.00, 0.00, 2360.00, 'Amazon Commission',
    'sample_amazon_commission_invoice.pdf', 'processed', now(), now()
),
(
    gen_random_uuid(), '12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA',
    'AMZ-STOR-2025-004', '2025-08-18', 'Storage Fee',
    300.00, 0.18, 27.00, 27.00, 0.00, 354.00, 'Amazon Storage Fee',
    'sample_amazon_storage_invoice.pdf', 'processed', now(), now()
),
(
    gen_random_uuid(), '12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA',
    'AMZ-ADS-2025-005', '2025-08-19', 'Advertising Fee',
    1500.00, 0.18, 135.00, 135.00, 0.00, 1770.00, 'Amazon Advertising Fee',
    'sample_amazon_ads_invoice.pdf', 'processed', now(), now()
);

-- Show the beautiful invoice summary! 📄
SELECT 
    '🎉 SUCCESS: 5 Sample Amazon Invoices Created!' as status,
    COUNT(*) as total_invoices,
    SUM(taxable_value) as total_taxable_amount,
    SUM(cgst + sgst + igst) as total_gst_amount,
    SUM(total_value) as grand_total,
    string_agg(DISTINCT expense_type, ', ') as expense_types
FROM public.seller_invoices 
WHERE run_id = '12345678-1234-1234-1234-123456789abc';

-- Show individual invoice details
SELECT 
    invoice_no,
    expense_type,
    'Rs. ' || taxable_value as taxable,
    'Rs. ' || (cgst + sgst + igst) as gst,
    'Rs. ' || total_value as total,
    invoice_date
FROM public.seller_invoices 
WHERE run_id = '12345678-1234-1234-1234-123456789abc'
ORDER BY invoice_date;
