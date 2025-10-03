-- Part-6 Sample Data - Fixed Version
-- Run this in your Supabase SQL Editor to populate Part-6 tables

-- First, create a sample run record with a fixed UUID
INSERT INTO public.runs (run_id, channel, gstin, month, status, started_at, finished_at)
VALUES ('12345678-1234-1234-1234-123456789abc', 'amazon', '06ABGCS4796R1ZA', '2025-08', 'completed', now() - interval '1 hour', now())
ON CONFLICT (run_id) DO NOTHING;

-- Insert sample seller invoices using the fixed run_id
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

-- Insert sample expense export record
INSERT INTO public.expense_exports (
    id, run_id, gstin, channel, month, file_path,
    total_taxable, total_tax, export_status, created_at, updated_at
) VALUES (
    gen_random_uuid(), '12345678-1234-1234-1234-123456789abc', '06ABGCS4796R1ZA', 'amazon', '2025-08',
    'ingestion_layer/exports/X2Beta_Expense_06ABGCS4796R1ZA_2025-08.xlsx',
    5300.00, 954.00, 'completed', now(), now()
);

-- Show summary
SELECT 
    '✅ Part-6 Sample Data Inserted Successfully!' as message,
    COUNT(*) as seller_invoices_count,
    SUM(total_value) as total_invoice_amount
FROM public.seller_invoices 
WHERE run_id = '12345678-1234-1234-1234-123456789abc';
