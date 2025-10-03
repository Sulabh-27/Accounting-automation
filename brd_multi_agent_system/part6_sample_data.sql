-- Part-6 Sample Data for Testing
-- Run this in your Supabase SQL Editor to populate Part-6 tables

-- Generate a sample run_id (you can replace this with an existing run_id from your runs table)
DO $$
DECLARE
    sample_run_id uuid := gen_random_uuid();
BEGIN
    -- Insert sample run record
    INSERT INTO public.runs (run_id, channel, gstin, month, status, started_at, finished_at)
    VALUES (sample_run_id, 'amazon', '06ABGCS4796R1ZA', '2025-08', 'completed', now() - interval '1 hour', now())
    ON CONFLICT (run_id) DO NOTHING;

    -- Insert sample seller invoices
    INSERT INTO public.seller_invoices (
        id, run_id, channel, gstin, invoice_no, invoice_date, expense_type,
        taxable_value, gst_rate, cgst, sgst, igst, total_value, ledger_name,
        file_path, processing_status, created_at, updated_at
    ) VALUES 
    (
        gen_random_uuid(), sample_run_id, 'amazon', '06ABGCS4796R1ZA',
        'AMZ-FEE-2025-001', '2025-08-15', 'Closing Fee',
        1000.00, 0.18, 90.00, 90.00, 0.00, 1180.00, 'Amazon Closing Fee',
        'sample_amazon_fee_invoice.pdf', 'processed', now(), now()
    ),
    (
        gen_random_uuid(), sample_run_id, 'amazon', '06ABGCS4796R1ZA',
        'AMZ-SHIP-2025-002', '2025-08-16', 'Shipping Fee',
        500.00, 0.18, 45.00, 45.00, 0.00, 590.00, 'Amazon Shipping Fee',
        'sample_amazon_shipping_invoice.pdf', 'processed', now(), now()
    ),
    (
        gen_random_uuid(), sample_run_id, 'amazon', '06ABGCS4796R1ZA',
        'AMZ-COMM-2025-003', '2025-08-17', 'Commission',
        2000.00, 0.18, 180.00, 180.00, 0.00, 2360.00, 'Amazon Commission',
        'sample_amazon_commission_invoice.pdf', 'processed', now(), now()
    ),
    (
        gen_random_uuid(), sample_run_id, 'amazon', '06ABGCS4796R1ZA',
        'AMZ-STOR-2025-004', '2025-08-18', 'Storage Fee',
        300.00, 0.18, 27.00, 27.00, 0.00, 354.00, 'Amazon Storage Fee',
        'sample_amazon_storage_invoice.pdf', 'processed', now(), now()
    ),
    (
        gen_random_uuid(), sample_run_id, 'amazon', '06ABGCS4796R1ZA',
        'AMZ-ADS-2025-005', '2025-08-19', 'Advertising Fee',
        1500.00, 0.18, 135.00, 135.00, 0.00, 1770.00, 'Amazon Advertising Fee',
        'sample_amazon_ads_invoice.pdf', 'processed', now(), now()
    );

    -- Insert sample expense mapping rules
    INSERT INTO public.expense_mapping (
        id, channel, expense_type, ledger_name, gst_rate, is_input_gst, is_active,
        created_at, updated_at
    ) VALUES 
    (gen_random_uuid(), 'amazon', 'Closing Fee', 'Amazon Closing Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'amazon', 'Shipping Fee', 'Amazon Shipping Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'amazon', 'Commission', 'Amazon Commission', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'amazon', 'Storage Fee', 'Amazon Storage Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'amazon', 'Advertising Fee', 'Amazon Advertising Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'amazon', 'Fulfillment Fee', 'Amazon Fulfillment Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'flipkart', 'Commission', 'Flipkart Commission', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'flipkart', 'Collection Fee', 'Flipkart Collection Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'flipkart', 'Fixed Fee', 'Flipkart Fixed Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'flipkart', 'Shipping Fee', 'Flipkart Shipping Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'pepperfry', 'Commission', 'Pepperfry Commission', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'pepperfry', 'Shipping Fee', 'Pepperfry Shipping Fee', 0.18, true, true, now(), now()),
    (gen_random_uuid(), 'pepperfry', 'Payment Gateway Fee', 'Pepperfry Payment Gateway Fee', 0.18, true, true, now(), now())
    ON CONFLICT (channel, expense_type) DO NOTHING;

    -- Insert sample expense export record
    INSERT INTO public.expense_exports (
        id, run_id, gstin, channel, month, file_path, total_records,
        total_taxable, total_tax, export_status, created_at, updated_at
    ) VALUES (
        gen_random_uuid(), sample_run_id, '06ABGCS4796R1ZA', 'amazon', '2025-08',
        'ingestion_layer/exports/X2Beta_Expense_06ABGCS4796R1ZA_2025-08.xlsx',
        5, 5300.00, 954.00, 'completed', now(), now()
    );

    -- Show summary
    RAISE NOTICE '✅ Part-6 Sample Data Inserted Successfully!';
    RAISE NOTICE '📊 Summary:';
    RAISE NOTICE '    - Run ID: %', sample_run_id;
    RAISE NOTICE '    - Seller Invoices: 5 records (₹6,254 total)';
    RAISE NOTICE '    - Expense Mappings: 13 rules across 3 channels';
    RAISE NOTICE '    - Expense Exports: 1 record';
    RAISE NOTICE '';
    RAISE NOTICE '🔍 Check your Supabase tables:';
    RAISE NOTICE '    📄 seller_invoices: Should now have 5+ rows';
    RAISE NOTICE '    🗂️  expense_mapping: Should now have 13+ rows';
    RAISE NOTICE '    🏭 expense_exports: Should now have 1+ row';

END $$;
