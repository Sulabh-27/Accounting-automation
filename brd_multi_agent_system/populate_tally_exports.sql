-- Populate tally_exports table with pipeline results
-- Generated on: 2025-09-28T16:51:23.736868

INSERT INTO public.tally_exports (
    id, run_id, channel, gstin, month, gst_rate,
    template_name, file_path, file_size, record_count,
    total_taxable, total_tax, export_status, created_at, updated_at
) VALUES
    ('ca9a3515-5345-40ef-9833-935c3aa3e37e', 'f2f8f3e9-1981-4a91-8da0-cab967d4d53e', 'amazon_mtr', '06ABGCS4796R1ZA', '2025-08', 0.0, 'X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx', 'ingestion_layer/exports/amazon_mtr_06ABGCS4796R1ZA_2025-08_0pct_x2beta.xlsx', 5215, 1, 13730.0, 0.0, 'success', '2025-09-28T16:51:24.381621', '2025-09-28T16:51:24.381621'),
    ('25fc58fa-5718-4667-95a1-353eb8c72f49', 'f2f8f3e9-1981-4a91-8da0-cab967d4d53e', 'amazon_mtr', '06ABGCS4796R1ZA', '2025-08', 0.18, 'X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx', 'ingestion_layer/exports/amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_x2beta.xlsx', 5261, 1, 297171.85, 53490.93, 'success', '2025-09-28T16:51:24.417575', '2025-09-28T16:51:24.417575');