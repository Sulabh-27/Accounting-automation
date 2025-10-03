-- Fix Database Paths - Clean up fake Supabase paths
-- Run this in your Supabase SQL Editor to fix path issues

-- 1. Check current problematic records
SELECT 
    'BEFORE CLEANUP' as status,
    COUNT(*) as total_reports,
    COUNT(CASE WHEN file_path LIKE 'Zaggle/%' THEN 1 END) as zaggle_paths,
    COUNT(CASE WHEN file_path LIKE 'ingestion_layer/%' THEN 1 END) as local_paths
FROM public.reports;

-- 2. Update fake Zaggle paths to local paths
UPDATE public.reports 
SET file_path = CASE 
    WHEN file_path LIKE 'Zaggle/%' THEN 
        'ingestion_layer/data/' || split_part(file_path, '/', 3)
    ELSE file_path 
END
WHERE file_path LIKE 'Zaggle/%';

-- 3. Check results after cleanup
SELECT 
    'AFTER CLEANUP' as status,
    COUNT(*) as total_reports,
    COUNT(CASE WHEN file_path LIKE 'Zaggle/%' THEN 1 END) as zaggle_paths,
    COUNT(CASE WHEN file_path LIKE 'ingestion_layer/%' THEN 1 END) as local_paths
FROM public.reports;

-- 4. Show sample of fixed paths
SELECT 
    run_id,
    report_type,
    file_path,
    created_at
FROM public.reports 
ORDER BY created_at DESC 
LIMIT 10;
