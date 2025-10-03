-- Part-6 Migration Script
-- Safely adds missing columns to existing expense_mapping table

-- Add the missing is_input_gst column to expense_mapping table
ALTER TABLE public.expense_mapping 
ADD COLUMN IF NOT EXISTS is_input_gst boolean DEFAULT true;

-- Update existing records to have is_input_gst = true (since all expenses are typically input GST)
UPDATE public.expense_mapping 
SET is_input_gst = true 
WHERE is_input_gst IS NULL;

-- Now insert the default expense mapping rules (will update existing or insert new)
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
('flipkart', 'Payment Gateway Fee', 'Flipkart Payment Gateway Fee', 0.18, true),
('pepperfry', 'Commission', 'Pepperfry Commission', 0.18, true),
('pepperfry', 'Shipping Fee', 'Pepperfry Shipping Fee', 0.18, true),
('pepperfry', 'Payment Gateway Fee', 'Pepperfry Payment Gateway Fee', 0.18, true)
ON CONFLICT (channel, expense_type) DO UPDATE SET
    ledger_name = EXCLUDED.ledger_name,
    gst_rate = EXCLUDED.gst_rate,
    is_input_gst = EXCLUDED.is_input_gst,
    updated_at = now();

-- Verify the migration
SELECT 
    'Migration Complete' as status,
    COUNT(*) as total_expense_rules,
    COUNT(CASE WHEN is_input_gst = true THEN 1 END) as input_gst_rules,
    array_agg(DISTINCT channel) as channels
FROM public.expense_mapping;
