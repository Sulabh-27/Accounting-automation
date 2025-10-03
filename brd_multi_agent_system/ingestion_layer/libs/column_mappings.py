"""
Column mapping configurations for different channels and report types
"""

# Amazon MTR (Monthly Tax Report) - TESTED & WORKING
AMAZON_MTR_MAPPING = {
    "invoice_date": ["invoice_date", "final_invoice_date"],
    "order_id": ["order_id", "order id"],
    "sku": ["sku"],
    "asin": ["asin"],
    "quantity": ["quantity"],
    "taxable_value": ["principal_amount", "tax_exclusive_gross", "invoice_amount"],
    "gst_rate": ["igst_rate", "cgst_rate", "sgst_rate"],  # Will sum these for total GST rate
    "state_code": ["ship_to_state", "bill_from_state"],
    "transaction_type": ["transaction_type"]
}

# Amazon STR (State Tax Report) - To be tested
AMAZON_STR_MAPPING = {
    "invoice_date": ["invoice_date", "posting_date", "shipment_date"],
    "order_id": ["order_id", "amazon_order_id"],
    "asin": ["asin"],
    "quantity": ["quantity"],
    "taxable_value": ["principal_amount", "tax_exclusive_gross", "item_price"],
    "gst_rate": ["igst_rate", "cgst_rate", "sgst_rate"],
    "state_code": ["ship_to_state", "destination_state"],
    "seller_state": ["ship_from_state", "seller_state_code"]
}

# Flipkart - To be configured when data is available
FLIPKART_MAPPING = {
    "invoice_date": ["invoice_date", "order_date", "date"],
    "order_id": ["order_id", "order"],
    "sku": ["sku", "fsn"],
    "quantity": ["quantity", "qty"],
    "taxable_value": ["taxable_value", "net_amount", "item_price"],
    "gst_rate": ["gst_rate", "tax_rate"],
    "state_code": ["ship_to_state_code", "state_code", "state"]
}

# Pepperfry - To be configured when data is available
PEPPERFRY_MAPPING = {
    "invoice_date": ["invoice_date", "date"],
    "order_id": ["order_id", "order"],
    "sku": ["sku", "item_sku"],
    "quantity": ["quantity", "qty"],
    "taxable_value": ["taxable_value", "net_amount", "item_price"],
    "gst_rate": ["gst_rate", "tax_rate"],
    "state_code": ["state_code", "ship_to_state_code", "state"]
}

# Standard output schema - all agents normalize to this
STANDARD_SCHEMA = [
    "invoice_date",
    "type",
    "order_id",
    "sku",
    "asin",
    "quantity",
    "taxable_value",
    "gst_rate",
    "state_code",
    "channel",
    "gstin",
    "month"
]
