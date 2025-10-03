# Part 1: Data Ingestion & Normalization

🎯 **Purpose**: Ingests raw Excel/CSV files from multiple e-commerce channels and normalizes them to a standard 12-column schema with comprehensive data validation.

## 📊 Overview

Part 1 is the foundation of the multi-agent accounting system, responsible for:
- Reading raw data files from various e-commerce platforms
- Normalizing data to a consistent schema
- Performing data validation and quality checks
- Storing processed data in Supabase with full audit trail

## 🏗️ Architecture

```
Raw Excel/CSV Files → Agent Processing → Normalized CSV → Supabase Storage
```

### **Supported Channels:**
- **Amazon MTR** (Monthly Transaction Report) ✅
- **Amazon STR** (Settlement Transaction Report) ✅
- **Flipkart** (Settlement reports) ✅
- **Pepperfry** (Transaction reports) ✅
- **Universal Agent** (Generic CSV processing) ✅

## 📋 Standard Schema (12 Columns)

All data is normalized to this consistent format:

| Column | Description | Example |
|--------|-------------|---------|
| `invoice_date` | Transaction date | 2025-08-15 |
| `type` | Transaction type | Shipment, Refund |
| `order_id` | Unique order identifier | 123-4567890-1234567 |
| `sku` | Stock Keeping Unit | ABC-123-XYZ |
| `asin` | Amazon Standard ID | B08N5WRWNW |
| `quantity` | Item quantity | 2 |
| `taxable_value` | Pre-tax amount | 1000.00 |
| `gst_rate` | GST rate (decimal) | 0.18 |
| `state_code` | Destination state | HR, DL, UP |
| `channel` | Sales channel | amazon, flipkart |
| `gstin` | Company GSTIN | 06ABGCS4796R1ZA |
| `month` | Processing month | 2025-08 |

## 🚀 Usage

### **Individual Channel Processing:**

```bash
# Amazon MTR Processing
python -m ingestion_layer.main \
  --agent amazon_mtr \
  --input "path/to/Amazon MTR B2C Report.xlsx" \
  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08

# Amazon STR Processing
python -m ingestion_layer.main \
  --agent amazon_str \
  --input "path/to/Amazon STR Report.xlsx" \
  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08

# Flipkart Processing
python -m ingestion_layer.main \
  --agent flipkart \
  --input "path/to/Flipkart Settlement.xlsx" \
  --channel flipkart --gstin 06ABGCS4796R1ZA --month 2025-08

# Pepperfry Processing
python -m ingestion_layer.main \
  --agent pepperfry \
  --input "path/to/Pepperfry Sales.xlsx" "path/to/Pepperfry Returns.xlsx" \
  --channel pepperfry --gstin 06ABGCS4796R1ZA --month 2025-08
```

### **Universal Agent (Generic CSV):**

```bash
python -m ingestion_layer.main \
  --agent universal \
  --input "path/to/generic_data.csv" \
  --channel custom --gstin 06ABGCS4796R1ZA --month 2025-08
```

## 🔧 Components

### **1. Agent Classes**

#### **AmazonMTRAgent**
- **File**: `ingestion_layer/agents/amazon_mtr_agent.py`
- **Purpose**: Processes Amazon Monthly Transaction Reports
- **Features**:
  - Excel/CSV file support
  - Automatic column mapping
  - Transaction type filtering (Shipment/Refund)
  - Data validation and cleanup

#### **AmazonSTRAgent**
- **File**: `ingestion_layer/agents/amazon_str_agent.py`
- **Purpose**: Processes Amazon Settlement Transaction Reports
- **Features**:
  - ASIN mapping support
  - Inter-state transaction filtering
  - Settlement-specific data handling

#### **FlipkartAgent**
- **File**: `ingestion_layer/agents/flipkart_agent.py`
- **Purpose**: Processes Flipkart settlement reports
- **Features**:
  - Multi-file processing
  - Flipkart-specific column mapping
  - Commission and fee handling

#### **PepperfryAgent**
- **File**: `ingestion_layer/agents/pepperfry_agent.py`
- **Purpose**: Processes Pepperfry transaction reports
- **Features**:
  - Separate sales and returns files
  - Pepperfry-specific data structure
  - Combined processing workflow

#### **UniversalAgent**
- **File**: `ingestion_layer/agents/universal_agent.py`
- **Purpose**: Generic CSV processing for any channel
- **Features**:
  - Flexible column mapping
  - Configurable data transformation
  - Custom channel support

### **2. Schema Validator**

#### **SchemaValidatorAgent**
- **File**: `ingestion_layer/agents/schema_validator_agent.py`
- **Purpose**: Validates data against the 12-column schema
- **Features**:
  - Required field validation
  - Data type checking
  - Value range validation
  - Error reporting and logging

### **3. Utilities**

#### **CSV Utils**
- **File**: `ingestion_layer/libs/csv_utils.py`
- **Purpose**: Safe CSV/Excel reading with encoding detection
- **Features**:
  - Automatic encoding detection
  - Error handling for malformed files
  - Support for multiple file formats

#### **Supabase Client**
- **File**: `ingestion_layer/libs/supabase_client.py`
- **Purpose**: Database integration and file storage
- **Features**:
  - File upload and metadata tracking
  - Run tracking and audit trail
  - Error handling and retry logic

## 📊 Database Schema

### **Tables Created:**

#### **runs**
```sql
CREATE TABLE runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel TEXT NOT NULL,
    gstin TEXT NOT NULL,
    month TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT
);
```

#### **reports**
```sql
CREATE TABLE reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES runs(id),
    report_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## 🧪 Testing

### **Test Files:**
- `ingestion_layer/tests/test_amazon_mtr.py`
- `ingestion_layer/tests/test_amazon_str.py`
- `ingestion_layer/tests/test_flipkart.py`
- `ingestion_layer/tests/test_pepperfry.py`
- `ingestion_layer/tests/test_universal.py`

### **Run Tests:**
```bash
# Test all Part 1 components
python -m pytest ingestion_layer/tests/test_amazon_mtr.py -v
python -m pytest ingestion_layer/tests/test_schema_validator.py -v

# Test with real data
python -m ingestion_layer.main --agent amazon_mtr --input "test_data.xlsx" --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08
```

## 📈 Performance

### **Benchmarks:**
- **File Size**: Up to 762KB Excel files
- **Record Count**: 698+ transactions processed
- **Processing Time**: <5 seconds for normalization
- **Memory Usage**: Efficient pandas-based processing
- **Success Rate**: 100% with proper input files

### **Output:**
- **Location**: `ingestion_layer/data/normalized/`
- **Format**: CSV files with standardized naming
- **Naming**: `{channel}_{agent}_{hash}.csv`
- **Example**: `amazon_mtr_a1b2c3d4e5f6.csv`

## 🔍 Data Quality

### **Validation Rules:**
1. **Required Fields**: All 12 columns must be present
2. **Data Types**: Proper typing for dates, numbers, strings
3. **Value Ranges**: Positive values for quantities and amounts
4. **Date Formats**: Consistent date formatting
5. **State Codes**: Valid Indian state codes
6. **GSTIN Format**: Valid GSTIN pattern matching

### **Error Handling:**
- **Missing Columns**: Automatic detection and reporting
- **Invalid Data**: Row-level validation with error logging
- **File Format**: Support for Excel and CSV with encoding detection
- **Duplicate Detection**: Hash-based duplicate prevention

## 🚀 Next Steps

After Part 1 completion, data flows to:
- **Part 2**: Item & Ledger Master Mapping
- **Part 3**: Tax Computation & Invoice Numbering
- **Part 4**: Pivoting & Batch Splitting
- **Part 5**: Tally Export (X2Beta Templates)
- **Part 6**: Seller Invoices & Credit Notes

## 📞 Support

### **Common Issues:**

1. **File Format Errors:**
   ```bash
   # Ensure Excel files are in .xlsx format
   # Check for special characters in file names
   ```

2. **Column Mapping Issues:**
   ```bash
   # Verify input file has expected columns
   # Check column names match agent expectations
   ```

3. **Supabase Connection:**
   ```bash
   # Verify .env file configuration
   # Check Supabase credentials and permissions
   ```

---

**🎯 Part 1 Status: PRODUCTION READY**  
**📊 Channels Supported: 5**  
**🚀 Ready for: REAL DATA PROCESSING**
