# BRD Multi-Agent Accounting Automation System

An end-to-end multi-agent system designed to ingest, process, validate, transform, and export e-commerce transactional data across multiple marketplaces (Amazon MTR/STR, Flipkart, Pepperfry) into standardized accounting formats (X2Beta Excel schema, Tally XML/Excel, Supabase database, and MIS reports).

---

## 🎯 Business Context & Core Purpose

E-commerce businesses operating across multi-channel marketplaces (Amazon India, Flipkart, Pepperfry, etc.) encounter massive volumes of transactional data daily. Each platform provides reports with varying schemas, tax column definitions, and fee structures.

Manual accounting and GST filing for these platforms leads to:

- **Tax Mismatches** — incorrect split between intra-state (CGST + SGST) and inter-state (IGST) taxes.
- **Complex Commission Reconciliation** — marketplace fee structures (fulfillment fees, seller fees, referral fees) mapped to the wrong ledgers.
- **Format Incompatibility** — target accounting platforms like Tally or X2Beta require rigid, GSTIN-specific Excel/XML templates.

This system automates the entire ingestion-to-export lifecycle, acting as an intelligent bridge between raw marketplace reports and corporate accounting ledgers.

---

## 🏗️ Architecture Overview

The system follows a modular, agent-based ingestion pipeline. Data flows sequentially through specialized agents and utility libraries to ensure strict tax calculation accuracy, schema compliance, item/ledger resolution, and exception handling.

```
                ┌─────────────────────────┐
                │  Raw Marketplace Inputs  │
                │  (Amazon MTR/STR, etc.)  │
                └────────────┬─────────────┘
                              │
                              ▼
                ┌─────────────────────────┐
                │      Ingestion Layer     │
                │ (Universal & Marketplace │
                │         Agents)          │
                └────────────┬─────────────┘
                              │
                              ▼
                ┌─────────────────────────┐
                │  Tax Engine & Item       │
                │  Master Resolver         │
                └────────────┬─────────────┘
                              │
                              ▼
                ┌─────────────────────────┐
                │  Pivoter & Schema        │
                │  Validator               │
                └────────────┬─────────────┘
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
  ┌─────────────────────────┐   ┌─────────────────────────┐
  │  Approval & Exception    │   │   Output Exporters       │
  │  Workflow                │   │  (X2Beta, Tally, MIS,    │
  │  (CLI & Streamlit)       │   │   Database Storage)      │
  └─────────────────────────┘   └─────────────────────────┘
```

### Detailed Agent-Level Flow

```
                ┌────────────────────────────────┐
                │      Raw Marketplace Input      │
                │   (Amazon MTR/STR, Flipkart)    │
                └────────────────┬─────────────────┘
                                  ▼
                ┌────────────────────────────────┐
                │         Universal Agent          │
                └────────────────┬─────────────────┘
                                  │
               ┌──────────────────┴──────────────────┐
               ▼                                      ▼
┌───────────────────────────────┐   ┌───────────────────────────────┐
│      Marketplace Agents        │   │     Seller Invoice Parser      │
│  (Amazon, Flipkart, etc.)      │   │  (Non-marketplace Purchases)   │
└────────────────┬───────────────┘   └────────────────┬───────────────┘
                  │                                    │
                  └──────────────────┬─────────────────┘
                                      ▼
                     ┌────────────────────────────────┐
                     │         Batch Splitter           │
                     └────────────────┬─────────────────┘
                                       ▼
                     ┌────────────────────────────────┐
                     │  Item Master Resolver &          │
                     │  Tax Engine                      │
                     └────────────────┬─────────────────┘
                                       ▼
                     ┌────────────────────────────────┐
                     │  Pivoter & Ledger/               │
                     │  Expense Mappers                 │
                     └────────────────┬─────────────────┘
                                       ▼
                     ┌────────────────────────────────┐
                     │  Approval Workflow &             │
                     │  Exception Handler                │
                     └────────────────┬─────────────────┘
                                       │
                ┌──────────────────────┼──────────────────────┐
                ▼                      ▼                      ▼
  ┌─────────────────────┐   ┌─────────────────┐   ┌─────────────────────────┐
  │    X2Beta Writer      │   │  Tally Exporter  │   │  Supabase & MIS Engine   │
  │ (GSTIN Sales/Expense   │   │  (XML / Excel)   │   │  (DB Storage &            │
  │        Excel)          │   │                   │   │   Dashboards)             │
  └─────────────────────┘   └─────────────────┘   └─────────────────────────┘
```

---

## 🔑 Key System Components

### Ingestion Layer (`/ingestion_layer`)

- **Universal Agent** (`universal_agent.py`) — Acts as a smart router. Inspects incoming file headers/metadata to identify the platform source and delegates processing to dedicated platform agents.
- **Platform Agents**:
  - `amazon_mtr_agent.py` — Handles Amazon Merchant Tax Report (MTR) B2B/B2C transactions.
  - `amazon_str_agent.py` — Processes Amazon Summary Tax Reports (STR).
  - `flipkart_agent.py` — Parses Flipkart sales, returns, and fee statements.
  - `pepperfry_agent.py` — Manages Pepperfry order and remittance data.
- **Seller Invoice Parser** (`seller_invoice_parser.py`) — Reads direct purchase/expense documents from external vendors (non-marketplace invoices).

### Processing & Business Logic Agents

- **Batch Splitter** (`batch_splitter.py`) — Divides large transactional files into smaller processing batches, categorized by Seller GSTIN and GST tax rate slabs (e.g., 0%, 18%). This isolation ensures tax computations match target filing templates.
- **Item Master Resolver** (`item_master_resolver.py`) — Cross-references raw marketplace SKUs and descriptions against master item listings (`Item Master - Sample.xlsx`) to retrieve unified internal item codes, standardized units of measure (UOM), and baseline tax classifications.
- **Tax Engine** (`tax_engine.py`) — Enforces Indian GST logic:
  - Evaluates State of Origin vs. Place of Supply.
  - Determines local intra-state sales (CGST + SGST) vs. inter-state sales (IGST).
  - Calculates taxable values, tax components, and invoice totals.
- **Pivoter** (`pivoter.py`) — Converts line-item transactional rows into aggregated voucher-level summaries required for accounting ledger entries.
- **Ledger / Expense Mapper** (`ledger_mapper.py`, `expense_mapper.py`) — Maps marketplace fee types (commission, storage, advertising, shipping) to the appropriate accounting chart-of-accounts ledgers.

### Exporters & Integrations

- **X2Beta Writer** (`libs/x2beta_writer.py`) — Dynamically writes pivoted sales and expense batches into pre-built, GSTIN-specific X2Beta Excel templates (`templates/X2Beta Sales Template - *.xlsx`).
- **Tally Exporter** (`tally_exporter.py`, `expense_tally_exporter.py`) — Converts mapped transactions into Tally-compatible XML/Excel formats for seamless import into Tally ERP/Prime.
- **MIS Generator** (`mis_generator.py`) — Compiles executive summary reports: gross sales, return deductions, net tax liabilities, and marketplace fee deductions.
- **Supabase Client** (`libs/supabase_client.py`) — Manages real-time database persistence, batch status tracking, and audit logging into Cloud PostgreSQL tables.

### Governance & Human-in-the-Loop

- **Approval Workflow** (`approval_workflow.py`, `approval_cli.py`) — Enforces validation gates before final export or database commit; batches stay pending until a human accountant reviews key figures.
- **Exception Handler** (`exception_handler.py`) — Routes unmapped SKUs, missing tax rates, or schema mismatches to an isolated queue for manual resolution, preventing pipeline crashes.
- **Audit Logger** (`audit_logger.py`) — Maintains trace logs for every state transition (Ingested → Split → Normalized → Approved → Exported) with timestamps, for full compliance traceability.
- **Streamlit Application** (`/streamlit_app`) — Dashboard UI providing:
  - Pipeline execution control
  - Analytics (revenue, tax liabilities, fee charts)
  - Exception management (resolve unmapped SKUs / tax errors)
  - Master data & settings (SKU maps, ledgers, DB connections)

---

## 🔄 Complete Step-by-Step Data Flow

```
[Raw Report Input]
        │
        ▼
1. Universal Agent inspects file & assigns platform agent
        │
        ▼
2. Batch Splitter breaks data down by GSTIN and GST rate slabs
        │
        ▼
3. Item Master Resolver matches SKUs with master data
        │
        ▼
4. Tax Engine calculates intra-state (CGST+SGST) or inter-state (IGST) split
        │
        ▼
5. Ledger Mapper classifies marketplace fees to chart-of-accounts
        │
        ▼
6. Pivoter groups rows into invoice/voucher-level summaries
        │
        ▼
7. Exception Handler catches invalid rows; Approval Workflow gates the batch
        │
        ▼
8. Exporters output files to X2Beta Excel, Tally XML, MIS CSV, and Supabase DB
```

### Workflow Execution Steps (summary)

| Step | Stage | Description |
|------|-------|-------------|
| 1 | **Batch Ingestion** | Raw CSV/Excel reports (e.g., Amazon MTR CSVs) are placed in `ingestion_layer/data/batches/`. `batch_splitter.py` segments records by seller GSTIN and tax slabs. |
| 2 | **Parsing & Normalization** | The designated marketplace agent normalizes raw fields into a standardized schema contract (`libs/contracts.py`). |
| 3 | **Master Resolution & Tax Computation** | SKUs are matched against the Item Master. The Tax Engine validates states, handles zero-rated vs. taxable items, and assigns CGST, SGST, or IGST. |
| 4 | **Pivoting & Template Writing** | `pivoter.py` organizes rows into voucher-level summaries; `x2beta_writer.py` populates pre-formatted GSTIN Excel templates. |
| 5 | **Approval & Exception Review** | Transactions with missing ledgers or invalid GSTINs are sent to `exception_handler.py`. Users review/resolve via the Streamlit UI (`04_Exceptions.py`) or CLI (`approval_cli.py`). |
| 6 | **Export & Persistence** | Approved batches are exported to Excel (`exports/`), converted to Tally XML, or inserted into Supabase (`insert_to_supabase_direct.py`). |

---

## 🗄️ Database & Schema Design (`complete_schema.sql`)

The system relies on relational tables in Supabase/PostgreSQL to manage pipeline state and reporting:

- **`batch_runs`** — Batch-level execution metadata, status (`PENDING`, `APPROVED`, `REJECTED`), source marketplace, and batch parameters.
- **`normalized_transactions`** — Standardized transaction line items post-parsing.
- **`item_master` & `ledger_master`** — Reference tables for SKU mappings and ledger classifications.
- **`approval_queue`** — Queue for human-in-the-loop review.
- **`audit_logs`** — Detailed operational logging for compliance and troubleshooting.

---

## 📂 Directory Structure

```
brd_multi_agent_system/
├── complete_schema.sql              # Full database DDL for Supabase
├── create_golden_x2beta.py          # Test data and golden template generation
├── final_pipeline_demo.py           # End-to-end demo script
├── run_complete_pipeline_fixed.py   # Master pipeline execution script
├── ingestion_layer/                 # Core processing core
│   ├── agents/                      # Micro-agents (Amazon, Tax, Pivoter, Tally, etc.)
│   ├── data/                        # Raw, batch, normalized, and master sample datasets
│   ├── exports/                     # Generated output files (MIS, X2Beta Excel)
│   ├── libs/                        # Shared utilities (Tax rules, Excel writers, Supabase)
│   ├── sql/                         # Schema migration scripts by pipeline part
│   ├── templates/                   # Blank X2Beta Sales & Expense templates per GSTIN
│   └── tests/                       # Unit tests & golden file validations
└── streamlit_app/                   # Web dashboard UI
    ├── app.py                       # Streamlit main entry point
    └── pages/                       # Dashboard views (Pipeline, Analytics, Exceptions, etc.)
```

---

## ⚙️ Setup & Execution

### Prerequisites

- Python 3.10+
- Virtual environment (recommended)

### Environment Configuration

Copy `.env.template` to `.env` and configure your credentials:

```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

### Dependency Installation

```bash
pip install -r ingestion_layer/requirements.txt
pip install -r streamlit_app/requirements.txt
```

### Running the End-to-End Pipeline

```bash
python run_complete_pipeline_fixed.py
```

### Launching the Dashboard UI

```bash
cd streamlit_app
streamlit run app.py
```
