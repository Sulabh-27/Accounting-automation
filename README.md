BRD Multi-Agent Accounting Automation System
An end-to-end multi-agent system designed to ingest, process, validate, transform, and export e-commerce transactional data across multiple marketplaces (Amazon MTR/STR, Flipkart, Pepperfry) into standardized accounting formats (X2Beta Excel schema, Tally XML/Excel, Supabase database, and MIS reports).

Architecture Overview
The system follows a modular, agent-based ingestion pipeline. Data flows sequentially through specialized agents and utility libraries to ensure strict tax calculation accuracy, schema compliance, item/ledger resolution, and exception handling.

                    ┌─────────────────────────┐
                    │ Raw Marketplace Inputs  │
                    │ (Amazon MTR/STR, etc.)  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │    Ingestion Layer      │
                    │  (Universal & Marketplace│
                    │        Agents)          │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │    Tax Engine & Item    │
                    │    Master Resolver      │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │    Pivoter & Schema     │
                    │       Validator         │
                    └────────────┬────────────┘
                                 │
                   ┌─────────────┴─────────────┐
                   ▼                           ▼
      ┌─────────────────────────┐ ┌─────────────────────────┐
      │   Approval & Exception  │ │ Output Exporters        │
      │        Workflow         │ │ (X2Beta, Tally, MIS,    │
      │   (CLI & Streamlit)     │ │    Database Storage)    │
      └─────────────────────────┘ └─────────────────────────┘
Key System Components
1. Ingestion Layer (/ingestion_layer)
Universal Agent (universal_agent.py): Generic ingestion gateway that inspects incoming file formats and routes them to platform-specific agents.

Platform Agents:

amazon_mtr_agent.py: Handles Amazon Merchant Tax Report (MTR) B2B/B2C transactions.

amazon_str_agent.py: Processes Amazon Summary Tax Reports (STR).

flipkart_agent.py: Parses Flipkart sales, returns, and fee statements.

pepperfry_agent.py: Manages Pepperfry order and remittance data.

Seller Invoice Parser (seller_invoice_parser.py): Parses non-marketplace purchase and expense invoices.

2. Processing & Business Logic Agents
Batch Splitter (batch_splitter.py): Splits large input datasets into processing batches grouped by GSTIN and tax rates (e.g., 0%, 18%).

Item Master Resolver (item_master_resolver.py): Maps raw product SKUs and descriptions to master item codes and standard unit measures.

Tax Engine (tax_engine.py): Computes intra-state (CGST + SGST) vs. inter-state (IGST) tax breakdowns based on place of supply and GST rates.

Pivoter (pivoter.py): Aggregates batch line items into summarized tax and sales voucher structures required by target systems.

Ledger / Expense Mapper (ledger_mapper.py, expense_mapper.py): Maps marketplace fee types, commission structures, and shipping fees to appropriate accounting chart-of-accounts ledgers.

3. Exporters & Integrations
X2Beta Writer (libs/x2beta_writer.py): Generates structured Excel files strictly matching the GSTIN-specific X2Beta sales and expense templates.

Tally Exporter (tally_exporter.py, expense_tally_exporter.py): Converts mapped transactions into Tally-compatible XML/Excel formats.

MIS Generator (mis_generator.py): Compiles executive summary reports and financial metrics.

Supabase Client (libs/supabase_client.py): Manages real-time database persistence, batch status tracking, and audit logging.

4. Governance & Human-in-the-Loop
Approval Workflow (approval_workflow.py, approval_cli.py): Enforces validation gates before final export or database commit.

Exception Handler (exception_handler.py): Routes unmapped SKUs, missing tax rates, or schema mismatches to an isolated queue for manual resolution.

Audit Logger (audit_logger.py): Maintains trace logs for every state transition and pipeline step.

Streamlit Application (/streamlit_app): Dashboard UI providing visual controls for pipeline execution, analytics, exception management, master data maintenance, and Tally export monitoring.

Workflow Execution Steps
[Raw Files] ──> 1. Batch Splitting ──> 2. Parsing & Mapping ──> 3. Tax Calculation 
                 ──> 4. Pivot Summarization ──> 5. Approval Gate ──> 6. Export Generation
Batch Ingestion:
Raw CSV or Excel reports (e.g., Amazon MTR CSV files) are placed in the ingestion batch directory (ingestion_layer/data/batches/). batch_splitter.py segments records by seller GSTIN and tax slabs.

Parsing & Normalization:
The designated marketplace agent normalizes raw fields into a standardized schema contract (libs/contracts.py).

Master Resolution & Tax Computation:
SKUs are matched against the Item Master (Item Master - Sample.xlsx). The tax_engine validates states, handles zero-rated vs. taxable items, and assigns CGST, SGST, or IGST.

Pivoting & Template Writing:
pivoter.py organizes rows into voucher-level summaries. x2beta_writer.py populates pre-formatted GSTIN Excel templates.

Approval & Exception Review:
Transactions with missing ledgers or invalid GSTINs are sent to exception_handler.py. Users can review and resolve exceptions via the Streamlit UI (04_Exceptions.py) or CLI (approval_cli.py).

Export & Persistence:
Approved batches are exported to Excel (exports/), converted to Tally XML, or inserted into Supabase (insert_to_supabase_direct.py).

Directory Structure Overview
brd_multi_agent_system/
├── complete_schema.sql             # Full database DDL for Supabase
├── create_golden_x2beta.py         # Test data and golden template generation
├── final_pipeline_demo.py          # End-to-end demo script
├── run_complete_pipeline_fixed.py  # Master pipeline execution script
├── ingestion_layer/                # Core processing core
│   ├── agents/                     # Micro-agents (Amazon, Tax, Pivoter, Tally, etc.)
│   ├── data/                       # Raw, batch, normalized, and master sample datasets
│   ├── exports/                    # Generated output files (MIS, X2Beta Excel)
│   ├── libs/                       # Shared utilities (Tax rules, Excel writers, Supabase)
│   ├── sql/                        # Schema migration scripts by pipeline part
│   ├── templates/                  # Blank X2Beta Sales & Expense templates per GSTIN
│   └── tests/                      # Unit tests & golden file validations
└── streamlit_app/                  # Web dashboard UI
    ├── app.py                      # Streamlit main entry point
    └── pages/                      # Dashboard views (Pipeline, Analytics, Exceptions, etc.)
Setup & Execution
Prerequisites
Python 3.10+

Virtual Environment (recommended)

Environment Configuration
Copy .env.template to .env and configure your credentials:

Bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
Dependency Installation
Bash
pip install -r ingestion_layer/requirements.txt
pip install -r streamlit_app/requirements.txt
Running the End-to-End Pipeline
To run the full end-to-end automated processing pipeline:

Bash
python run_complete_pipeline_fixed.py
Launching the Dashboard UI
To launch the interactive management interface:

Bash
cd streamlit_app
streamlit run app.py
provide more explantion about the project

Here is an extended breakdown of the project, expanding on its business context, deep-dive component responsibilities, end-to-end data pipeline, and multi-agent design principles.  
ZIP

🎯 Business Context & Core Purpose
E-commerce businesses operating across multi-channel marketplaces (such as Amazon India, Flipkart, and Pepperfry) encounter massive volumes of transactional data daily. Each platform provides reports with varying schemas, tax column definitions, and fee structures.  
ZIP
+ 1

Manual accounting and GST filing for these platforms lead to:

Tax Mismatches: Incorrect split between intra-state (CGST + SGST) and inter-state (IGST) taxes.

Complex Commission Reconciliation: Marketplace fee structures (fulfillment fees, seller fees, referral fees) mapped to wrong ledgers.  
ZIP

Format Incompatibility: Target accounting platforms like Tally or X2Beta require very rigid, GSTIN-specific Excel/XML templates.  
ZIP

This BRD Multi-Agent System automates the entire ingestion-to-export lifecycle, acting as an intelligent bridge between raw marketplace reports and corporate accounting ledgers.  
ZIP

🛠️ Detailed Architecture & Agent Responsibilities
                               ┌────────────────────────────────┐
                               │     Raw Marketplace Input      │
                               │   (Amazon MTR/STR, Flipkart)   │
                               └───────────────┬────────────────┘
                                               │
                                               ▼
                               ┌────────────────────────────────┐
                               │       Universal Agent          │
                               └───────────────┬────────────────┘
                                               │
                       ┌───────────────────────┴───────────────────────┐
                       ▼                                               ▼
       ┌───────────────────────────────┐               ┌───────────────────────────────┐
       │     Marketplace Agents        │               │     Seller Invoice Parser     │
       │   (Amazon, Flipkart, etc.)    │               │  (Non-marketplace Purchases)  │
       └───────────────┬───────────────┘               └───────────────┬───────────────┘
                       │                                               │
                       └───────────────────────┬───────────────────────┘
                                               │
                                               ▼
                               ┌────────────────────────────────┐
                               │         Batch Splitter         │
                               └───────────────┬────────────────┘
                                               │
                                               ▼
                               ┌────────────────────────────────┐
                               │     Item Master Resolver &     │
                               │           Tax Engine           │
                               └───────────────┬────────────────┘
                                               │
                                               ▼
                               ┌────────────────────────────────┐
                               │      Pivoter & Ledger/         │
                               │        Expense Mappers         │
                               └───────────────┬────────────────┘
                                               │
                                               ▼
                               ┌────────────────────────────────┐
                               │      Approval Workflow &       │
                               │       Exception Handler        │
                               └───────────────┬────────────────┘
                                               │
                       ┌───────────────────────┼───────────────────────┐
                       ▼                       ▼                       ▼
        ┌─────────────────────────────┐ ┌─────────────┐ ┌─────────────────────────────┐
        │        X2Beta Writer        │ │Tally Exporter│ │   Supabase & MIS Engine    │
        │(GSTIN Sales/Expense Excel)  │ │(XML / Excel)│ │  (DB Storage & Dashboards)  │
        └─────────────────────────────┘ └─────────────┘ └─────────────────────────────┘
1. Ingestion Layer
Universal Agent (universal_agent.py): Acts as a smart router. It inspects file headers/metadata to identify the platform source and delegates processing to dedicated platform agents.  
ZIP
+ 1

Platform Agents (amazon_mtr_agent.py, amazon_str_agent.py, flipkart_agent.py, pepperfry_agent.py): Each platform agent contains specialized parsing rules to ingest raw marketplace files (including B2B and B2C reports), extract financial attributes, and normalize them into standard data contracts.  
ZIP

Seller Invoice Parser (seller_invoice_parser.py): Reads direct purchase/expense documents from external vendors.  
ZIP

2. Processing & Business Logic Layer
Batch Splitter (batch_splitter.py): Divides massive transactional files into smaller processing batches categorized by Seller GSTIN and GST Tax Rate Slabs (e.g., 0%, 18%). This isolation ensures that tax computations match target filing templates perfectly.  
ZIP
+ 1

Item Master Resolver (item_master_resolver.py): Cross-references raw marketplace SKUs and descriptions against master item listings (Item Master Sample.xlsx) to retrieve unified internal item codes, standardized units of measure (UOM), and baseline tax classifications.  
ZIP

Tax Engine (tax_engine.py): Enforces Indian GST logic:

Evaluates State of Origin vs. Place of Supply.  
ZIP

Determines local intra-state sales (CGST+SGST) versus inter-state sales (IGST).  
ZIP

Calculates taxable values, tax components, and invoice totals accurately.  
ZIP

Pivoter (pivoter.py): Converts line-item transactional rows into aggregated voucher-level summaries required for accounting ledger entries.  
ZIP

Ledger & Expense Mapper (ledger_mapper.py, expense_mapper.py): Maps marketplace fees (e.g., storage fees, advertising costs, technology commission) to corresponding expense ledgers in the corporate chart of accounts.  
ZIP

3. Exporters & System Integrations
X2Beta Writer (x2beta_writer.py): Dynamically writes pivoted sales and expense batches into pre-built GSTIN-specific X2Beta Excel templates (templates/X2Beta Sales Template - *.xlsx).  
ZIP

Tally Exporter (tally_exporter.py, expense_tally_exporter.py): Generates Tally XML files/Excel sheets formatted for seamless import into Tally ERP/Prime.  
ZIP

MIS Generator (mis_generator.py): Compiles executive summaries detailing gross sales, return deductions, net tax liabilities, and marketplace fee deductions.  
ZIP

Supabase Database (libs/supabase_client.py): Stores processed batches, audit trails, and execution states into Cloud PostgreSQL tables defined in complete_schema.sql.  
ZIP

4. Human-in-the-Loop & System Governance
Approval Workflow (approval_workflow.py, approval_cli.py): Enforces validation checkpoints. Batches remain in a pending state until human accountants review key figures.  
ZIP
+ 1

Exception Handler (exception_handler.py): Isolates problematic rows—such as unmapped SKUs, missing seller GSTINs, or invalid tax rates—into a separate queue, preventing pipeline crashes while allowing manual remediation.  
ZIP

Audit Logger (audit_logger.py): Tracks every action and state change (Ingested → Split → Normalized → Approved → Exported) with timestamps for full compliance traceability.  
ZIP

Streamlit App (/streamlit_app): An interactive Web UI providing:

Pipeline Execution Control: Run workflows visually.  
ZIP

Analytics: Revenue, tax liabilities, and fee charts.  
ZIP

Exception Management: Resolve unmapped SKUs or tax errors.  
ZIP

Master Data & Settings: Manage SKU maps, ledgers, and database connections.  
ZIP

🔄 Complete Step-by-Step Data Flow
[Raw Report Input] 
      │
      ▼
1. Universal Agent inspects file & assigns platform agent.
      │
      ▼
2. Batch Splitter breaks data down by GSTIN and GST Rate Slabs.
      │
      ▼
3. Item Master Resolver matches SKUs with Master Data.
      │
      ▼
4. Tax Engine calculates intra-state (CGST+SGST) or inter-state (IGST) split.
      │
      ▼
5. Ledger Mapper classifies marketplace fees to chart-of-accounts.
      │
      ▼
6. Pivoter groups rows into invoice/voucher-level summaries.
      │
      ▼
7. Exception Handler catches invalid rows; Approval Workflow gates batch[cite: 1].
      │
      ▼
8. Exporters output files to X2Beta Excel, Tally XML, MIS CSV, and Supabase DB[cite: 1].
🗄️ Database & Schema Design (complete_schema.sql)
The system relies on relational tables in Supabase/PostgreSQL to manage pipeline state and reporting[cite: 1]:

batch_runs: Stores batch-level execution metadata, status (PENDING, APPROVED, REJECTED), source marketplace, and batch parameters[cite: 1].

normalized_transactions: Stores standardized transaction line items post-parsing[cite: 1].

item_master & ledger_master: Reference tables for SKU mappings and ledger classifications[cite: 1].

approval_queue: Queue for human-in-the-loop review[cite: 1].

audit_logs: Detailed operational logging for compliance and troubleshooting[cite: 1].

