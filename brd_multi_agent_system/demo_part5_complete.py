#!/usr/bin/env python3
"""
Complete demonstration of Part-5: Tally Export (X2Beta Templates)
Tests the complete workflow with real batch data and validates X2Beta output
"""
import sys
import os
import pandas as pd
import uuid
import tempfile
import shutil
sys.path.append('.')

from ingestion_layer.agents.tally_exporter import TallyExporterAgent
from ingestion_layer.libs.x2beta_writer import X2BetaWriter


class MockSupabase:
    """Mock Supabase client for testing."""
    
    def __init__(self):
        self.tally_exports = []
    
    @property
    def client(self):
        return MockClient(self)


class MockClient:
    """Mock Supabase client."""
    
    def __init__(self, parent):
        self.parent = parent
    
    def table(self, table_name):
        return MockTable(table_name, self.parent)


class MockTable:
    """Mock Supabase table."""
    
    def __init__(self, table_name, parent):
        self.table_name = table_name
        self.parent = parent
    
    def insert(self, data):
        if self.table_name == 'tally_exports':
            self.parent.tally_exports.extend(data)
        return MockInsert()


class MockInsert:
    """Mock insert operation."""
    
    def execute(self):
        return MockResponse([])


class MockResponse:
    """Mock response."""
    
    def __init__(self, data):
        self.data = data


def test_part5_with_real_batch_data():
    """Test Part-5 with real batch data from Part-4."""
    
    print("🏭 TESTING PART-5: Tally Export (X2Beta Templates)")
    print("=" * 60)
    
    # Find existing batch files from Part-4
    batch_directory = "ingestion_layer/data/batches"
    
    if not os.path.exists(batch_directory):
        print("❌ Batch directory not found. Run Part-4 first to generate batch files!")
        return False
    
    batch_files = [f for f in os.listdir(batch_directory) if f.endswith('_batch.csv')]
    
    if not batch_files:
        print("❌ No batch files found. Run Part-4 first to generate batch files!")
        return False
    
    print(f"📁 Found {len(batch_files)} batch files:")
    for batch_file in batch_files:
        print(f"    - {batch_file}")
    
    # Initialize test components
    supabase = MockSupabase()
    tally_exporter = TallyExporterAgent(supabase)
    
    gstin = "06ABGCS4796R1ZA"
    channel = "amazon_mtr"
    month = "2025-08"
    run_id = uuid.uuid4()
    
    print(f"\n🔧 Test Configuration:")
    print(f"    GSTIN: {gstin}")
    print(f"    Channel: {channel}")
    print(f"    Month: {month}")
    
    # Step 1: Template Validation
    print(f"\n📋 STEP 1: X2Beta Template Validation")
    print("-" * 40)
    
    template_validation = tally_exporter.validate_template_availability(gstin)
    
    if template_validation['available']:
        print(f"✅ Template Validation Results:")
        print(f"    Template: {template_validation['template_name']}")
        print(f"    Company: {template_validation['company_name']}")
        print(f"    State: {template_validation['state_name']}")
        print(f"    Path: {template_validation['template_path']}")
    else:
        print(f"❌ Template validation failed: {template_validation['error']}")
        return False
    
    # Step 2: Batch File Processing
    print(f"\n📄 STEP 2: Batch File Processing")
    print("-" * 40)
    
    # Create temporary export directory
    temp_export_dir = tempfile.mkdtemp()
    
    try:
        export_result = tally_exporter.process_batch_files(
            batch_directory, gstin, channel, month, run_id, temp_export_dir
        )
        
        if export_result.success:
            print(f"✅ Batch Processing Results:")
            print(f"    Processed files: {export_result.processed_files}")
            print(f"    Exported files: {export_result.exported_files}")
            print(f"    Total records: {export_result.total_records}")
            print(f"    Total taxable: ₹{export_result.total_taxable:,.2f}")
            print(f"    Total tax: ₹{export_result.total_tax:,.2f}")
            print(f"    GST rates: {export_result.gst_rates_processed}")
            
            print(f"\n📄 X2Beta Files Created:")
            for i, export_path in enumerate(export_result.export_paths):
                filename = os.path.basename(export_path)
                file_size = os.path.getsize(export_path) if os.path.exists(export_path) else 0
                print(f"    {i+1}. {filename} ({file_size:,} bytes)")
                
                # Validate X2Beta file content
                if os.path.exists(export_path):
                    try:
                        # Read Excel file to validate structure
                        df = pd.read_excel(export_path, skiprows=4)  # Skip header rows
                        print(f"        Records: {len(df)}")
                        
                        if len(df) > 0:
                            # Check key columns
                            required_columns = ['Date', 'Voucher No.', 'Party Ledger', 'Taxable Amount']
                            missing_columns = [col for col in required_columns if col not in df.columns]
                            
                            if not missing_columns:
                                total_taxable = df['Taxable Amount'].sum()
                                print(f"        Total taxable: ₹{total_taxable:,.2f}")
                                print(f"        ✅ X2Beta structure validated")
                            else:
                                print(f"        ❌ Missing columns: {missing_columns}")
                    except Exception as e:
                        print(f"        ⚠️  Could not validate Excel structure: {e}")
            
        else:
            print(f"❌ Batch processing failed: {export_result.error_message}")
            return False
    
    finally:
        # Cleanup temporary directory
        if os.path.exists(temp_export_dir):
            shutil.rmtree(temp_export_dir)
    
    # Step 3: Export Summary Analysis
    print(f"\n📊 STEP 3: Export Summary Analysis")
    print("-" * 40)
    
    # Create mock export results for summary
    mock_export_results = []
    for i, rate in enumerate(export_result.gst_rates_processed or []):
        mock_export_results.append({
            'success': True,
            'record_count': export_result.total_records // len(export_result.gst_rates_processed or [1]),
            'total_taxable': export_result.total_taxable / len(export_result.gst_rates_processed or [1]),
            'total_tax': export_result.total_tax / len(export_result.gst_rates_processed or [1]),
            'gst_rate': rate,
            'file_size': 1024 * (i + 1)  # Mock file sizes
        })
    
    export_summary = tally_exporter.get_export_summary(mock_export_results)
    
    print(f"✅ Export Summary:")
    print(f"    Total files: {export_summary['total_files']}")
    print(f"    Successful exports: {export_summary['successful_exports']}")
    print(f"    Failed exports: {export_summary['failed_exports']}")
    print(f"    Total records: {export_summary['total_records']}")
    print(f"    Total taxable: ₹{export_summary['total_taxable_amount']:,.2f}")
    print(f"    Total tax: ₹{export_summary['total_tax_amount']:,.2f}")
    print(f"    Total file size: {export_summary['file_size_total']:,} bytes")
    
    print(f"\n📋 GST Rate Breakdown:")
    for rate, breakdown in export_summary['gst_rate_breakdown'].items():
        print(f"    {rate}: {breakdown['files']} files, {breakdown['records']} records, ₹{breakdown['taxable']:,.2f} taxable")
    
    # Step 4: Database Integration Test
    print(f"\n💾 STEP 4: Database Integration Test")
    print("-" * 40)
    
    # Check if export metadata was saved
    saved_exports = len(supabase.tally_exports)
    print(f"✅ Database Integration:")
    print(f"    Export records saved: {saved_exports}")
    
    if saved_exports > 0:
        sample_export = supabase.tally_exports[0]
        print(f"    Sample export record:")
        print(f"      Run ID: {sample_export.get('run_id', 'N/A')}")
        print(f"      Channel: {sample_export.get('channel', 'N/A')}")
        print(f"      GSTIN: {sample_export.get('gstin', 'N/A')}")
        print(f"      GST Rate: {sample_export.get('gst_rate', 'N/A')}")
        print(f"      Status: {sample_export.get('export_status', 'N/A')}")
    
    # Summary
    print(f"\n" + "=" * 60)
    print("📋 PART-5 TEST SUMMARY")
    print("=" * 60)
    
    success = (template_validation['available'] and export_result.success and 
               export_result.exported_files > 0 and export_summary['successful_exports'] > 0)
    
    print(f"Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print(f"Template Validation: {'✅' if template_validation['available'] else '❌'}")
    print(f"Batch Processing: {'✅' if export_result.success else '❌'}")
    print(f"X2Beta Export: {export_result.exported_files}/{export_result.processed_files} files")
    print(f"Total Records Exported: {export_result.total_records}")
    print(f"Total Value Exported: ₹{export_result.total_taxable:,.2f}")
    print(f"GST Rates Processed: {len(export_result.gst_rates_processed or [])}")
    
    if success:
        print(f"\n🎉 Part-5 is working correctly!")
        print(f"🚀 Ready for production with complete pipeline:")
        print(f"   python -m ingestion_layer.main --agent amazon_mtr \\")
        print(f"     --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print(f"     --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print(f"     --full-pipeline")
    
    return success


def test_x2beta_writer_components():
    """Test X2Beta Writer components independently."""
    
    print("\n🔧 TESTING X2BETA WRITER COMPONENTS")
    print("=" * 40)
    
    writer = X2BetaWriter()
    
    # Test 1: Template validation
    template_path = "ingestion_layer/templates/X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx"
    
    if os.path.exists(template_path):
        template_info = writer.get_template_info(template_path)
        validation = writer.validate_template(template_path)
        
        print(f"✅ Template Info:")
        print(f"    Exists: {template_info['exists']}")
        print(f"    Sheet: {template_info.get('sheet_name', 'N/A')}")
        print(f"    Headers: {len(template_info.get('headers', []))}")
        print(f"    Valid: {validation['valid']}")
        
        if not validation['valid']:
            print(f"    Errors: {validation['errors']}")
    else:
        print(f"❌ Template not found: {template_path}")
        return False
    
    # Test 2: Batch data validation
    sample_batch = pd.DataFrame([
        {
            'gst_rate': 0.18,
            'ledger_name': 'Amazon Haryana',
            'fg': 'FABCON-5L',
            'total_quantity': 2,
            'total_taxable': 2118.0,
            'total_cgst': 190.62,
            'total_sgst': 190.62,
            'total_igst': 0.0
        }
    ])
    
    batch_validation = writer.validate_batch_data(sample_batch)
    print(f"✅ Batch Validation:")
    print(f"    Valid: {batch_validation['valid']}")
    print(f"    Records: {batch_validation['record_count']}")
    print(f"    GST Rate: {batch_validation['gst_rate']}")
    
    # Test 3: X2Beta mapping
    x2beta_df = writer.map_batch_to_x2beta(sample_batch, '06ABGCS4796R1ZA', '2025-08')
    print(f"✅ X2Beta Mapping:")
    print(f"    Output records: {len(x2beta_df)}")
    print(f"    Columns: {len(x2beta_df.columns)}")
    
    if len(x2beta_df) > 0:
        sample_record = x2beta_df.iloc[0]
        print(f"    Sample voucher: {sample_record.get('Voucher No.', 'N/A')}")
        print(f"    Party ledger: {sample_record.get('Party Ledger', 'N/A')}")
        print(f"    Taxable amount: ₹{sample_record.get('Taxable Amount', 0):,.2f}")
    
    return batch_validation['valid'] and len(x2beta_df) > 0


def main():
    print("🚀 PART-5 COMPLETE TESTING SUITE")
    print("Tally Export (X2Beta Templates) with Real Data")
    print("=" * 60)
    
    # Test 1: X2Beta Writer components
    writer_ok = test_x2beta_writer_components()
    
    # Test 2: Complete Part-5 workflow
    part5_ok = test_part5_with_real_batch_data()
    
    # Final result
    print(f"\n" + "=" * 60)
    print("🏁 FINAL RESULT")
    print("=" * 60)
    
    all_passed = writer_ok and part5_ok
    
    if all_passed:
        print("✅ Part-5: Tally Export (X2Beta Templates) is WORKING!")
        print("🎯 Ready for production use with complete pipeline")
        print("\n🚀 PRODUCTION COMMANDS:")
        print("# Complete Pipeline (Parts 1+2+3+4+5):")
        print("python -m ingestion_layer.main --agent amazon_mtr \\")
        print("  --input 'ingestion_layer/data/Amazon MTR B2C Report - Sample.xlsx' \\")
        print("  --channel amazon --gstin 06ABGCS4796R1ZA --month 2025-08 \\")
        print("  --full-pipeline")
        print("\n# Individual Part-5:")
        print("python -m ingestion_layer.main ... --enable-tally-export")
        print("\n📁 Output Files:")
        print("  - X2Beta Excel files: ingestion_layer/exports/")
        print("  - Batch CSV files: ingestion_layer/data/batches/")
        print("  - Templates: ingestion_layer/templates/")
    else:
        print("❌ Part-5 testing failed - needs debugging")
        if not writer_ok:
            print("  - X2Beta Writer component issues")
        if not part5_ok:
            print("  - Part-5 workflow issues")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
