#!/usr/bin/env python3
"""
Populate tally_exports table with data from successful pipeline run
"""
import sys
import os
import pandas as pd
import uuid
from datetime import datetime
sys.path.append('.')


def populate_tally_exports_table():
    """Generate SQL to populate tally_exports table with actual pipeline results."""
    
    print("📊 POPULATING TALLY_EXPORTS TABLE")
    print("Based on successful pipeline execution")
    print("=" * 50)
    
    # Check the X2Beta files that were created
    export_dir = "ingestion_layer/exports"
    if not os.path.exists(export_dir):
        print("❌ No exports directory found")
        return False
    
    export_files = [f for f in os.listdir(export_dir) if f.endswith('_x2beta.xlsx')]
    if not export_files:
        print("❌ No X2Beta export files found")
        return False
    
    print(f"✅ Found {len(export_files)} X2Beta export files")
    
    # Generate mock run_id (in production this would be from the actual pipeline run)
    run_id = str(uuid.uuid4())
    
    # Prepare SQL statements
    sql_statements = []
    sql_statements.append("-- Populate tally_exports table with pipeline results")
    sql_statements.append(f"-- Generated on: {datetime.now().isoformat()}")
    sql_statements.append("")
    
    records_data = []
    
    for export_file in export_files:
        try:
            file_path = os.path.join(export_dir, export_file)
            file_size = os.path.getsize(file_path)
            
            # Read the Excel file to get actual data
            df = pd.read_excel(file_path)
            record_count = len(df)
            
            # Extract metadata from filename
            # Format: amazon_mtr_06ABGCS4796R1ZA_2025-08_18pct_x2beta.xlsx
            parts = export_file.replace('_x2beta.xlsx', '').split('_')
            
            channel = f"{parts[0]}_{parts[1]}"  # amazon_mtr
            gstin = parts[2]  # 06ABGCS4796R1ZA
            month = parts[3]  # 2025-08
            gst_rate_str = parts[4]  # 18pct or 0pct
            
            # Convert GST rate
            if gst_rate_str == '0pct':
                gst_rate = 0.0
            elif gst_rate_str == '18pct':
                gst_rate = 0.18
            else:
                gst_rate = 0.0
            
            # Calculate totals from Excel data
            total_taxable = 0.0
            total_tax = 0.0
            
            if 'Taxable Amount' in df.columns:
                total_taxable = float(df['Taxable Amount'].sum())
            
            # Calculate tax amounts
            tax_columns = ['CGST Amount', 'SGST Amount', 'IGST Amount']
            for col in tax_columns:
                if col in df.columns:
                    total_tax += float(df[col].sum())
            
            # Template name
            template_name = f"X2Beta Sales Template - {gstin}.xlsx"
            
            # Create record
            record = {
                'id': str(uuid.uuid4()),
                'run_id': run_id,
                'channel': channel,
                'gstin': gstin,
                'month': month,
                'gst_rate': gst_rate,
                'template_name': template_name,
                'file_path': f'ingestion_layer/exports/{export_file}',
                'file_size': file_size,
                'record_count': record_count,
                'total_taxable': total_taxable,
                'total_tax': total_tax,
                'export_status': 'success',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            records_data.append(record)
            
            print(f"📄 {export_file}:")
            print(f"    Records: {record_count}")
            print(f"    GST Rate: {gst_rate*100:.0f}%")
            print(f"    Total Taxable: ₹{total_taxable:,.2f}")
            print(f"    Total Tax: ₹{total_tax:,.2f}")
            
        except Exception as e:
            print(f"❌ Error processing {export_file}: {e}")
            continue
    
    # Generate SQL INSERT statements
    if records_data:
        print(f"\n📝 Generating SQL INSERT statements...")
        
        sql_statements.append("INSERT INTO public.tally_exports (")
        sql_statements.append("    id, run_id, channel, gstin, month, gst_rate,")
        sql_statements.append("    template_name, file_path, file_size, record_count,")
        sql_statements.append("    total_taxable, total_tax, export_status, created_at, updated_at")
        sql_statements.append(") VALUES")
        
        for i, record in enumerate(records_data):
            comma = "," if i < len(records_data) - 1 else ";"
            
            sql_line = f"    ('{record['id']}', '{record['run_id']}', '{record['channel']}', '{record['gstin']}', '{record['month']}', {record['gst_rate']}, '{record['template_name']}', '{record['file_path']}', {record['file_size']}, {record['record_count']}, {record['total_taxable']}, {record['total_tax']}, '{record['export_status']}', '{record['created_at']}', '{record['updated_at']}'){comma}"
            
            sql_statements.append(sql_line)
        
        # Write SQL to file
        sql_file = "populate_tally_exports.sql"
        with open(sql_file, 'w') as f:
            f.write('\n'.join(sql_statements))
        
        print(f"✅ SQL file created: {sql_file}")
        
        # Display the SQL
        print(f"\n📋 SQL TO RUN IN SUPABASE:")
        print("=" * 60)
        for line in sql_statements:
            print(line)
        
        print(f"\n" + "=" * 60)
        print(f"📊 SUMMARY:")
        print(f"    Records to insert: {len(records_data)}")
        print(f"    Total taxable: ₹{sum(r['total_taxable'] for r in records_data):,.2f}")
        print(f"    Total tax: ₹{sum(r['total_tax'] for r in records_data):,.2f}")
        
        print(f"\n🚀 INSTRUCTIONS:")
        print(f"1. Copy the SQL above")
        print(f"2. Go to your Supabase SQL Editor")
        print(f"3. Paste and run the SQL")
        print(f"4. Refresh the tally_exports table")
        print(f"5. You should see {len(records_data)} records!")
        
        return True
    else:
        print(f"❌ No valid records found")
        return False


def main():
    print("🗄️  SUPABASE TALLY_EXPORTS TABLE POPULATION")
    print("Converting pipeline results to database records")
    print("=" * 70)
    
    success = populate_tally_exports_table()
    
    if success:
        print(f"\n✅ SUCCESS!")
        print(f"SQL generated to populate your tally_exports table")
        print(f"After running the SQL, your table will show the pipeline results")
    else:
        print(f"\n❌ FAILED!")
        print(f"Could not generate SQL - check if X2Beta files exist")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
