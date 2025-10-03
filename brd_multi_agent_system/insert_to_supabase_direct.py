#!/usr/bin/env python3
"""
Direct insertion to Supabase tally_exports table
Run this after setting up .env file with Supabase credentials
"""
import sys
import os
import pandas as pd
import uuid
from datetime import datetime
sys.path.append('.')


def insert_to_supabase_direct():
    """Insert tally_exports records directly to Supabase."""
    
    print("🗄️  DIRECT SUPABASE INSERTION")
    print("=" * 40)
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("❌ No .env file found!")
        print("📋 To enable direct Supabase insertion:")
        print("1. Copy .env.template to .env")
        print("2. Fill in your Supabase URL and key")
        print("3. Run this script again")
        print("\n📝 Get your Supabase credentials from:")
        print("   Project Settings → API → Project URL & anon public key")
        return False
    
    # Try to connect to Supabase
    try:
        from dotenv import load_dotenv
        from supabase import create_client
        
        load_dotenv()
        
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            print("❌ Missing Supabase credentials in .env file!")
            print("📋 Make sure .env contains:")
            print("   SUPABASE_URL=https://your-project.supabase.co")
            print("   SUPABASE_KEY=your-anon-key")
            return False
        
        # Create Supabase client
        supabase = create_client(url, key)
        print(f"✅ Connected to Supabase: {url[:30]}...")
        
    except ImportError:
        print("❌ Supabase library not installed!")
        print("📋 Install with: pip install supabase")
        return False
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return False
    
    # Check X2Beta files
    export_dir = "ingestion_layer/exports"
    if not os.path.exists(export_dir):
        print("❌ No exports directory found")
        return False
    
    export_files = [f for f in os.listdir(export_dir) if f.endswith('_x2beta.xlsx')]
    if not export_files:
        print("❌ No X2Beta export files found")
        return False
    
    print(f"📄 Found {len(export_files)} X2Beta files to insert")
    
    # Generate run_id
    run_id = str(uuid.uuid4())
    
    # Process each file and insert
    inserted_count = 0
    
    for export_file in export_files:
        try:
            file_path = os.path.join(export_dir, export_file)
            file_size = os.path.getsize(file_path)
            
            # Read Excel data
            df = pd.read_excel(file_path)
            record_count = len(df)
            
            # Extract metadata from filename
            parts = export_file.replace('_x2beta.xlsx', '').split('_')
            channel = f"{parts[0]}_{parts[1]}"  # amazon_mtr
            gstin = parts[2]  # 06ABGCS4796R1ZA
            month = parts[3]  # 2025-08
            gst_rate_str = parts[4]  # 18pct or 0pct
            
            # Convert GST rate
            gst_rate = 0.18 if gst_rate_str == '18pct' else 0.0
            
            # Calculate totals
            total_taxable = float(df['Taxable Amount'].sum()) if 'Taxable Amount' in df.columns else 0.0
            
            total_tax = 0.0
            for col in ['CGST Amount', 'SGST Amount', 'IGST Amount']:
                if col in df.columns:
                    total_tax += float(df[col].sum())
            
            # Prepare record
            record = {
                "id": str(uuid.uuid4()),
                "run_id": run_id,
                "channel": channel,
                "gstin": gstin,
                "month": month,
                "gst_rate": gst_rate,
                "template_name": f"X2Beta Sales Template - {gstin}.xlsx",
                "file_path": f"ingestion_layer/exports/{export_file}",
                "file_size": file_size,
                "record_count": record_count,
                "total_taxable": total_taxable,
                "total_tax": total_tax,
                "export_status": "success",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            print(f"📊 Inserting: {export_file}")
            print(f"    GST Rate: {gst_rate*100:.0f}%")
            print(f"    Records: {record_count}")
            print(f"    Taxable: ₹{total_taxable:,.2f}")
            print(f"    Tax: ₹{total_tax:,.2f}")
            
            # Insert to Supabase
            result = supabase.table("tally_exports").insert(record).execute()
            
            if result.data:
                print(f"    ✅ Successfully inserted to tally_exports table")
                inserted_count += 1
            else:
                print(f"    ❌ Insert failed - no data returned")
                
        except Exception as e:
            print(f"❌ Error processing {export_file}: {e}")
            continue
    
    # Summary
    print(f"\n📊 INSERTION SUMMARY:")
    print(f"    Files processed: {len(export_files)}")
    print(f"    Records inserted: {inserted_count}")
    print(f"    Run ID: {run_id}")
    
    if inserted_count > 0:
        print(f"\n🎉 SUCCESS!")
        print(f"✅ {inserted_count} records inserted to Supabase tally_exports table")
        print(f"🔍 Go to your Supabase table editor and refresh tally_exports")
        print(f"📊 You should see {inserted_count} new records with run_id: {run_id[:8]}...")
        return True
    else:
        print(f"\n❌ No records inserted")
        return False


def main():
    print("🚀 SUPABASE DIRECT INSERTION")
    print("Insert pipeline results directly to tally_exports table")
    print("=" * 60)
    
    success = insert_to_supabase_direct()
    
    if success:
        print(f"\n🎉 COMPLETE!")
        print(f"Your tally_exports table now contains the pipeline results")
        print(f"The 'empty table' issue is resolved!")
    else:
        print(f"\n⚠️  Setup needed")
        print(f"Follow the instructions above to enable direct Supabase insertion")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
