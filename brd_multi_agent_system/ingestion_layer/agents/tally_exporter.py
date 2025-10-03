"""
Tally Exporter Agent
Converts batch CSV files (from Part-4) into X2Beta Excel templates for Tally import
"""
import os
import pandas as pd
import uuid
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from ..libs.contracts import BatchSplitResult
from ..libs.utils import ensure_dir
from ..libs.x2beta_writer import X2BetaWriter
from ..libs.supabase_client import SupabaseClientWrapper
from ..libs.csv_utils import safe_read_csv


@dataclass
class TallyExportResult:
    """Result of Tally export operation."""
    success: bool
    processed_files: int
    exported_files: int
    total_records: int
    total_taxable: float
    total_tax: float
    export_paths: List[str]
    error_message: Optional[str] = None
    gstin: Optional[str] = None
    gst_rates_processed: List[float] = None


class TallyExporterAgent:
    """
    Agent for exporting batch CSV files to X2Beta Excel templates.
    
    Processes GST rate-wise batch files and converts them to Tally-compatible
    Excel files using appropriate X2Beta templates per GSTIN.
    """
    
    def __init__(self, supabase: SupabaseClientWrapper):
        self.supabase = supabase
        self.x2beta_writer = X2BetaWriter()
        
        # Template configuration
        self.template_configs = {
            '06ABGCS4796R1ZA': {
                'template_name': 'X2Beta Sales Template - 06ABGCS4796R1ZA.xlsx',
                'company_name': 'Zaggle Haryana Private Limited',
                'state_name': 'HARYANA'
            },
            '07ABGCS4796R1Z8': {
                'template_name': 'X2Beta Sales Template - 07ABGCS4796R1Z8.xlsx',
                'company_name': 'Zaggle Delhi Private Limited',
                'state_name': 'DELHI'
            },
            '09ABGCS4796R1Z4': {
                'template_name': 'X2Beta Sales Template - 09ABGCS4796R1Z4.xlsx',
                'company_name': 'Zaggle Uttar Pradesh Private Limited',
                'state_name': 'UTTAR PRADESH'
            },
            '24ABGCS4796R1ZC': {
                'template_name': 'X2Beta Sales Template - 24ABGCS4796R1ZC.xlsx',
                'company_name': 'Zaggle Gujarat Private Limited',
                'state_name': 'GUJARAT'
            },
            '29ABGCS4796R1Z2': {
                'template_name': 'X2Beta Sales Template - 29ABGCS4796R1Z2.xlsx',
                'company_name': 'Zaggle Karnataka Private Limited',
                'state_name': 'KARNATAKA'
            }
        }
    
    def process_batch_files(self, 
                           batch_directory: str,
                           gstin: str,
                           channel: str,
                           month: str,
                           run_id: uuid.UUID,
                           output_directory: Optional[str] = None) -> TallyExportResult:
        """
        Process all batch files in a directory and export to X2Beta templates.
        
        Args:
            batch_directory: Directory containing GST rate-wise batch CSV files
            gstin: Company GSTIN
            channel: Sales channel (amazon_mtr, flipkart, etc.)
            month: Processing month (YYYY-MM format)
            run_id: Processing run ID
            output_directory: Directory for output Excel files (default: ingestion_layer/exports)
            
        Returns:
            TallyExportResult with processing summary
        """
        print(f"🏭 Starting Tally export for GSTIN: {gstin}")
        
        if output_directory is None:
            output_directory = "ingestion_layer/exports"
        
        try:
            # Find batch files for this GSTIN and channel
            batch_files = self._find_batch_files(batch_directory, gstin, channel, month)
            
            if not batch_files:
                return TallyExportResult(
                    success=False,
                    processed_files=0,
                    exported_files=0,
                    total_records=0,
                    total_taxable=0.0,
                    total_tax=0.0,
                    export_paths=[],
                    error_message=f"No batch files found for {gstin} in {batch_directory}",
                    gstin=gstin
                )
            
            print(f"📁 Found {len(batch_files)} batch files to process")
            
            # Get template configuration
            template_config = self.template_configs.get(gstin)
            if not template_config:
                return TallyExportResult(
                    success=False,
                    processed_files=0,
                    exported_files=0,
                    total_records=0,
                    total_taxable=0.0,
                    total_tax=0.0,
                    export_paths=[],
                    error_message=f"No X2Beta template configuration found for GSTIN: {gstin}",
                    gstin=gstin
                )
            
            # Process each batch file
            export_results = []
            total_records = 0
            total_taxable = 0.0
            total_tax = 0.0
            gst_rates_processed = []
            
            for batch_file in batch_files:
                print(f"📄 Processing batch file: {os.path.basename(batch_file)}")
                
                result = self._process_single_batch_file(
                    batch_file, gstin, channel, month, run_id, 
                    template_config, output_directory
                )
                
                export_results.append(result)
                
                if result['success']:
                    total_records += result['record_count']
                    total_taxable += result['total_taxable']
                    total_tax += result['total_tax']
                    gst_rates_processed.append(result['gst_rate'])
                    print(f"    ✅ Exported: {os.path.basename(result['output_path'])}")
                else:
                    print(f"    ❌ Failed: {result['error']}")
            
            # Save export metadata to database
            successful_exports = [r for r in export_results if r['success']]
            self._save_export_metadata(successful_exports, run_id)
            
            return TallyExportResult(
                success=len(successful_exports) > 0,
                processed_files=len(batch_files),
                exported_files=len(successful_exports),
                total_records=total_records,
                total_taxable=total_taxable,
                total_tax=total_tax,
                export_paths=[r['output_path'] for r in successful_exports],
                gstin=gstin,
                gst_rates_processed=gst_rates_processed
            )
            
        except Exception as e:
            return TallyExportResult(
                success=False,
                processed_files=0,
                exported_files=0,
                total_records=0,
                total_taxable=0.0,
                total_tax=0.0,
                export_paths=[],
                error_message=str(e),
                gstin=gstin
            )
    
    def _find_batch_files(self, 
                         batch_directory: str, 
                         gstin: str, 
                         channel: str, 
                         month: str) -> List[str]:
        """
        Find batch CSV files for specific GSTIN, channel, and month.
        
        Args:
            batch_directory: Directory to search
            gstin: Company GSTIN
            channel: Sales channel
            month: Processing month
            
        Returns:
            List of batch file paths
        """
        if not os.path.exists(batch_directory):
            return []
        
        batch_files = []
        
        # Expected pattern: {channel}_{gstin}_{month}_{rate}pct_batch.csv
        for filename in os.listdir(batch_directory):
            if (filename.startswith(f"{channel}_{gstin}_{month}") and 
                filename.endswith("_batch.csv")):
                batch_files.append(os.path.join(batch_directory, filename))
        
        return sorted(batch_files)
    
    def _process_single_batch_file(self, 
                                  batch_file: str,
                                  gstin: str,
                                  channel: str,
                                  month: str,
                                  run_id: uuid.UUID,
                                  template_config: Dict,
                                  output_directory: str) -> Dict:
        """
        Process a single batch CSV file and export to X2Beta Excel.
        
        Args:
            batch_file: Path to batch CSV file
            gstin: Company GSTIN
            channel: Sales channel
            month: Processing month
            run_id: Processing run ID
            template_config: X2Beta template configuration
            output_directory: Output directory for Excel files
            
        Returns:
            Processing result dictionary
        """
        try:
            # Load batch data
            df = safe_read_csv(batch_file)
            
            # Validate batch data
            validation = self.x2beta_writer.validate_batch_data(df)
            if not validation['valid']:
                return {
                    'success': False,
                    'error': f"Batch validation failed: {validation['errors']}",
                    'batch_file': batch_file,
                    'record_count': 0,
                    'total_taxable': 0.0,
                    'total_tax': 0.0,
                    'gst_rate': None
                }
            
            gst_rate = validation['gst_rate']
            
            # Map batch data to X2Beta format
            x2beta_df = self.x2beta_writer.map_batch_to_x2beta(df, gstin, month)
            
            # Generate output filename
            rate_str = f"{int(gst_rate * 100)}pct" if gst_rate > 0 else "0pct"
            output_filename = f"{channel}_{gstin}_{month}_{rate_str}_x2beta.xlsx"
            output_path = os.path.join(output_directory, output_filename)
            
            # Get template path
            template_path = os.path.join("ingestion_layer/templates", template_config['template_name'])
            
            # Write to X2Beta template
            write_result = self.x2beta_writer.write_to_template(
                x2beta_df, template_path, output_path
            )
            
            if write_result['success']:
                return {
                    'success': True,
                    'batch_file': batch_file,
                    'output_path': output_path,
                    'template_name': template_config['template_name'],
                    'record_count': write_result['record_count'],
                    'total_taxable': write_result['total_taxable'],
                    'total_tax': write_result['total_tax'],
                    'file_size': write_result['file_size'],
                    'gst_rate': gst_rate,
                    'gstin': gstin,
                    'channel': channel,
                    'month': month
                }
            else:
                return {
                    'success': False,
                    'error': write_result['error'],
                    'batch_file': batch_file,
                    'record_count': 0,
                    'total_taxable': 0.0,
                    'total_tax': 0.0,
                    'gst_rate': gst_rate
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'batch_file': batch_file,
                'record_count': 0,
                'total_taxable': 0.0,
                'total_tax': 0.0,
                'gst_rate': None
            }
    
    def _save_export_metadata(self, export_results: List[Dict], run_id: uuid.UUID):
        """
        Save export metadata to Supabase database.
        
        Args:
            export_results: List of successful export results
            run_id: Processing run ID
        """
        try:
            export_records = []
            
            for result in export_results:
                export_record = {
                    'run_id': str(run_id),
                    'channel': result['channel'],
                    'gstin': result['gstin'],
                    'month': result['month'],
                    'gst_rate': result['gst_rate'],
                    'template_name': result['template_name'],
                    'file_path': result['output_path'],
                    'file_size': result['file_size'],
                    'record_count': result['record_count'],
                    'total_taxable': result['total_taxable'],
                    'total_tax': result['total_tax'],
                    'export_status': 'success'
                }
                export_records.append(export_record)
            
            if export_records:
                response = self.supabase.client.table('tally_exports').insert(export_records).execute()
                print(f"💾 Saved {len(export_records)} export records to database")
                
        except Exception as e:
            print(f"⚠️  Failed to save export metadata: {e}")
    
    def export_single_batch(self, 
                           batch_file: str,
                           gstin: str,
                           output_directory: Optional[str] = None) -> Dict:
        """
        Export a single batch file to X2Beta Excel format.
        
        Args:
            batch_file: Path to batch CSV file
            gstin: Company GSTIN
            output_directory: Output directory (default: ingestion_layer/exports)
            
        Returns:
            Export result dictionary
        """
        if output_directory is None:
            output_directory = "ingestion_layer/exports"
        
        # Extract metadata from filename
        filename = os.path.basename(batch_file)
        parts = filename.replace('.csv', '').split('_')
        
        if len(parts) >= 4:
            channel = parts[0]
            month = parts[2]
        else:
            return {
                'success': False,
                'error': f"Invalid batch filename format: {filename}"
            }
        
        # Get template configuration
        template_config = self.template_configs.get(gstin)
        if not template_config:
            return {
                'success': False,
                'error': f"No template configuration for GSTIN: {gstin}"
            }
        
        # Process the file
        run_id = uuid.uuid4()
        return self._process_single_batch_file(
            batch_file, gstin, channel, month, run_id, 
            template_config, output_directory
        )
    
    def get_export_summary(self, export_results: List[Dict]) -> Dict:
        """
        Generate summary statistics from export results.
        
        Args:
            export_results: List of export result dictionaries
            
        Returns:
            Summary statistics
        """
        if not export_results:
            return {
                'total_files': 0,
                'successful_exports': 0,
                'failed_exports': 0,
                'total_records': 0,
                'total_taxable_amount': 0.0,
                'total_tax_amount': 0.0,
                'gst_rate_breakdown': {},
                'file_size_total': 0
            }
        
        successful = [r for r in export_results if r.get('success', False)]
        failed = [r for r in export_results if not r.get('success', False)]
        
        total_records = sum(r.get('record_count', 0) for r in successful)
        total_taxable = sum(r.get('total_taxable', 0.0) for r in successful)
        total_tax = sum(r.get('total_tax', 0.0) for r in successful)
        total_file_size = sum(r.get('file_size', 0) for r in successful)
        
        # GST rate breakdown
        gst_breakdown = {}
        for result in successful:
            gst_rate = result.get('gst_rate', 0.0)
            rate_key = f"{gst_rate * 100:.0f}%"
            
            if rate_key not in gst_breakdown:
                gst_breakdown[rate_key] = {
                    'files': 0,
                    'records': 0,
                    'taxable': 0.0,
                    'tax': 0.0
                }
            
            gst_breakdown[rate_key]['files'] += 1
            gst_breakdown[rate_key]['records'] += result.get('record_count', 0)
            gst_breakdown[rate_key]['taxable'] += result.get('total_taxable', 0.0)
            gst_breakdown[rate_key]['tax'] += result.get('total_tax', 0.0)
        
        return {
            'total_files': len(export_results),
            'successful_exports': len(successful),
            'failed_exports': len(failed),
            'total_records': total_records,
            'total_taxable_amount': total_taxable,
            'total_tax_amount': total_tax,
            'gst_rate_breakdown': gst_breakdown,
            'file_size_total': total_file_size
        }
    
    def validate_template_availability(self, gstin: str) -> Dict:
        """
        Validate that X2Beta template is available for the given GSTIN.
        
        Args:
            gstin: Company GSTIN
            
        Returns:
            Validation result
        """
        template_config = self.template_configs.get(gstin)
        
        if not template_config:
            return {
                'available': False,
                'error': f"No template configuration for GSTIN: {gstin}"
            }
        
        template_path = os.path.join("ingestion_layer/templates", template_config['template_name'])
        
        if not os.path.exists(template_path):
            return {
                'available': False,
                'error': f"Template file not found: {template_path}"
            }
        
        # Validate template structure
        template_validation = self.x2beta_writer.validate_template(template_path)
        
        return {
            'available': template_validation['valid'],
            'template_path': template_path,
            'template_name': template_config['template_name'],
            'company_name': template_config['company_name'],
            'state_name': template_config['state_name'],
            'validation_errors': template_validation.get('errors', [])
        }
