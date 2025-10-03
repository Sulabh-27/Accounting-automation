"""
PDF utilities for parsing seller invoices and credit notes
Supports both text-based and scanned PDFs
"""
import re
import pandas as pd
from typing import Dict, List, Optional, Union
from datetime import datetime
import logging

try:
    import pdfplumber
    PDF_PLUMBER_AVAILABLE = True
except ImportError:
    PDF_PLUMBER_AVAILABLE = False
    logging.warning("pdfplumber not available - install with: pip install pdfplumber")

try:
    import pytesseract
    from PIL import Image
    import fitz  # PyMuPDF
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    logging.warning("OCR libraries not available - install with: pip install pytesseract PyMuPDF Pillow")


class PDFParser:
    """PDF parser for seller invoices with OCR fallback."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using pdfplumber with OCR fallback."""
        
        if not PDF_PLUMBER_AVAILABLE:
            raise ImportError("pdfplumber is required for PDF parsing")
        
        try:
            # Try pdfplumber first (for text-based PDFs)
            with pdfplumber.open(pdf_path) as pdf:
                text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                
                # If we got substantial text, return it
                if len(text.strip()) > 100:
                    self.logger.info(f"Extracted text from PDF using pdfplumber: {len(text)} characters")
                    return text
        
        except Exception as e:
            self.logger.warning(f"pdfplumber failed: {e}")
        
        # Fallback to OCR for scanned PDFs
        if OCR_AVAILABLE:
            try:
                return self._extract_text_with_ocr(pdf_path)
            except Exception as e:
                self.logger.error(f"OCR extraction failed: {e}")
        
        raise ValueError(f"Could not extract text from PDF: {pdf_path}")
    
    def _extract_text_with_ocr(self, pdf_path: str) -> str:
        """Extract text using OCR for scanned PDFs."""
        
        text = ""
        doc = fitz.open(pdf_path)
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            pix = page.get_pixmap()
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            
            # Use OCR to extract text
            page_text = pytesseract.image_to_string(img)
            text += page_text + "\n"
        
        doc.close()
        self.logger.info(f"Extracted text from PDF using OCR: {len(text)} characters")
        return text
    
    def parse_amazon_fee_invoice(self, pdf_text: str) -> Dict:
        """Parse Amazon fee invoice from extracted text."""
        
        invoice_data = {
            'invoice_no': None,
            'invoice_date': None,
            'gstin': None,
            'line_items': []
        }
        
        # Extract invoice number
        invoice_patterns = [
            r'Invoice\s+(?:No\.?|Number)\s*:?\s*([A-Z0-9\-]+)',
            r'Invoice\s+([A-Z0-9\-]+)',
            r'Bill\s+(?:No\.?|Number)\s*:?\s*([A-Z0-9\-]+)'
        ]
        
        for pattern in invoice_patterns:
            match = re.search(pattern, pdf_text, re.IGNORECASE)
            if match:
                invoice_data['invoice_no'] = match.group(1).strip()
                break
        
        # Extract invoice date
        date_patterns = [
            r'Invoice\s+Date\s*:?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
            r'Date\s*:?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
            r'Bill\s+Date\s*:?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, pdf_text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    # Try different date formats
                    for fmt in ['%d-%m-%Y', '%d/%m/%Y', '%d-%m-%y', '%d/%m/%y']:
                        try:
                            invoice_data['invoice_date'] = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                except:
                    pass
                break
        
        # Extract GSTIN
        gstin_pattern = r'GSTIN\s*:?\s*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9][Z][0-9])'
        match = re.search(gstin_pattern, pdf_text, re.IGNORECASE)
        if match:
            invoice_data['gstin'] = match.group(1)
        
        # Extract line items (Amazon-specific patterns)
        line_items = self._extract_amazon_line_items(pdf_text)
        invoice_data['line_items'] = line_items
        
        return invoice_data
    
    def _extract_amazon_line_items(self, text: str) -> List[Dict]:
        """Extract line items from Amazon fee invoice."""
        
        line_items = []
        
        # Common Amazon fee patterns
        fee_patterns = {
            'Closing Fee': r'Closing\s+Fee\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)',
            'Shipping Fee': r'Shipping\s+Fee\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)',
            'Commission': r'Commission\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)',
            'Fulfillment Fee': r'Fulfillment\s+Fee\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)',
            'Storage Fee': r'Storage\s+Fee\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)',
            'Advertising Fee': r'Advertising\s+Fee\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)',
            'Refund Processing Fee': r'Refund\s+Processing\s+Fee\s*.*?(\d+\.?\d*)\s*.*?(\d+\.?\d*)'
        }
        
        for expense_type, pattern in fee_patterns.items():
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    taxable_value = float(match.group(1))
                    total_value = float(match.group(2))
                    
                    # Calculate GST (assuming 18% for most Amazon fees)
                    gst_rate = 0.18
                    tax_amount = total_value - taxable_value
                    
                    line_item = {
                        'expense_type': expense_type,
                        'taxable_value': taxable_value,
                        'gst_rate': gst_rate,
                        'tax_amount': tax_amount,
                        'total_value': total_value
                    }
                    
                    line_items.append(line_item)
                    
                except (ValueError, IndexError):
                    continue
        
        # If no specific patterns found, try generic table extraction
        if not line_items:
            line_items = self._extract_generic_line_items(text)
        
        return line_items
    
    def _extract_generic_line_items(self, text: str) -> List[Dict]:
        """Extract line items using generic patterns."""
        
        line_items = []
        
        # Look for tabular data patterns
        lines = text.split('\n')
        
        for line in lines:
            # Skip empty lines and headers
            if not line.strip() or any(header in line.lower() for header in ['description', 'amount', 'total', 'gst']):
                continue
            
            # Look for lines with amounts (pattern: text followed by numbers)
            amount_pattern = r'(.+?)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)?\s*$'
            match = re.match(amount_pattern, line.strip())
            
            if match:
                description = match.group(1).strip()
                try:
                    taxable = float(match.group(2))
                    total = float(match.group(3))
                    
                    # Determine expense type from description
                    expense_type = self._classify_expense_type(description)
                    
                    if expense_type:
                        line_item = {
                            'expense_type': expense_type,
                            'taxable_value': taxable,
                            'gst_rate': 0.18,  # Default to 18%
                            'tax_amount': total - taxable,
                            'total_value': total
                        }
                        line_items.append(line_item)
                        
                except ValueError:
                    continue
        
        return line_items
    
    def _classify_expense_type(self, description: str) -> Optional[str]:
        """Classify expense type from description."""
        
        description_lower = description.lower()
        
        expense_keywords = {
            'Closing Fee': ['closing', 'closure'],
            'Shipping Fee': ['shipping', 'delivery', 'freight'],
            'Commission': ['commission', 'referral'],
            'Fulfillment Fee': ['fulfillment', 'fba'],
            'Storage Fee': ['storage', 'warehouse'],
            'Advertising Fee': ['advertising', 'ads', 'promotion'],
            'Payment Gateway Fee': ['payment', 'gateway', 'processing']
        }
        
        for expense_type, keywords in expense_keywords.items():
            if any(keyword in description_lower for keyword in keywords):
                return expense_type
        
        return 'Other Fee'  # Default category


class ExcelInvoiceParser:
    """Parser for Excel-based seller invoices."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parse_excel_invoice(self, excel_path: str) -> Dict:
        """Parse seller invoice from Excel file."""
        
        try:
            # Try to read Excel file
            df = pd.read_excel(excel_path)
            
            invoice_data = {
                'invoice_no': None,
                'invoice_date': None,
                'gstin': None,
                'line_items': []
            }
            
            # Look for invoice metadata in first few rows
            for i in range(min(10, len(df))):
                row = df.iloc[i]
                
                # Check for invoice number
                for col in df.columns:
                    cell_value = str(row[col]).strip()
                    
                    if 'invoice' in cell_value.lower() and any(c.isalnum() for c in cell_value):
                        # Extract alphanumeric part
                        invoice_match = re.search(r'([A-Z0-9\-]+)', cell_value)
                        if invoice_match:
                            invoice_data['invoice_no'] = invoice_match.group(1)
                    
                    # Check for date
                    if pd.notna(row[col]) and isinstance(row[col], (pd.Timestamp, datetime)):
                        invoice_data['invoice_date'] = row[col].date()
                    
                    # Check for GSTIN
                    gstin_match = re.search(r'([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9][Z][0-9])', cell_value)
                    if gstin_match:
                        invoice_data['gstin'] = gstin_match.group(1)
            
            # Extract line items from tabular data
            line_items = self._extract_excel_line_items(df)
            invoice_data['line_items'] = line_items
            
            return invoice_data
            
        except Exception as e:
            self.logger.error(f"Error parsing Excel invoice: {e}")
            raise
    
    def _extract_excel_line_items(self, df: pd.DataFrame) -> List[Dict]:
        """Extract line items from Excel DataFrame."""
        
        line_items = []
        
        # Look for columns that might contain line item data
        amount_cols = []
        desc_cols = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['amount', 'value', 'total', 'price']):
                amount_cols.append(col)
            elif any(keyword in col_lower for keyword in ['description', 'item', 'fee', 'charge']):
                desc_cols.append(col)
        
        # Process rows that look like line items
        for i, row in df.iterrows():
            # Skip rows that are clearly headers or metadata
            if i < 5:  # Skip first few rows which might be headers
                continue
            
            description = None
            taxable_value = None
            total_value = None
            
            # Get description
            for col in desc_cols:
                if pd.notna(row[col]) and str(row[col]).strip():
                    description = str(row[col]).strip()
                    break
            
            # Get amounts
            amounts = []
            for col in amount_cols:
                if pd.notna(row[col]) and isinstance(row[col], (int, float)):
                    amounts.append(float(row[col]))
            
            # If we have description and amounts, create line item
            if description and len(amounts) >= 1:
                if len(amounts) >= 2:
                    taxable_value = amounts[0]
                    total_value = amounts[1]
                else:
                    total_value = amounts[0]
                    taxable_value = total_value / 1.18  # Assume 18% GST
                
                expense_type = self._classify_expense_type_excel(description)
                
                line_item = {
                    'expense_type': expense_type,
                    'taxable_value': taxable_value,
                    'gst_rate': 0.18,
                    'tax_amount': total_value - taxable_value,
                    'total_value': total_value
                }
                
                line_items.append(line_item)
        
        return line_items
    
    def _classify_expense_type_excel(self, description: str) -> str:
        """Classify expense type from Excel description."""
        
        description_lower = description.lower()
        
        if any(keyword in description_lower for keyword in ['closing', 'closure']):
            return 'Closing Fee'
        elif any(keyword in description_lower for keyword in ['shipping', 'delivery']):
            return 'Shipping Fee'
        elif any(keyword in description_lower for keyword in ['commission', 'referral']):
            return 'Commission'
        elif any(keyword in description_lower for keyword in ['fulfillment', 'fba']):
            return 'Fulfillment Fee'
        elif any(keyword in description_lower for keyword in ['storage', 'warehouse']):
            return 'Storage Fee'
        elif any(keyword in description_lower for keyword in ['advertising', 'ads']):
            return 'Advertising Fee'
        else:
            return 'Other Fee'


def parse_invoice_file(file_path: str) -> Dict:
    """Parse invoice file (PDF or Excel) and return structured data."""
    
    file_path_lower = file_path.lower()
    
    if file_path_lower.endswith('.pdf'):
        parser = PDFParser()
        text = parser.extract_text_from_pdf(file_path)
        return parser.parse_amazon_fee_invoice(text)
    
    elif file_path_lower.endswith(('.xlsx', '.xls')):
        parser = ExcelInvoiceParser()
        return parser.parse_excel_invoice(file_path)
    
    else:
        raise ValueError(f"Unsupported file format: {file_path}")
