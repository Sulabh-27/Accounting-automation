"""
Invoice Numbering Rules Engine
Handles invoice number generation for different e-commerce channels
"""
import re
from datetime import datetime
from typing import Dict, Optional, Set
from dataclasses import dataclass


@dataclass
class NumberingPattern:
    """Invoice numbering pattern configuration."""
    prefix: str
    state_code: bool  # Whether to include state code
    month_code: bool  # Whether to include month code
    separator: str
    example: str


class NumberingRulesEngine:
    """
    Centralized invoice numbering engine for different e-commerce channels.
    Generates unique invoice numbers based on channel-specific patterns.
    """
    
    # Channel-specific numbering patterns
    PATTERNS = {
        "amazon_mtr": NumberingPattern(
            prefix="AMZ",
            state_code=True,
            month_code=True,
            separator="-",
            example="AMZ-TN-04"
        ),
        "amazon_str": NumberingPattern(
            prefix="AMZST",
            state_code=True,
            month_code=True,
            separator="-",
            example="AMZST-06-04"
        ),
        "flipkart": NumberingPattern(
            prefix="FLIP",
            state_code=True,
            month_code=True,
            separator="-",
            example="FLIP-TN-04"
        ),
        "pepperfry": NumberingPattern(
            prefix="PEPP",
            state_code=True,
            month_code=True,
            separator="-",
            example="PEPP-TN-04"
        )
    }
    
    # State code to state name mapping
    STATE_MAPPINGS = {
        "ANDHRA PRADESH": "AP", "ARUNACHAL PRADESH": "AR", "ASSAM": "AS",
        "BIHAR": "BR", "CHHATTISGARH": "CG", "GOA": "GA", "GUJARAT": "GJ",
        "HARYANA": "HR", "HIMACHAL PRADESH": "HP", "JHARKHAND": "JH",
        "KARNATAKA": "KA", "KERALA": "KL", "MADHYA PRADESH": "MP",
        "MAHARASHTRA": "MH", "MANIPUR": "MN", "MEGHALAYA": "ML",
        "MIZORAM": "MZ", "NAGALAND": "NL", "DELHI": "DL", "ODISHA": "OR",
        "PUNJAB": "PB", "RAJASTHAN": "RJ", "SIKKIM": "SK", "TAMIL NADU": "TN",
        "TELANGANA": "TG", "TRIPURA": "TR", "UTTAR PRADESH": "UP",
        "UTTARAKHAND": "UK", "WEST BENGAL": "WB", "JAMMU & KASHMIR": "JK",
        "LADAKH": "LA", "CHANDIGARH": "CH", "DADRA & NAGAR HAVELI": "DN",
        "DAMAN & DIU": "DD", "LAKSHADWEEP": "LD", "PUDUCHERRY": "PY"
    }
    
    # GSTIN state code to state abbreviation mapping
    GSTIN_STATE_CODES = {
        "01": "JK", "02": "HP", "03": "PB", "04": "CH", "05": "UK",
        "06": "HR", "07": "DL", "08": "RJ", "09": "UP", "10": "BR",
        "11": "SK", "12": "AR", "13": "NL", "14": "MN", "15": "MZ",
        "16": "TR", "17": "ML", "18": "AS", "19": "WB", "20": "JH",
        "21": "OR", "22": "CG", "23": "MP", "24": "GJ", "25": "DD",
        "26": "DN", "27": "MH", "28": "AP", "29": "KA", "30": "GA",
        "31": "LD", "32": "KL", "33": "TN", "34": "PY", "35": "AN",
        "36": "TG", "37": "LA"
    }
    
    def __init__(self, company_gstin: str):
        """
        Initialize numbering rules engine.
        
        Args:
            company_gstin: Company's GSTIN for state identification
        """
        self.company_gstin = company_gstin
        self.company_state_code = self._extract_state_from_gstin(company_gstin)
        self.generated_numbers: Set[str] = set()
    
    def _extract_state_from_gstin(self, gstin: str) -> str:
        """Extract state code from GSTIN."""
        if not gstin or len(gstin) < 2:
            return "99"
        gstin_state = gstin[:2]
        return self.GSTIN_STATE_CODES.get(gstin_state, "UN")
    
    def _get_state_code(self, state_name: str) -> str:
        """Convert state name to state code."""
        return self.STATE_MAPPINGS.get(state_name.upper(), "UN")
    
    def _get_month_code(self, month_str: str) -> str:
        """
        Extract month code from month string.
        
        Args:
            month_str: Month in format "YYYY-MM" or datetime
            
        Returns:
            Two-digit month code
        """
        try:
            if isinstance(month_str, str):
                if "-" in month_str:
                    return month_str.split("-")[1]
                elif len(month_str) >= 2:
                    return month_str[-2:]
            elif isinstance(month_str, datetime):
                return f"{month_str.month:02d}"
            
            # Default to current month
            return f"{datetime.now().month:02d}"
        except:
            return "01"
    
    def generate_invoice_number(self, 
                               channel: str, 
                               state_name: str, 
                               month: str,
                               sequence_number: Optional[int] = None) -> str:
        """
        Generate invoice number based on channel and parameters.
        
        Args:
            channel: Channel name (amazon_mtr, flipkart, etc.)
            state_name: Customer/delivery state name
            month: Month string (YYYY-MM format)
            sequence_number: Optional sequence number for uniqueness
            
        Returns:
            Generated invoice number
        """
        if channel not in self.PATTERNS:
            raise ValueError(f"Unknown channel: {channel}")
        
        pattern = self.PATTERNS[channel]
        parts = [pattern.prefix]
        
        if pattern.state_code:
            state_code = self._get_state_code(state_name)
            parts.append(state_code)
        
        if pattern.month_code:
            month_code = self._get_month_code(month)
            parts.append(month_code)
        
        # Add sequence number if provided
        if sequence_number is not None:
            parts.append(f"{sequence_number:04d}")
        
        invoice_number = pattern.separator.join(parts)
        
        # Ensure uniqueness
        original_number = invoice_number
        counter = 1
        while invoice_number in self.generated_numbers:
            if sequence_number is not None:
                # Increment sequence number
                parts[-1] = f"{sequence_number + counter:04d}"
            else:
                # Add counter
                parts.append(f"{counter:04d}")
            invoice_number = pattern.separator.join(parts)
            counter += 1
        
        self.generated_numbers.add(invoice_number)
        return invoice_number
    
    def generate_batch_invoice_numbers(self, 
                                     records: list, 
                                     channel: str,
                                     month: str) -> Dict[str, str]:
        """
        Generate invoice numbers for a batch of records.
        
        Args:
            records: List of records with state information
            channel: Channel name
            month: Month string
            
        Returns:
            Dict mapping record index to invoice number
        """
        invoice_numbers = {}
        
        # Group by state for sequential numbering
        state_groups = {}
        for i, record in enumerate(records):
            state = record.get('state_code', 'UNKNOWN')
            if state not in state_groups:
                state_groups[state] = []
            state_groups[state].append((i, record))
        
        # Generate numbers for each state group
        for state, state_records in state_groups.items():
            for seq, (index, record) in enumerate(state_records, 1):
                invoice_number = self.generate_invoice_number(
                    channel=channel,
                    state_name=state,
                    month=month,
                    sequence_number=seq
                )
                invoice_numbers[index] = invoice_number
        
        return invoice_numbers
    
    def validate_invoice_number(self, invoice_number: str, channel: str) -> bool:
        """
        Validate invoice number format for a channel.
        
        Args:
            invoice_number: Invoice number to validate
            channel: Channel name
            
        Returns:
            True if valid, False otherwise
        """
        if channel not in self.PATTERNS:
            return False
        
        pattern = self.PATTERNS[channel]
        
        # Build regex pattern
        regex_parts = [re.escape(pattern.prefix)]
        
        if pattern.state_code:
            regex_parts.append(r"[A-Z]{2}")
        
        if pattern.month_code:
            regex_parts.append(r"\d{2}")
        
        # Optional sequence number
        regex_parts.append(r"(?:\d{4})?")
        
        regex_pattern = f"^{re.escape(pattern.separator).join(regex_parts)}$"
        
        return bool(re.match(regex_pattern, invoice_number))
    
    def parse_invoice_number(self, invoice_number: str, channel: str) -> Dict[str, str]:
        """
        Parse invoice number into components.
        
        Args:
            invoice_number: Invoice number to parse
            channel: Channel name
            
        Returns:
            Dict with parsed components
        """
        if not self.validate_invoice_number(invoice_number, channel):
            raise ValueError(f"Invalid invoice number format: {invoice_number}")
        
        pattern = self.PATTERNS[channel]
        parts = invoice_number.split(pattern.separator)
        
        result = {"prefix": parts[0]}
        part_index = 1
        
        if pattern.state_code and part_index < len(parts):
            result["state_code"] = parts[part_index]
            part_index += 1
        
        if pattern.month_code and part_index < len(parts):
            result["month_code"] = parts[part_index]
            part_index += 1
        
        if part_index < len(parts):
            result["sequence"] = parts[part_index]
        
        return result
    
    def get_next_sequence_number(self, channel: str, state_name: str, month: str) -> int:
        """
        Get the next sequence number for a channel/state/month combination.
        
        Args:
            channel: Channel name
            state_name: State name
            month: Month string
            
        Returns:
            Next sequence number
        """
        state_code = self._get_state_code(state_name)
        month_code = self._get_month_code(month)
        
        # Find existing numbers with same prefix
        pattern = self.PATTERNS[channel]
        prefix = f"{pattern.prefix}{pattern.separator}{state_code}{pattern.separator}{month_code}"
        
        max_seq = 0
        for number in self.generated_numbers:
            if number.startswith(prefix):
                try:
                    # Extract sequence number
                    seq_part = number.split(pattern.separator)[-1]
                    seq_num = int(seq_part)
                    max_seq = max(max_seq, seq_num)
                except (ValueError, IndexError):
                    continue
        
        return max_seq + 1
    
    def register_existing_numbers(self, existing_numbers: list):
        """
        Register existing invoice numbers to avoid duplicates.
        
        Args:
            existing_numbers: List of existing invoice numbers
        """
        self.generated_numbers.update(existing_numbers)
    
    def get_pattern_example(self, channel: str) -> str:
        """Get example invoice number pattern for a channel."""
        if channel in self.PATTERNS:
            return self.PATTERNS[channel].example
        return "Unknown pattern"
    
    def get_supported_channels(self) -> list:
        """Get list of supported channels."""
        return list(self.PATTERNS.keys())
