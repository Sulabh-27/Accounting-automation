#!/usr/bin/env python3
"""
Debug Part-5 template validation
"""
import sys
import os
sys.path.append('.')

from ingestion_layer.libs.x2beta_writer import X2BetaWriter


def debug_template_validation():
    """Debug template validation issues."""
    
    print("🔍 DEBUGGING PART-5 TEMPLATE VALIDATION")
    print("=" * 50)
    
    writer = X2BetaWriter()
    gstin = "06ABGCS4796R1ZA"
    template_path = f"ingestion_layer/templates/X2Beta Sales Template - {gstin}.xlsx"
    
    print(f"Template Path: {template_path}")
    print(f"File exists: {os.path.exists(template_path)}")
    
    if os.path.exists(template_path):
        print(f"File size: {os.path.getsize(template_path):,} bytes")
        
        # Test template info
        try:
            template_info = writer.get_template_info(template_path)
            print(f"Template info: {template_info}")
        except Exception as e:
            print(f"Error getting template info: {e}")
        
        # Test template validation
        try:
            validation = writer.validate_template(template_path)
            print(f"Template validation: {validation}")
        except Exception as e:
            print(f"Error validating template: {e}")
    
    # Test the TallyExporter validation
    print(f"\n🏭 Testing TallyExporter validation:")
    
    try:
        from ingestion_layer.agents.tally_exporter import TallyExporterAgent
        
        class MockSupabase:
            @property
            def client(self):
                return None
        
        exporter = TallyExporterAgent(MockSupabase())
        validation_result = exporter.validate_template_availability(gstin)
        print(f"TallyExporter validation: {validation_result}")
        
    except Exception as e:
        print(f"Error in TallyExporter validation: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    debug_template_validation()
