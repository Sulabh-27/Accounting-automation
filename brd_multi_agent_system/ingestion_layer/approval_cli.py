#!/usr/bin/env python3
"""
Approval CLI for managing pending item and ledger mappings.
"""
import argparse
import sys
from dotenv import load_dotenv

from .libs.supabase_client import SupabaseClientWrapper
from .agents.approval_agent import ApprovalAgent


def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Approval CLI for Item & Ledger Mappings")
    parser.add_argument("--approver", default="manual", help="Name of the approver")
    parser.add_argument("--type", choices=["item", "ledger"], help="Filter by approval type")
    parser.add_argument("--list", action="store_true", help="List pending approvals and exit")
    parser.add_argument("--bulk-approve", action="store_true", help="Bulk approve all with suggested values")
    parser.add_argument("--interactive", action="store_true", help="Start interactive approval session")
    
    args = parser.parse_args()
    
    # Initialize components
    supabase = SupabaseClientWrapper()
    approval_agent = ApprovalAgent(supabase)
    
    if args.list:
        # Just list pending approvals
        approval_agent.display_pending_approvals(args.type)
        return 0
    
    elif args.bulk_approve:
        # Bulk approve all pending with suggested values
        print("🚀 Bulk approving all pending requests with suggested values...")
        
        approvals = approval_agent.get_pending_approvals(args.type)
        if not approvals:
            print("✅ No pending approvals found!")
            return 0
        
        item_count = approval_agent.bulk_approve_items(
            [a for a in approvals if a.get("type") == "item"], 
            args.approver
        )
        ledger_count = approval_agent.bulk_approve_ledgers(
            [a for a in approvals if a.get("type") == "ledger"], 
            args.approver
        )
        
        print(f"✅ Bulk approved {item_count} item mappings and {ledger_count} ledger mappings")
        return 0
    
    elif args.interactive:
        # Start interactive approval session
        approval_agent.interactive_approval_session(args.approver)
        return 0
    
    else:
        # Default: show pending and ask what to do
        approval_agent.display_pending_approvals(args.type)
        
        approvals = approval_agent.get_pending_approvals(args.type)
        if not approvals:
            return 0
        
        print("\nWhat would you like to do?")
        print("  1. Start interactive approval session")
        print("  2. Bulk approve all with suggested values")
        print("  3. Exit")
        
        try:
            choice = input("\nEnter your choice (1-3): ").strip()
            
            if choice == "1":
                approval_agent.interactive_approval_session(args.approver)
            elif choice == "2":
                item_count = approval_agent.bulk_approve_items(
                    [a for a in approvals if a.get("type") == "item"], 
                    args.approver
                )
                ledger_count = approval_agent.bulk_approve_ledgers(
                    [a for a in approvals if a.get("type") == "ledger"], 
                    args.approver
                )
                print(f"✅ Bulk approved {item_count} item mappings and {ledger_count} ledger mappings")
            elif choice == "3":
                print("👋 Goodbye!")
            else:
                print("❌ Invalid choice")
                return 1
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            return 0
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
