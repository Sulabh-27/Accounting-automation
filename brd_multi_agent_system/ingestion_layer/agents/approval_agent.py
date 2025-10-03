from __future__ import annotations
import json
from typing import List, Dict, Optional
from datetime import datetime

from ..libs.contracts import ApprovalRequest
from ..libs.supabase_client import SupabaseClientWrapper


class ApprovalAgent:
    """
    Manages human approval workflow for missing item and ledger mappings.
    Provides CLI interface for approvers to review and approve/reject requests.
    """

    def __init__(self, supabase: SupabaseClientWrapper):
        self.supabase = supabase

    def get_pending_approvals(self, approval_type: str = None) -> List[Dict]:
        """Get all pending approval requests."""
        return self.supabase.get_pending_approvals(approval_type)

    def display_pending_approvals(self, approval_type: str = None) -> None:
        """Display pending approvals in a user-friendly format."""
        approvals = self.get_pending_approvals(approval_type)
        
        if not approvals:
            print("✅ No pending approvals found!")
            return

        print(f"\n📋 Pending Approvals ({len(approvals)} items)")
        print("=" * 60)

        for i, approval in enumerate(approvals, 1):
            approval_id = approval.get("id", "")
            approval_type = approval.get("type", "")
            payload = approval.get("payload", {})
            created_at = approval.get("created_at", "")

            print(f"\n{i}. {approval_type.upper()} MAPPING REQUEST")
            print(f"   ID: {approval_id}")
            print(f"   Created: {created_at}")
            
            if approval_type == "item":
                sku = payload.get("sku", "")
                asin = payload.get("asin", "")
                suggested_fg = payload.get("suggested_fg", "")
                gst_rate = payload.get("gst_rate", 0.18)
                
                print(f"   SKU: {sku}")
                print(f"   ASIN: {asin}")
                print(f"   Suggested FG: {suggested_fg}")
                print(f"   GST Rate: {gst_rate * 100}%")
                
            elif approval_type == "ledger":
                channel = payload.get("channel", "")
                state_code = payload.get("state_code", "")
                suggested_ledger = payload.get("suggested_ledger_name", "")
                
                print(f"   Channel: {channel}")
                print(f"   State: {state_code}")
                print(f"   Suggested Ledger: {suggested_ledger}")

        print("\n" + "=" * 60)

    def approve_item_mapping(self, approval_id: str, fg_name: str, gst_rate: float = 0.18, 
                           item_code: str = None, approver: str = "manual") -> bool:
        """
        Approve an item mapping request and create the item master record.
        
        Args:
            approval_id: ID of the approval request
            fg_name: Final Goods name to use
            gst_rate: GST rate (default 18%)
            item_code: Item code (optional)
            approver: Who approved this
            
        Returns:
            bool: Success status
        """
        try:
            # Get the approval request
            approvals = self.supabase.get_pending_approvals("item")
            approval = next((a for a in approvals if a["id"] == approval_id), None)
            
            if not approval:
                print(f"❌ Approval request {approval_id} not found")
                return False

            payload = approval["payload"]
            sku = payload.get("sku", "")
            asin = payload.get("asin", "")
            
            # Create item master record
            self.supabase.insert_item_master(
                sku=sku,
                asin=asin or "",
                item_code=item_code or sku,
                fg=fg_name,
                gst_rate=gst_rate,
                approved_by=approver
            )
            
            # Mark approval as approved
            self.supabase.approve_request(approval_id, approver)
            
            print(f"✅ Item mapping approved: {sku} → {fg_name}")
            return True
            
        except Exception as e:
            print(f"❌ Error approving item mapping: {e}")
            return False

    def approve_ledger_mapping(self, approval_id: str, ledger_name: str, 
                             approver: str = "manual") -> bool:
        """
        Approve a ledger mapping request and create the ledger master record.
        
        Args:
            approval_id: ID of the approval request
            ledger_name: Ledger name to use
            approver: Who approved this
            
        Returns:
            bool: Success status
        """
        try:
            # Get the approval request
            approvals = self.supabase.get_pending_approvals("ledger")
            approval = next((a for a in approvals if a["id"] == approval_id), None)
            
            if not approval:
                print(f"❌ Approval request {approval_id} not found")
                return False

            payload = approval["payload"]
            channel = payload.get("channel", "")
            state_code = payload.get("state_code", "")
            
            # Create ledger master record
            self.supabase.insert_ledger_master(
                channel=channel,
                state_code=state_code,
                ledger_name=ledger_name,
                approved_by=approver
            )
            
            # Mark approval as approved
            self.supabase.approve_request(approval_id, approver)
            
            print(f"✅ Ledger mapping approved: {channel} + {state_code} → {ledger_name}")
            return True
            
        except Exception as e:
            print(f"❌ Error approving ledger mapping: {e}")
            return False

    def reject_request(self, approval_id: str, approver: str = "manual") -> bool:
        """
        Reject an approval request.
        
        Args:
            approval_id: ID of the approval request
            approver: Who rejected this
            
        Returns:
            bool: Success status
        """
        try:
            self.supabase.reject_request(approval_id, approver)
            print(f"❌ Request {approval_id} rejected")
            return True
        except Exception as e:
            print(f"❌ Error rejecting request: {e}")
            return False

    def bulk_approve_items(self, approvals: List[Dict], approver: str = "bulk") -> int:
        """
        Bulk approve item mappings using suggested values.
        
        Args:
            approvals: List of approval records
            approver: Who approved these
            
        Returns:
            int: Number of approvals processed
        """
        approved_count = 0
        
        for approval in approvals:
            if approval.get("type") != "item":
                continue
                
            try:
                approval_id = approval["id"]
                payload = approval["payload"]
                suggested_fg = payload.get("suggested_fg", "")
                gst_rate = payload.get("gst_rate", 0.18)
                
                if suggested_fg:
                    success = self.approve_item_mapping(
                        approval_id=approval_id,
                        fg_name=suggested_fg,
                        gst_rate=gst_rate,
                        approver=approver
                    )
                    if success:
                        approved_count += 1
                        
            except Exception as e:
                print(f"⚠️  Error bulk approving {approval.get('id', '')}: {e}")
                continue
        
        return approved_count

    def bulk_approve_ledgers(self, approvals: List[Dict], approver: str = "bulk") -> int:
        """
        Bulk approve ledger mappings using suggested values.
        
        Args:
            approvals: List of approval records
            approver: Who approved these
            
        Returns:
            int: Number of approvals processed
        """
        approved_count = 0
        
        for approval in approvals:
            if approval.get("type") != "ledger":
                continue
                
            try:
                approval_id = approval["id"]
                payload = approval["payload"]
                suggested_ledger = payload.get("suggested_ledger_name", "")
                
                if suggested_ledger:
                    success = self.approve_ledger_mapping(
                        approval_id=approval_id,
                        ledger_name=suggested_ledger,
                        approver=approver
                    )
                    if success:
                        approved_count += 1
                        
            except Exception as e:
                print(f"⚠️  Error bulk approving {approval.get('id', '')}: {e}")
                continue
        
        return approved_count

    def interactive_approval_session(self, approver: str = "interactive") -> None:
        """
        Run an interactive approval session in the CLI.
        """
        print("\n🔍 INTERACTIVE APPROVAL SESSION")
        print("=" * 50)
        
        while True:
            # Show pending approvals
            self.display_pending_approvals()
            
            approvals = self.get_pending_approvals()
            if not approvals:
                break
            
            print("\nOptions:")
            print("  1. Approve specific item")
            print("  2. Approve specific ledger")
            print("  3. Bulk approve all items (with suggestions)")
            print("  4. Bulk approve all ledgers (with suggestions)")
            print("  5. Reject specific request")
            print("  6. Refresh list")
            print("  0. Exit")
            
            try:
                choice = input("\nEnter your choice (0-6): ").strip()
                
                if choice == "0":
                    break
                elif choice == "1":
                    self._handle_item_approval(approvals, approver)
                elif choice == "2":
                    self._handle_ledger_approval(approvals, approver)
                elif choice == "3":
                    item_approvals = [a for a in approvals if a.get("type") == "item"]
                    count = self.bulk_approve_items(item_approvals, approver)
                    print(f"✅ Bulk approved {count} item mappings")
                elif choice == "4":
                    ledger_approvals = [a for a in approvals if a.get("type") == "ledger"]
                    count = self.bulk_approve_ledgers(ledger_approvals, approver)
                    print(f"✅ Bulk approved {count} ledger mappings")
                elif choice == "5":
                    self._handle_rejection(approvals, approver)
                elif choice == "6":
                    continue
                else:
                    print("❌ Invalid choice")
                    
            except KeyboardInterrupt:
                print("\n👋 Approval session ended")
                break
            except Exception as e:
                print(f"❌ Error: {e}")

    def _handle_item_approval(self, approvals: List[Dict], approver: str) -> None:
        """Handle interactive item approval."""
        item_approvals = [a for a in approvals if a.get("type") == "item"]
        if not item_approvals:
            print("No item approvals pending")
            return
        
        try:
            idx = int(input(f"Enter item number (1-{len(item_approvals)}): ")) - 1
            if 0 <= idx < len(item_approvals):
                approval = item_approvals[idx]
                payload = approval["payload"]
                suggested_fg = payload.get("suggested_fg", "")
                
                fg_name = input(f"Enter FG name (default: {suggested_fg}): ").strip()
                if not fg_name:
                    fg_name = suggested_fg
                
                gst_rate_input = input("Enter GST rate % (default: 18): ").strip()
                gst_rate = float(gst_rate_input) / 100 if gst_rate_input else 0.18
                
                self.approve_item_mapping(approval["id"], fg_name, gst_rate, approver=approver)
            else:
                print("❌ Invalid item number")
        except (ValueError, IndexError):
            print("❌ Invalid input")

    def _handle_ledger_approval(self, approvals: List[Dict], approver: str) -> None:
        """Handle interactive ledger approval."""
        ledger_approvals = [a for a in approvals if a.get("type") == "ledger"]
        if not ledger_approvals:
            print("No ledger approvals pending")
            return
        
        try:
            idx = int(input(f"Enter ledger number (1-{len(ledger_approvals)}): ")) - 1
            if 0 <= idx < len(ledger_approvals):
                approval = ledger_approvals[idx]
                payload = approval["payload"]
                suggested_ledger = payload.get("suggested_ledger_name", "")
                
                ledger_name = input(f"Enter ledger name (default: {suggested_ledger}): ").strip()
                if not ledger_name:
                    ledger_name = suggested_ledger
                
                self.approve_ledger_mapping(approval["id"], ledger_name, approver=approver)
            else:
                print("❌ Invalid ledger number")
        except (ValueError, IndexError):
            print("❌ Invalid input")

    def _handle_rejection(self, approvals: List[Dict], approver: str) -> None:
        """Handle interactive rejection."""
        try:
            idx = int(input(f"Enter request number to reject (1-{len(approvals)}): ")) - 1
            if 0 <= idx < len(approvals):
                approval = approvals[idx]
                self.reject_request(approval["id"], approver=approver)
            else:
                print("❌ Invalid request number")
        except (ValueError, IndexError):
            print("❌ Invalid input")
