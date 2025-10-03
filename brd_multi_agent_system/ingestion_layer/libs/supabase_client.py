from __future__ import annotations
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv

try:
    # supabase-py v2
    from supabase import create_client, Client
except Exception:  # pragma: no cover - tests will use Fake client
    create_client = None  # type: ignore
    Client = Any  # type: ignore

from .utils import file_sha256

class SupabaseClientWrapper:
    """Thin wrapper around supabase-py for storage and table operations.

    For tests, you can provide `client=None` and override methods or subclass.
    """

    def __init__(self, url: Optional[str] = None, key: Optional[str] = None, bucket: Optional[str] = None, development_mode: bool = True):
        load_dotenv()
        self.url = url or os.getenv("SUPABASE_URL")
        self.key = key or os.getenv("SUPABASE_KEY")
        self.bucket = bucket or os.getenv("SUPABASE_BUCKET", "raw-reports")
        self.client: Optional[Client] = None
        
        # Force development mode for local file processing
        if development_mode:
            print("🔧 Running in development mode - using local files only")
            self.client = None
        elif self.url and self.key and create_client:
            self.client = create_client(self.url, self.key)

    # ------------------- Storage -------------------
    def upload_file(self, local_path: str, dest_path: Optional[str] = None) -> str:
        """Upload a local file to Supabase storage bucket and return the storage path."""
        storage_path = dest_path or f"{uuid.uuid4()}/{os.path.basename(local_path)}"
        if self.client is None:
            # No-op in case of missing client; return local path for development
            # Fix any path issues by normalizing the path
            normalized_path = os.path.normpath(local_path).replace('\\', '/')
            # Fix the "daata" typo if it exists
            if 'daata' in normalized_path:
                normalized_path = normalized_path.replace('daata', 'data')
                print(f"⚠️  Fixed path typo: daata → data")
            # Also fix it in the display message
            display_path = normalized_path
            if 'daata' in local_path:
                display_path = local_path.replace('daata', 'data').replace('\\', '/')
                print(f"⚠️  Fixed display path typo: daata → data")
            print(f"⚠️  Supabase client not available - using local path: {display_path}")
            return normalized_path  # Return corrected local path
        
        # Upload to Supabase storage
        try:
            with open(local_path, "rb") as f:
                self.client.storage.from_(self.bucket).upload(storage_path, f)
            print(f"📤 Successfully uploaded to Supabase storage: {self.bucket}/{storage_path}")
            return f"{self.bucket}/{storage_path}"
        except Exception as e:
            print(f"⚠️  Supabase upload failed, using local path: {e}")
            return local_path

    def download_file(self, storage_path: str, local_path: str) -> str:
        """Download a file from Supabase storage to local path."""
        if self.client is None:
            print(f"⚠️  Supabase client not available - cannot download: {storage_path}")
            return storage_path  # Return original path if no client
        
        try:
            # Remove bucket prefix if present
            if storage_path.startswith(f"{self.bucket}/"):
                storage_path = storage_path[len(f"{self.bucket}/"):]
            
            # Download file
            response = self.client.storage.from_(self.bucket).download(storage_path)
            
            # Ensure local directory exists
            local_dir = os.path.dirname(local_path)
            if local_dir:  # Only create directory if dirname is not empty
                os.makedirs(local_dir, exist_ok=True)
            
            # Write to local file
            with open(local_path, "wb") as f:
                f.write(response)
            
            print(f"📥 Successfully downloaded from Supabase storage: {storage_path} -> {local_path}")
            return local_path
        except Exception as e:
            print(f"⚠️  Supabase download failed: {e}")
            return storage_path  # Return original path if download fails

    # ------------------- Tables -------------------
    def insert_report_metadata(self, run_id: uuid.UUID, report_type: str, file_path: str) -> Dict[str, Any]:
        h = file_sha256(file_path)
        row = {
            "id": str(uuid.uuid4()),
            "run_id": str(run_id),
            "report_type": report_type,
            "file_path": file_path,
            "hash": h,
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            self.client.table("reports").insert(row).execute()
        return row

    def list_reports(self, run_id: uuid.UUID) -> list[dict]:
        if self.client is None:
            return []
        res = self.client.table("reports").select("*").eq("run_id", str(run_id)).execute()
        return getattr(res, "data", [])

    def insert_run_start(self, run_id: uuid.UUID, channel: str, gstin: str, month: str) -> dict:
        row = {
            "run_id": str(run_id),
            "channel": channel,
            "gstin": gstin,
            "month": month,
            "status": "running",
            "started_at": datetime.utcnow().isoformat(timespec="seconds"),
            "finished_at": None,
        }
        if self.client is not None:
            self.client.table("runs").insert(row).execute()
        return row

    def update_run_finish(self, run_id: uuid.UUID, status: str = "success") -> dict:
        patch = {
            "status": status,
            "finished_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            self.client.table("runs").update(patch).eq("run_id", str(run_id)).execute()
        return patch

    # ------------------- Part-2: Item & Ledger Master Operations -------------------
    
    def get_item_master(self, sku: str = None, asin: str = None) -> Optional[dict]:
        """Get item master record by SKU or ASIN."""
        if self.client is None:
            return None
        
        query = self.client.table("item_master").select("*")
        if sku:
            query = query.eq("sku", sku)
        elif asin:
            query = query.eq("asin", asin)
        else:
            return None
            
        result = query.execute()
        data = getattr(result, "data", [])
        return data[0] if data else None
    
    def get_ledger_master(self, channel: str, state_code: str) -> Optional[dict]:
        """Get ledger master record by channel and state."""
        if self.client is None:
            return None
            
        result = self.client.table("ledger_master").select("*").eq("channel", channel).eq("state_code", state_code).execute()
        data = getattr(result, "data", [])
        return data[0] if data else None
    
    def insert_item_master(self, sku: str, asin: str, item_code: str, fg: str, gst_rate: float, approved_by: str = "system") -> dict:
        """Insert new item master record."""
        row = {
            "sku": sku,
            "asin": asin,
            "item_code": item_code,
            "fg": fg,
            "gst_rate": gst_rate,
            "approved_by": approved_by,
            "approved_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            result = self.client.table("item_master").insert(row).execute()
            data = getattr(result, "data", [])
            return data[0] if data else row
        return row
    
    def insert_ledger_master(self, channel: str, state_code: str, ledger_name: str, approved_by: str = "system") -> dict:
        """Insert new ledger master record."""
        row = {
            "channel": channel,
            "state_code": state_code,
            "ledger_name": ledger_name,
            "approved_by": approved_by,
            "approved_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            result = self.client.table("ledger_master").insert(row).execute()
            data = getattr(result, "data", [])
            return data[0] if data else row
        return row
    
    def insert_approval_request(self, approval_type: str, payload: dict) -> dict:
        """Insert approval request for missing mapping."""
        row = {
            "type": approval_type,
            "payload": payload,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            result = self.client.table("approvals").insert(row).execute()
            data = getattr(result, "data", [])
            return data[0] if data else row
        return row
    
    def get_pending_approvals(self, approval_type: str = None) -> list[dict]:
        """Get pending approval requests."""
        if self.client is None:
            return []
            
        query = self.client.table("approvals").select("*").eq("status", "pending")
        if approval_type:
            query = query.eq("type", approval_type)
            
        result = query.execute()
        return getattr(result, "data", [])
    
    def approve_request(self, approval_id: str, approver: str) -> dict:
        """Approve a pending request."""
        patch = {
            "status": "approved",
            "approver": approver,
            "decided_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            result = self.client.table("approvals").update(patch).eq("id", approval_id).execute()
            data = getattr(result, "data", [])
            return data[0] if data else patch
        return patch
    
    def reject_request(self, approval_id: str, approver: str) -> dict:
        """Reject a pending request."""
        patch = {
            "status": "rejected",
            "approver": approver,
            "decided_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        if self.client is not None:
            result = self.client.table("approvals").update(patch).eq("id", approval_id).execute()
            data = getattr(result, "data", [])
            return data[0] if data else patch
        return patch
