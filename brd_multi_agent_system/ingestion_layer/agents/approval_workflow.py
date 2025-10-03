"""
Approval Workflow Agent for Part 7: Exception Handling & Approval Workflows
Manages human-in-the-loop approval processes for ambiguous decisions
"""
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging

from ..libs.error_codes import get_error_definition, requires_approval
from ..libs.notification_utils import notify_approval_request, notify_approval_completion
from ..libs.supabase_client import SupabaseClientWrapper


@dataclass
class ApprovalRequest:
    """Structured approval request."""
    id: str
    run_id: str
    request_type: str
    payload: Dict[str, Any]
    context_data: Optional[Dict[str, Any]] = None
    priority: str = "medium"
    status: str = "pending"
    approver: Optional[str] = None
    approval_notes: Optional[str] = None
    created_at: Optional[datetime] = None
    decided_at: Optional[datetime] = None


@dataclass
class ApprovalResult:
    """Result of approval processing."""
    total_requests: int
    pending_requests: int
    approved_requests: int
    rejected_requests: int
    auto_approved_requests: int
    processing_successful: bool
    approval_summary: Dict[str, int]


class ApprovalWorkflowAgent:
    """Manages approval workflows for exception resolution."""
    
    def __init__(self, supabase_client: Optional[SupabaseClientWrapper] = None):
        self.supabase = supabase_client
        self.logger = logging.getLogger(__name__)
        self.approval_rules = self._load_approval_rules()
    
    def _load_approval_rules(self) -> Dict[str, Any]:
        """Load approval rules from database or configuration."""
        
        default_rules = {
            'item_mapping': {
                'auto_approve_similar': True,
                'similarity_threshold': 0.9,
                'max_auto_approve_value': 5000
            },
            'ledger_mapping': {
                'auto_approve_standard_channels': True,
                'standard_channels': ['amazon', 'flipkart', 'pepperfry'],
                'auto_approve_known_states': True
            },
            'gst_rate_override': {
                'auto_approve': False,
                'allowed_rates': [0.0, 0.05, 0.12, 0.18, 0.28]
            },
            'invoice_override': {
                'auto_approve_format_fix': True,
                'auto_approve_date_adjustment': False
            }
        }
        
        # TODO: Load from database if available
        if self.supabase:
            try:
                result = self.supabase.client.table('approval_rules').select('*').eq('is_active', True).execute()
                if result.data:
                    # Process database rules
                    pass
            except Exception as e:
                self.logger.warning(f"Could not load approval rules from database: {e}")
        
        return default_rules
    
    def create_approval_request(
        self,
        request_type: str,
        payload: Dict[str, Any],
        run_id: uuid.UUID,
        context_data: Optional[Dict[str, Any]] = None,
        priority: str = "medium"
    ) -> str:
        """Create a new approval request."""
        
        request_id = str(uuid.uuid4())
        
        approval_request = ApprovalRequest(
            id=request_id,
            run_id=str(run_id),
            request_type=request_type,
            payload=payload,
            context_data=context_data or {},
            priority=priority,
            created_at=datetime.now()
        )
        
        # Check if request can be auto-approved
        auto_approval_result = self._check_auto_approval(approval_request)
        
        if auto_approval_result['can_auto_approve']:
            # Auto-approve the request
            approval_request.status = 'approved'
            approval_request.approver = 'system_auto'
            approval_request.approval_notes = auto_approval_result['reason']
            approval_request.decided_at = datetime.now()
            
            self.logger.info(f"Auto-approved request {request_id}: {auto_approval_result['reason']}")
        else:
            # Queue for human approval
            notify_approval_request(
                request_type=request_type,
                payload=payload,
                priority=priority
            )
        
        # Save to database
        if self.supabase:
            self._save_approval_request(approval_request)
        
        return request_id
    
    def _check_auto_approval(self, request: ApprovalRequest) -> Dict[str, Any]:
        """Check if request can be auto-approved based on rules."""
        
        request_type = request.request_type
        payload = request.payload
        
        if request_type not in self.approval_rules:
            return {'can_auto_approve': False, 'reason': 'No rules defined for request type'}
        
        rules = self.approval_rules[request_type]
        
        if request_type == 'item_mapping':
            return self._check_item_mapping_auto_approval(payload, rules)
        elif request_type == 'ledger_mapping':
            return self._check_ledger_mapping_auto_approval(payload, rules)
        elif request_type == 'gst_rate_override':
            return self._check_gst_rate_auto_approval(payload, rules)
        elif request_type == 'invoice_override':
            return self._check_invoice_auto_approval(payload, rules)
        else:
            return {'can_auto_approve': False, 'reason': 'Unknown request type'}
    
    def _check_item_mapping_auto_approval(
        self,
        payload: Dict[str, Any],
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check auto-approval for item mapping requests."""
        
        # Check if similar mappings exist
        if rules.get('auto_approve_similar', False):
            sku = payload.get('sku', '')
            
            # Simple similarity check (can be enhanced with fuzzy matching)
            if len(sku) >= 3:
                sku_prefix = sku[:3]
                
                # TODO: Query database for similar SKUs
                # For now, simulate similarity check
                if sku_prefix in ['ABC', 'XYZ', 'DEF']:  # Example patterns
                    return {
                        'can_auto_approve': True,
                        'reason': f'Similar SKU pattern found: {sku_prefix}'
                    }
        
        # Check value threshold
        max_value = rules.get('max_auto_approve_value', 5000)
        item_value = payload.get('estimated_value', 0)
        
        if item_value > max_value:
            return {
                'can_auto_approve': False,
                'reason': f'Item value {item_value} exceeds threshold {max_value}'
            }
        
        return {'can_auto_approve': False, 'reason': 'No auto-approval criteria met'}
    
    def _check_ledger_mapping_auto_approval(
        self,
        payload: Dict[str, Any],
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check auto-approval for ledger mapping requests."""
        
        channel = payload.get('channel', '')
        state_code = payload.get('state_code', '')
        
        # Check if channel is in standard list
        if rules.get('auto_approve_standard_channels', False):
            standard_channels = rules.get('standard_channels', [])
            if channel in standard_channels:
                # Generate standard ledger name
                state_names = {
                    'HR': 'Haryana', 'DL': 'Delhi', 'UP': 'Uttar Pradesh',
                    'GJ': 'Gujarat', 'KA': 'Karnataka', 'MH': 'Maharashtra'
                    # Add more as needed
                }
                
                if state_code in state_names:
                    suggested_ledger = f"{channel.title()} {state_names[state_code]}"
                    return {
                        'can_auto_approve': True,
                        'reason': f'Standard channel and state: {suggested_ledger}',
                        'suggested_value': suggested_ledger
                    }
        
        return {'can_auto_approve': False, 'reason': 'Manual approval required for ledger mapping'}
    
    def _check_gst_rate_auto_approval(
        self,
        payload: Dict[str, Any],
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check auto-approval for GST rate override requests."""
        
        # GST rate overrides typically require manual approval
        if not rules.get('auto_approve', False):
            return {'can_auto_approve': False, 'reason': 'GST rate changes require manual approval'}
        
        proposed_rate = payload.get('proposed_gst_rate', 0)
        allowed_rates = rules.get('allowed_rates', [])
        
        if proposed_rate not in allowed_rates:
            return {
                'can_auto_approve': False,
                'reason': f'Proposed rate {proposed_rate} not in allowed rates {allowed_rates}'
            }
        
        return {'can_auto_approve': True, 'reason': 'Proposed rate is valid'}
    
    def _check_invoice_auto_approval(
        self,
        payload: Dict[str, Any],
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check auto-approval for invoice override requests."""
        
        override_type = payload.get('override_type', '')
        
        if override_type == 'format_fix' and rules.get('auto_approve_format_fix', False):
            return {
                'can_auto_approve': True,
                'reason': 'Auto-approved invoice format correction'
            }
        
        if override_type == 'date_adjustment' and rules.get('auto_approve_date_adjustment', False):
            return {
                'can_auto_approve': True,
                'reason': 'Auto-approved invoice date adjustment'
            }
        
        return {'can_auto_approve': False, 'reason': 'Manual approval required for invoice changes'}
    
    def process_approval_request(
        self,
        request_id: str,
        decision: str,
        approver: str,
        notes: Optional[str] = None
    ) -> bool:
        """Process an approval decision."""
        
        if decision not in ['approved', 'rejected']:
            raise ValueError("Decision must be 'approved' or 'rejected'")
        
        try:
            # Update request in database
            if self.supabase:
                update_data = {
                    'status': decision,
                    'approver': approver,
                    'approval_notes': notes,
                    'decided_at': datetime.now().isoformat()
                }
                
                result = self.supabase.client.table('approval_queue').update(update_data).eq('id', request_id).execute()
                
                if not result.data:
                    self.logger.error(f"Failed to update approval request {request_id}")
                    return False
                
                request_data = result.data[0]
                
                # Apply the approval decision
                if decision == 'approved':
                    self._apply_approval_decision(request_data)
                
                # Send notification
                notify_approval_completion(
                    request_type=request_data['request_type'],
                    decision=decision,
                    approver=approver,
                    notes=notes
                )
                
                self.logger.info(f"Processed approval request {request_id}: {decision} by {approver}")
                return True
            
        except Exception as e:
            self.logger.error(f"Error processing approval request {request_id}: {e}")
            return False
        
        return False
    
    def _apply_approval_decision(self, request_data: Dict[str, Any]) -> bool:
        """Apply the approved decision to the system."""
        
        request_type = request_data['request_type']
        payload = request_data['payload']
        
        try:
            if request_type == 'item_mapping':
                return self._apply_item_mapping_approval(payload)
            elif request_type == 'ledger_mapping':
                return self._apply_ledger_mapping_approval(payload)
            elif request_type == 'gst_rate_override':
                return self._apply_gst_rate_approval(payload)
            elif request_type == 'invoice_override':
                return self._apply_invoice_approval(payload)
            else:
                self.logger.error(f"Unknown request type for approval: {request_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error applying approval decision: {e}")
            return False
    
    def _apply_item_mapping_approval(self, payload: Dict[str, Any]) -> bool:
        """Apply approved item mapping to database."""
        
        if not self.supabase:
            return False
        
        try:
            mapping_data = {
                'sku': payload.get('sku'),
                'asin': payload.get('asin'),
                'final_goods_name': payload.get('final_goods_name'),
                'channel': payload.get('channel'),
                'approved_by': payload.get('approver', 'system')
            }
            
            result = self.supabase.client.table('item_master').insert(mapping_data).execute()
            return bool(result.data)
            
        except Exception as e:
            self.logger.error(f"Error applying item mapping approval: {e}")
            return False
    
    def _apply_ledger_mapping_approval(self, payload: Dict[str, Any]) -> bool:
        """Apply approved ledger mapping to database."""
        
        if not self.supabase:
            return False
        
        try:
            mapping_data = {
                'channel': payload.get('channel'),
                'state_code': payload.get('state_code'),
                'ledger_name': payload.get('ledger_name'),
                'approved_by': payload.get('approver', 'system')
            }
            
            result = self.supabase.client.table('ledger_master').insert(mapping_data).execute()
            return bool(result.data)
            
        except Exception as e:
            self.logger.error(f"Error applying ledger mapping approval: {e}")
            return False
    
    def _apply_gst_rate_approval(self, payload: Dict[str, Any]) -> bool:
        """Apply approved GST rate override."""
        
        # GST rate overrides might need special handling
        # For now, just log the approval
        self.logger.info(f"Applied GST rate override: {payload}")
        return True
    
    def _apply_invoice_approval(self, payload: Dict[str, Any]) -> bool:
        """Apply approved invoice override."""
        
        # Invoice overrides might need special handling
        # For now, just log the approval
        self.logger.info(f"Applied invoice override: {payload}")
        return True
    
    def _save_approval_request(self, request: ApprovalRequest) -> bool:
        """Save approval request to database."""
        
        if not self.supabase:
            return False
        
        try:
            request_data = {
                'id': request.id,
                'run_id': request.run_id,
                'request_type': request.request_type,
                'payload': request.payload,
                'context_data': request.context_data,
                'priority': request.priority,
                'status': request.status,
                'approver': request.approver,
                'approval_notes': request.approval_notes,
                'auto_approve_eligible': request.status == 'approved' and request.approver == 'system_auto'
            }
            
            if request.decided_at:
                request_data['decided_at'] = request.decided_at.isoformat()
            
            result = self.supabase.client.table('approval_queue').insert(request_data).execute()
            return bool(result.data)
            
        except Exception as e:
            self.logger.error(f"Error saving approval request: {e}")
            return False
    
    def get_pending_approvals(
        self,
        run_id: Optional[uuid.UUID] = None,
        request_type: Optional[str] = None
    ) -> List[ApprovalRequest]:
        """Get pending approval requests."""
        
        if not self.supabase:
            return []
        
        try:
            query = self.supabase.client.table('approval_queue').select('*').eq('status', 'pending')
            
            if run_id:
                query = query.eq('run_id', str(run_id))
            
            if request_type:
                query = query.eq('request_type', request_type)
            
            result = query.execute()
            
            approvals = []
            for data in result.data:
                approval = ApprovalRequest(
                    id=data['id'],
                    run_id=data['run_id'],
                    request_type=data['request_type'],
                    payload=data['payload'],
                    context_data=data.get('context_data'),
                    priority=data['priority'],
                    status=data['status'],
                    approver=data.get('approver'),
                    approval_notes=data.get('approval_notes'),
                    created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
                    decided_at=datetime.fromisoformat(data['decided_at']) if data.get('decided_at') else None
                )
                approvals.append(approval)
            
            return approvals
            
        except Exception as e:
            self.logger.error(f"Error getting pending approvals: {e}")
            return []
    
    def get_approval_summary(
        self,
        run_id: Optional[uuid.UUID] = None
    ) -> ApprovalResult:
        """Get summary of approval requests."""
        
        if not self.supabase:
            return ApprovalResult(
                total_requests=0,
                pending_requests=0,
                approved_requests=0,
                rejected_requests=0,
                auto_approved_requests=0,
                processing_successful=True,
                approval_summary={}
            )
        
        try:
            query = self.supabase.client.table('approval_queue').select('*')
            
            if run_id:
                query = query.eq('run_id', str(run_id))
            
            result = query.execute()
            
            total_requests = len(result.data)
            pending_requests = sum(1 for r in result.data if r['status'] == 'pending')
            approved_requests = sum(1 for r in result.data if r['status'] == 'approved')
            rejected_requests = sum(1 for r in result.data if r['status'] == 'rejected')
            auto_approved_requests = sum(1 for r in result.data if r.get('approver') == 'system_auto')
            
            # Create summary by request type
            approval_summary = {}
            for request in result.data:
                req_type = request['request_type']
                status = request['status']
                key = f"{req_type}_{status}"
                approval_summary[key] = approval_summary.get(key, 0) + 1
            
            return ApprovalResult(
                total_requests=total_requests,
                pending_requests=pending_requests,
                approved_requests=approved_requests,
                rejected_requests=rejected_requests,
                auto_approved_requests=auto_approved_requests,
                processing_successful=pending_requests == 0,
                approval_summary=approval_summary
            )
            
        except Exception as e:
            self.logger.error(f"Error getting approval summary: {e}")
            return ApprovalResult(
                total_requests=0,
                pending_requests=0,
                approved_requests=0,
                rejected_requests=0,
                auto_approved_requests=0,
                processing_successful=False,
                approval_summary={}
            )
    
    def create_item_mapping_request(
        self,
        sku: str,
        asin: Optional[str],
        channel: str,
        run_id: uuid.UUID,
        suggested_fg_name: Optional[str] = None,
        estimated_value: float = 0
    ) -> str:
        """Create item mapping approval request."""
        
        payload = {
            'sku': sku,
            'asin': asin,
            'channel': channel,
            'suggested_fg_name': suggested_fg_name,
            'estimated_value': estimated_value
        }
        
        context_data = {
            'source': 'exception_handler',
            'error_code': 'MAP-001' if sku else 'MAP-002'
        }
        
        return self.create_approval_request(
            request_type='item_mapping',
            payload=payload,
            run_id=run_id,
            context_data=context_data,
            priority='medium'
        )
    
    def create_ledger_mapping_request(
        self,
        channel: str,
        state_code: str,
        run_id: uuid.UUID,
        suggested_ledger_name: Optional[str] = None
    ) -> str:
        """Create ledger mapping approval request."""
        
        payload = {
            'channel': channel,
            'state_code': state_code,
            'suggested_ledger_name': suggested_ledger_name
        }
        
        context_data = {
            'source': 'exception_handler',
            'error_code': 'LED-001'
        }
        
        return self.create_approval_request(
            request_type='ledger_mapping',
            payload=payload,
            run_id=run_id,
            context_data=context_data,
            priority='medium'
        )
