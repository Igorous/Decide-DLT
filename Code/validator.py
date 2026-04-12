
import json
import base64
from datetime import datetime
from typing import List, Dict, Any, Optional
from crypto_utils import generate_key_pair, sign_data, verify_signature, get_public_key_hash, sha256_hash

class Validator:
    def __init__(self, name: str, organization: str):
        self.name = name
        self.organization = organization
        self.private_key, self.public_key = generate_key_pair()
        self.validator_id = get_public_key_hash(self.public_key)
        self.status = "active"
        self.total_confirmations = 0
        self.uptime_percentage = 100.0
        self.rating = 0.5 # Initial rating
        self.registered_at = datetime.utcnow().isoformat() + "Z"
        self.last_seen = self.registered_at

    def validate_transaction(self, tx: Dict[str, Any], meetings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Basic validation logic (simplified)
        # 1. Check signature
        voter_pk_b64 = tx.get("payload", {}).get("voter_public_key", "")
        if not voter_pk_b64:
            return None
        
        voter_pk_bytes = base64.b64decode(voter_pk_b64)
        tx_data = json.dumps({"id": tx["transaction_id"], "payload": tx["payload"], "time": tx["timestamp"]}, sort_keys=True)
        if not verify_signature(voter_pk_bytes, tx_data, tx["signature"]):
            return None
            
        # 2. Check meeting
        meeting_id = tx["payload"].get("meeting_id")
        if meeting_id not in meetings:
            return None
        meeting = meetings[meeting_id]
        if meeting.status not in ["active", "pending"]:
            return None
            
        # 3. Create confirmation
        confirmation = {
            "transaction_id": tx["transaction_id"],
            "validator_id": self.validator_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "signature": ""
        }
        conf_data = json.dumps({"tx_id": confirmation["transaction_id"], "time": confirmation["timestamp"]}, sort_keys=True)
        confirmation["signature"] = sign_data(self.private_key, conf_data)
        
        self.total_confirmations += 1
        return confirmation

    def to_dict(self):
        return {
            "validator_id": self.validator_id,
            "public_key": base64.b64encode(self.public_key).decode('utf-8'),
            "name": self.name,
            "organization": self.organization,
            "status": self.status,
            "registered_at": self.registered_at,
            "last_seen": self.last_seen,
            "total_confirmations": self.total_confirmations,
            "uptime_percentage": self.uptime_percentage,
            "rating": self.rating
        }
