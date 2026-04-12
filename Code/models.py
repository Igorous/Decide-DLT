
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

class Transaction:
    def __init__(self, tx_type: str, payload: Dict[str, Any]):
        self.type = tx_type
        self.payload = payload
        self.transaction_id = "" # To be filled after hashing payload
        self.timestamp = datetime.utcnow().isoformat() + "Z"
        self.signature = ""

    def to_dict(self):
        return {
            "type": self.type,
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
            "signature": self.signature
        }

    @classmethod
    def from_dict(cls, data):
        tx = cls(data["type"], data["payload"])
        tx.transaction_id = data["transaction_id"]
        tx.timestamp = data["timestamp"]
        tx.signature = data["signature"]
        return tx

    def sign(self, private_bytes: bytes):
        from crypto_utils import sign_data, sha256_hash
        # Hash payload to get ID
        payload_str = json.dumps(self.payload, sort_keys=True)
        self.transaction_id = sha256_hash(payload_str + self.timestamp)
        # Sign the transaction
        tx_data = json.dumps({"id": self.transaction_id, "payload": self.payload, "time": self.timestamp}, sort_keys=True)
        self.signature = sign_data(private_bytes, tx_data)

class Block:
    def __init__(self, index: int, prev_hash: str, transactions: List[Dict[str, Any]]):
        self.index = index
        self.prev_hash = prev_hash
        self.timestamp = datetime.utcnow().isoformat() + "Z"
        self.transactions = transactions
        self.merkle_root = self.calculate_merkle_root()
        self.proposer = ""
        self.proposer_signature = ""
        self.validator_signatures = [] # List of {validator_id, signature}
        self.hash = self.calculate_hash()

    def calculate_merkle_root(self):
        from crypto_utils import sha256_hash
        if not self.transactions:
            return ""
        tx_hashes = [sha256_hash(json.dumps(tx, sort_keys=True)) for tx in self.transactions]
        # Simplified Merkle root for MVP (just hash all together)
        return sha256_hash("".join(tx_hashes))

    def calculate_hash(self):
        from crypto_utils import sha256_hash
        data = {
            "index": self.index,
            "prev_hash": self.prev_hash,
            "merkle_root": self.merkle_root,
            "timestamp": self.timestamp,
            "proposer": self.proposer
        }
        return sha256_hash(json.dumps(data, sort_keys=True))

    def to_dict(self):
        return {
            "index": self.index,
            "prev_hash": self.prev_hash,
            "merkle_root": self.merkle_root,
            "timestamp": self.timestamp,
            "transactions": self.transactions,
            "proposer": self.proposer,
            "proposer_signature": self.proposer_signature,
            "validator_signatures": self.validator_signatures,
            "hash": self.hash
        }

    @classmethod
    def from_dict(cls, data):
        block = cls(data["index"], data["prev_hash"], data["transactions"])
        block.timestamp = data["timestamp"]
        block.merkle_root = data["merkle_root"]
        block.proposer = data["proposer"]
        block.proposer_signature = data["proposer_signature"]
        block.validator_signatures = data["validator_signatures"]
        block.hash = data["hash"]
        return block

class Meeting:
    def __init__(self, meeting_id: str, title: str, agenda: List[Dict[str, Any]], initiator_id: str):
        self.meeting_id = meeting_id
        self.title = title
        self.agenda = agenda
        self.initiator_id = initiator_id
        self.status = "active"
        self.results = {} # {item_number: {option: count}}
        self.allowed_voter_ids = [] # List of IDs who can vote
        self.start_time = ""
        self.end_time = ""
        for item in agenda:
            self.results[str(item['item_number'])] = {opt: 0 for opt in item['options']}

    def to_dict(self):
        return {
            "meeting_id": self.meeting_id,
            "title": self.title,
            "agenda": self.agenda,
            "initiator_id": self.initiator_id,
            "status": self.status,
            "results": self.results,
            "allowed_voter_ids": self.allowed_voter_ids,
            "start_time": self.start_time,
            "end_time": self.end_time
        }

    @classmethod
    def from_dict(cls, data):
        meeting = cls(data["meeting_id"], data["title"], data["agenda"], data["initiator_id"])
        meeting.status = data["status"]
        meeting.results = data["results"]
        meeting.allowed_voter_ids = data.get("allowed_voter_ids", [])
        meeting.start_time = data.get("start_time", "")
        meeting.end_time = data.get("end_time", "")
        return meeting
