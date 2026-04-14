
import json
import time
import base64
import os
import sys
import shutil
import socket
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional
from crypto_utils import generate_key_pair, sign_data, verify_signature, get_public_key_hash, sha256_hash
from models import Transaction, Block, Meeting
from validator import Validator
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

BASE_DIR = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))

_ENV_DATA_DIR = os.getenv("DDLT_DATA_DIR", "").strip()
if _ENV_DATA_DIR:
    BASE_DIR = _ENV_DATA_DIR

def _count_blocks_in_dir(dir_path: str) -> int:
    try:
        p = os.path.join(dir_path, "blockchain.json")
        if not os.path.exists(p):
            return 0
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return len(data)
        return 0
    except Exception:
        return 0

def _select_and_migrate_data_dir(base_dir: str) -> str:
    preferred = os.path.join(base_dir, "blockchain")
    candidates = [
        preferred,
        os.path.join(base_dir, "blockchain_final"),
        os.path.join(os.getcwd(), "blockchain"),
        os.path.join(os.getcwd(), "blockchain_final"),
    ]

    best_dir = preferred
    best_len = _count_blocks_in_dir(preferred)
    for c in candidates:
        l = _count_blocks_in_dir(c)
        if l > best_len:
            best_len = l
            best_dir = c

    os.makedirs(preferred, exist_ok=True)
    if best_dir != preferred and best_len > _count_blocks_in_dir(preferred):
        for name in ["blockchain.json", "blockchain.backup.json", "users.json", "validators.json", "meetings.json", "tx_pool.json"]:
            src = os.path.join(best_dir, name)
            dst = os.path.join(preferred, name)
            if os.path.exists(src):
                try:
                    shutil.copy2(src, dst)
                except Exception:
                    pass
        best_dir = preferred

    os.makedirs(best_dir, exist_ok=True)
    return best_dir

DATA_DIR = _select_and_migrate_data_dir(BASE_DIR)

BLOCKCHAIN_FILE = os.path.join(DATA_DIR, "blockchain.json")
MEETINGS_FILE = os.path.join(DATA_DIR, "meetings.json")
USERS_FILE = os.path.join(DATA_DIR, "users.json")
VALIDATORS_FILE = os.path.join(DATA_DIR, "validators.json")
POOL_FILE = os.path.join(DATA_DIR, "tx_pool.json")

class MVPEngine:
    def __init__(self):
        self.blockchain = self.load_blockchain()
        self.users = self.load_users()
        self.validators = self.load_validators()
        self.transaction_pool = self.load_pool()
        
        # ALWAYS recompute meetings from verified blockchain to prevent JSON tampering
        self.meetings = self.recompute_state_from_blockchain()
        self.recompute_user_state_from_blockchain()
        
        if not self.blockchain:
            genesis = self.create_genesis_block()
            self.blockchain.append(genesis)
            self.save_blockchain()

        if not self.validators:
            # Create default validators if none exist
            self.add_validator("Validator-Moscow", "OOO OpenDev")
            self.add_validator("Validator-SPb", "University IT")
            self.add_validator("Validator-Kazan", "Tech Hub")
            self.add_validator("Validator-EKB", "Digital Center")
            
        if self.blockchain and len(self.blockchain) > 1:
            self.verify_block_signatures()

        # Start Sync Server (simple TCP)
        self.port = int(os.getenv("DDLT_PORT", "9001"))
        self.udp_port = int(os.getenv("DDLT_UDP_PORT", "9002"))
        self.peers = [] # List of (ip, port)
        self.start_sync_server()
        self.start_discovery_listener()
        self.start_discovery_broadcast()
        self.start_periodic_sync()

    def verify_block_signatures(self) -> bool:
        validators_by_id = {v.validator_id: v for v in self.validators}
        for b in self.blockchain[1:]:
            proposer = validators_by_id.get(b.proposer)
            if proposer:
                data = json.dumps({"index": b.index, "hash": b.hash}, sort_keys=True)
                if not verify_signature(proposer.public_key, data, b.proposer_signature):
                    print(f"CRITICAL: Proposer signature invalid for block {b.index}")
                    return False
        return True

    def _validator_private_matches_public(self, v: Validator) -> bool:
        try:
            if not getattr(v, "private_key", None):
                return False
            pk = ed25519.Ed25519PrivateKey.from_private_bytes(v.private_key)
            derived = pk.public_key().public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )
            return derived == v.public_key
        except Exception:
            return False

    def _block_hash_matches(self, block: Block) -> bool:
        if block.hash == block.calculate_hash():
            return True
        legacy_proposer = block.proposer
        block.proposer = ""
        legacy_hash = block.calculate_hash()
        block.proposer = legacy_proposer
        return block.hash == legacy_hash

    def _validate_chain(self, chain: List[Block]) -> bool:
        if not chain:
            return True
        for i in range(1, len(chain)):
            prev = chain[i - 1]
            curr = chain[i]
            if curr.prev_hash != prev.hash:
                return False
            if not self._block_hash_matches(curr):
                return False
        return True

    def _verify_tx_signature(self, tx: Dict[str, Any]) -> bool:
        try:
            pk_b64 = ""
            if tx.get("type") == "vote":
                pk_b64 = tx.get("payload", {}).get("voter_public_key", "")
            elif tx.get("type") == "init_meeting":
                pk_b64 = tx.get("payload", {}).get("initiator_public_key", "")
            if not pk_b64:
                return False
            pk_bytes = base64.b64decode(pk_b64)
            signed_data = json.dumps({"id": tx.get("transaction_id", ""), "payload": tx.get("payload", {}), "time": tx.get("timestamp", "")}, sort_keys=True)
            return verify_signature(pk_bytes, signed_data, tx.get("signature", ""))
        except Exception:
            return False

    def recompute_state_from_blockchain(self) -> Dict[str, Meeting]:
        """
        Scans the entire verified blockchain to reconstruct the current state of all meetings.
        This is the only way to ensure meetings.json hasn't been tampered with.
        """
        meetings = {}
        for block in self.blockchain:
            for tx in block.transactions:
                try:
                    p = tx['payload']
                    if tx['type'] == 'init_meeting':
                        m_id = p['meeting_id']
                        if m_id not in meetings:
                            m = Meeting(p["meeting_id"], p["title"], p["agenda"], p["initiator_id"])
                            m.allowed_voter_ids = p.get("allowed_voter_ids", [])
                            m.start_time = p.get("start_time", "")
                            m.end_time = p.get("end_time", "")
                            meetings[m_id] = m
                    elif tx['type'] == 'vote':
                        m_id = p['meeting_id']
                        if m_id in meetings:
                            item_key = str(p['agenda_item'])
                            option = p['vote_option']
                            if item_key in meetings[m_id].results:
                                meetings[m_id].results[item_key][option] += 1
                except Exception as e:
                    print(f"RECOMPUTE: Error processing transaction in block {block.index}: {e}")

        # Add pending meetings from the tx pool (unconfirmed)
        for tx in self.transaction_pool:
            try:
                if tx.get("type") != "init_meeting":
                    continue
                if not self._verify_tx_signature(tx):
                    continue
                p = tx.get("payload", {})
                m_id = p.get("meeting_id")
                if not m_id or m_id in meetings:
                    continue
                m = Meeting(p["meeting_id"], p["title"], p["agenda"], p["initiator_id"])
                m.allowed_voter_ids = p.get("allowed_voter_ids", [])
                m.start_time = p.get("start_time", "")
                m.end_time = p.get("end_time", "")
                m.status = "pending"
                meetings[m_id] = m
            except Exception:
                pass
        
        # Save the recomputed state back to meetings.json only when we have evidence of real ledger state.
        # This prevents wiping local cache if blockchain files are missing/unavailable.
        self.meetings = meetings
        if len(self.blockchain) > 1 or len(self.transaction_pool) > 0:
            self.save_meetings()
        return meetings

    def recompute_user_state_from_blockchain(self):
        if len(self.blockchain) <= 1 and len(self.transaction_pool) == 0:
            return

        created_by_user: Dict[str, List[str]] = {}
        voted_by_user: Dict[str, List[Dict[str, Any]]] = {}

        titles_by_meeting: Dict[str, str] = {}
        for m_id, m in self.meetings.items():
            titles_by_meeting[m_id] = m.title

        for block in self.blockchain:
            for tx in block.transactions:
                try:
                    p = tx.get("payload", {})
                    if tx.get("type") == "init_meeting":
                        initiator_id = p.get("initiator_id", "")
                        m_id = p.get("meeting_id", "")
                        if initiator_id and m_id:
                            created_by_user.setdefault(initiator_id, []).append(m_id)
                    elif tx.get("type") == "vote":
                        voter_id = p.get("voter_id", "")
                        m_id = p.get("meeting_id", "")
                        if voter_id and m_id:
                            voted_by_user.setdefault(voter_id, []).append({
                                "meeting_id": m_id,
                                "title": titles_by_meeting.get(m_id, ""),
                                "option": p.get("vote_option", ""),
                                "transaction_id": tx.get("transaction_id", ""),
                                "time": tx.get("timestamp", ""),
                                "status": "confirmed"
                            })
                except Exception:
                    pass

        # Add pending votes/created meetings from tx pool (unconfirmed)
        for tx in self.transaction_pool:
            try:
                if not self._verify_tx_signature(tx):
                    continue
                p = tx.get("payload", {})
                if tx.get("type") == "init_meeting":
                    initiator_id = p.get("initiator_id", "")
                    m_id = p.get("meeting_id", "")
                    if initiator_id and m_id:
                        created_by_user.setdefault(initiator_id, []).append(m_id)
                elif tx.get("type") == "vote":
                    voter_id = p.get("voter_id", "")
                    m_id = p.get("meeting_id", "")
                    if voter_id and m_id:
                        voted_by_user.setdefault(voter_id, []).append({
                            "meeting_id": m_id,
                            "title": titles_by_meeting.get(m_id, ""),
                            "option": p.get("vote_option", ""),
                            "transaction_id": tx.get("transaction_id", ""),
                            "time": tx.get("timestamp", ""),
                            "status": "pending"
                        })
            except Exception:
                pass

        changed = False
        for user_name, u in self.users.items():
            uid = u.get("id", "")
            new_voted = voted_by_user.get(uid, [])
            new_created = created_by_user.get(uid, [])
            if u.get("voted_meetings") != new_voted:
                u["voted_meetings"] = new_voted
                changed = True
            if u.get("created_meetings") != new_created:
                u["created_meetings"] = new_created
                changed = True

        if changed:
            self.save_users()

    def start_discovery_listener(self):
        def listen():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('', self.udp_port))
                print(f"Discovery listener started on port {self.udp_port}...")
                my_ips = self.get_all_my_ips()
                while True:
                    data, addr = s.recvfrom(1024)
                    try:
                        msg = json.loads(data.decode('utf-8'))
                        # Filter out our own broadcast on ANY local interface
                        if msg.get("type") == "discovery_ping" and addr[0] not in my_ips:
                            peer_port = int(msg.get("port", self.port))
                            self.add_peer(addr[0], peer_port)
                            self.broadcast_discovery_ping()
                    except:
                        pass
            except Exception as e:
                print(f"Discovery listener error: {e}")

        threading.Thread(target=listen, daemon=True).start()

    def _ip_key(self, ip: str):
        try:
            parts = ip.split(".")
            if len(parts) == 4:
                return tuple(int(p) for p in parts)
        except Exception:
            pass
        return (999, 999, 999, 999)

    def _node_key(self, ip: str, port: int):
        return (self._ip_key(ip), int(port))

    def _my_best_ip(self) -> str:
        ips = [ip for ip in self.get_all_my_ips() if ip not in ["127.0.0.1", "0.0.0.0"]]
        if not ips:
            return self.get_my_ip()
        return sorted(ips, key=self._ip_key)[0]

    def _am_i_leader(self) -> bool:
        my_ip = self._my_best_ip()
        nodes = [(my_ip, self.port)] + [(p[0], p[1]) for p in self.peers]
        leader = sorted(nodes, key=lambda n: self._node_key(n[0], n[1]))[0] if nodes else (my_ip, self.port)
        return (my_ip, self.port) == leader

    def get_all_my_ips(self):
        ips = ["127.0.0.1", "0.0.0.0", self.get_my_ip()]
        try:
            # Add all local IPs to be sure
            hostname = socket.gethostname()
            ips.extend(socket.gethostbyname_ex(hostname)[2])
        except:
            pass
        return list(set(ips))

    def start_discovery_broadcast(self):
        def broadcast_loop():
            while True:
                self.broadcast_discovery_ping()
                time.sleep(2)

        threading.Thread(target=broadcast_loop, daemon=True).start()

    def pool_fingerprint(self) -> str:
        try:
            ids = sorted([tx.get("transaction_id", "") for tx in self.transaction_pool if tx.get("transaction_id")])
            return sha256_hash("|".join(ids))
        except Exception:
            return ""

    def chain_tip_hash(self) -> str:
        try:
            if self.blockchain:
                return self.blockchain[-1].hash
        except Exception:
            pass
        return ""

    def send_sync_request(self, ip: str, port: Optional[int] = None):
        if port is None:
            port = self.port
        self.send_to_peer(ip, port, {
            "type": "sync_request",
            "current_height": len(self.blockchain),
            "tip_hash": self.chain_tip_hash(),
            "port": self.port,
            "pool_size": len(self.transaction_pool),
            "pool_fingerprint": self.pool_fingerprint()
        })

    def start_periodic_sync(self):
        def loop():
            while True:
                try:
                    for peer_ip, peer_port in list(self.peers):
                        self.send_sync_request(peer_ip, peer_port)
                except Exception:
                    pass
                time.sleep(2)
        threading.Thread(target=loop, daemon=True).start()

    def broadcast_discovery_ping(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            msg = json.dumps({"type": "discovery_ping", "ip": self.get_my_ip(), "port": self.port})
            payload = msg.encode('utf-8')
            targets = set()
            targets.add(("255.255.255.255", self.udp_port))
            targets.add(("<broadcast>", self.udp_port))
            my_ip = self.get_my_ip()
            try:
                if my_ip and my_ip.count(".") == 3:
                    parts = my_ip.split(".")
                    parts[-1] = "255"
                    targets.add((".".join(parts), self.udp_port))
            except Exception:
                pass
            for host, port in targets:
                try:
                    s.sendto(payload, (host, port))
                except Exception:
                    pass
            s.close()
        except Exception as e:
            pass

    def get_my_ip(self):
        try:
            # This is a trick to get the primary IP of the machine
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"

    def start_sync_server(self):
        def listen():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('0.0.0.0', self.port))
                s.listen(5)
                print(f"Sync server listening on port {self.port}...")
                while True:
                    conn, addr = s.accept()
                    data = b""
                    max_bytes = 20 * 1024 * 1024
                    try:
                        while True:
                            chunk = conn.recv(65536)
                            if not chunk:
                                break
                            data += chunk
                            if len(data) > max_bytes:
                                raise ValueError("Sync payload too large")
                        if data:
                            msg = json.loads(data.decode('utf-8'))
                            self.handle_sync_message(msg, addr)
                    except Exception as e:
                        print(f"Sync error: {e}")
                    conn.close()
            except Exception as e:
                print(f"Sync server could not start: {e}")

        threading.Thread(target=listen, daemon=True).start()

    def handle_sync_message(self, msg, addr):
        try:
            if addr and addr[0]:
                self._remember_peer(addr[0], int(msg.get("port", self.port)))
        except Exception:
            pass

        # Full sync: if someone requests it, send them our chain
        if msg.get("type") == "sync_request":
            requester_height = msg.get("current_height", 0)
            requester_tip = msg.get("tip_hash", "")
            requester_port = int(msg.get("port", self.port))
            requester_pool_fp = msg.get("pool_fingerprint", "")
            requester_pool_size = msg.get("pool_size", 0)
            need_chain = requester_height < len(self.blockchain) or (requester_height == len(self.blockchain) and requester_tip and requester_tip != self.chain_tip_hash())
            need_pool = requester_pool_fp != self.pool_fingerprint() or requester_pool_size != len(self.transaction_pool)

            if need_chain:
                resp = {
                    "type": "sync_full_state",
                    "blockchain": [b.to_dict() for b in self.blockchain],
                    "validators": [v.to_dict() for v in self.validators],
                    "tx_pool": list(self.transaction_pool)
                }
            elif need_pool:
                resp = {"type": "sync_pool", "tx_pool": list(self.transaction_pool)}
            else:
                resp = {"type": "sync_noop"}

            self.send_to_peer(addr[0], requester_port, resp)
        
        elif msg.get("type") == "sync_full_state":
            # Remember the sender as a peer (important if discovery is one-way)
            if addr and addr[0]:
                self._remember_peer(addr[0], self.port)

            new_chain_data = msg.get("blockchain", [])
            replaced_chain = False
            if isinstance(new_chain_data, list) and (len(new_chain_data) > len(self.blockchain) or (len(new_chain_data) == len(self.blockchain) and len(new_chain_data) > 0)):
                temp_chain = [Block.from_dict(b) for b in new_chain_data]
                if not self._validate_chain(temp_chain):
                    print(f"SYNC: Rejected corrupted chain from {addr}")
                    return
                incoming_tip = temp_chain[-1].hash if temp_chain else ""
                local_tip = self.chain_tip_hash()
                should_replace = len(temp_chain) > len(self.blockchain) or (incoming_tip and local_tip and incoming_tip < local_tip)
                if should_replace:
                    print(f"SYNC: Received full state from {addr}. Replacing local chain...")
                    self.blockchain = temp_chain
                    self.save_blockchain()
                    replaced_chain = True

            # Merge validators, never dropping local validators (dropping them breaks mining).
            local_by_id = {v.validator_id: v for v in self.validators}
            new_validators_data = msg.get("validators", [])
            if isinstance(new_validators_data, list) and new_validators_data:
                for v_data in new_validators_data:
                    vid = v_data.get("validator_id", "")
                    if not vid:
                        continue
                    existing = local_by_id.get(vid)
                    if existing:
                        try:
                            existing.public_key = base64.b64decode(v_data["public_key"])
                        except Exception:
                            pass
                        existing.name = v_data.get("name", existing.name)
                        existing.organization = v_data.get("organization", existing.organization)
                        existing.status = v_data.get("status", existing.status)
                        existing.rating = v_data.get("rating", existing.rating)
                    else:
                        v = Validator(v_data.get("name", "Validator"), v_data.get("organization", "Peer"))
                        try:
                            v.public_key = base64.b64decode(v_data["public_key"])
                        except Exception:
                            v.public_key = b""
                        v.private_key = b""
                        v.validator_id = vid
                        v.status = v_data.get("status", "active")
                        v.rating = v_data.get("rating", 0.5)
                        self.validators.append(v)
                        local_by_id[vid] = v
                self.save_validators()

            # Merge pool from peer even if chain length is the same (this is what makes pending sync immediate)
            peer_pool = msg.get("tx_pool", [])
            merged_pool = False
            before_len = len(self.transaction_pool)
            if isinstance(peer_pool, list) and peer_pool:
                self._merge_pool(peer_pool)
                merged_pool = len(self.transaction_pool) != before_len

            # Clear pool of any transactions now in blockchain (after possible chain replacement)
            if replaced_chain and self.transaction_pool:
                all_tx_ids = set()
                for b in self.blockchain:
                    for tx in b.transactions:
                        all_tx_ids.add(tx['transaction_id'])
                self.transaction_pool = [tx for tx in self.transaction_pool if tx['transaction_id'] not in all_tx_ids]
                self.save_pool()

            if replaced_chain or merged_pool:
                self.meetings = self.recompute_state_from_blockchain()
                self.recompute_user_state_from_blockchain()
                print("SYNC: State updated successfully.")

        elif msg.get("type") == "sync_pool":
            peer_pool = msg.get("tx_pool", [])
            if isinstance(peer_pool, list) and peer_pool:
                self._merge_pool(peer_pool)
                self.meetings = self.recompute_state_from_blockchain()
                self.recompute_user_state_from_blockchain()

        # Very simple sync: if we get a block we don't have, add it
        elif msg.get("type") == "new_block":
            block_data = msg.get("data")
            new_block = Block.from_dict(block_data)
            
            # Double check integrity of single block
            if new_block.index == len(self.blockchain) and \
               new_block.prev_hash == self.blockchain[-1].hash and \
               self._block_hash_matches(new_block):
                proposer = next((v for v in self.validators if v.validator_id == new_block.proposer), None)
                if proposer:
                    sig_data = json.dumps({"index": new_block.index, "hash": new_block.hash}, sort_keys=True)
                    if not verify_signature(proposer.public_key, sig_data, new_block.proposer_signature):
                        print(f"SYNC: Ignored block #{new_block.index} from {addr} (invalid proposer signature)")
                        return
                
                self.blockchain.append(new_block)
                self.save_blockchain()

                # Recompute state from chain to keep UI consistent and to avoid relying on mutable meetings.json
                self.meetings = self.recompute_state_from_blockchain()
                self.recompute_user_state_from_blockchain()

                # Remove mined transactions from our pool
                mined_ids = [tx['transaction_id'] for tx in new_block.transactions]
                self.transaction_pool = [tx for tx in self.transaction_pool if tx['transaction_id'] not in mined_ids]
                self.save_pool()
                
                print(f"SYNC: Received valid new block #{new_block.index} from {addr}")
            else:
                print(f"SYNC: Ignored invalid or duplicate block #{new_block.index} from {addr}")
                # If we are behind or on a fork, request full state
                try:
                    self.send_sync_request(addr[0], self.port)
                except Exception:
                    pass
        
        elif msg.get("type") == "new_tx":
            tx_data = msg.get("data")
            self.add_to_pool(tx_data)
            # Ensure pending meetings/votes show up immediately on receiver
            self.meetings = self.recompute_state_from_blockchain()
            self.recompute_user_state_from_blockchain()

    def broadcast(self, msg_type, data):
        # Broadcast to all known peers
        msg = {"type": msg_type, "data": data, "port": self.port}
        for peer in self.peers:
            self.send_to_peer(peer[0], peer[1], msg)

    def _remember_peer(self, ip: str, port: int):
        if ip in self.get_all_my_ips():
            return
        if (ip, port) not in self.peers:
            self.peers.append((ip, port))

    def _merge_pool(self, txs: List[Dict[str, Any]]):
        existing = set(tx.get("transaction_id") for tx in self.transaction_pool)
        chain_ids = set()
        for b in self.blockchain:
            for tx in b.transactions:
                chain_ids.add(tx.get("transaction_id"))
        changed = False
        for tx in txs:
            tx_id = tx.get("transaction_id")
            if not tx_id:
                continue
            if tx_id in existing or tx_id in chain_ids:
                continue
            self.transaction_pool.append(tx)
            existing.add(tx_id)
            changed = True
        if changed:
            self.save_pool()

    def add_peer(self, ip, port=None):
        if port is None:
            port = self.port
        if ip in self.get_all_my_ips():
            return
        if (ip, port) not in self.peers:
            self.peers.append((ip, port))
            print(f"Peer added: {ip}")
            # When a peer is added, request their blockchain
            self.send_sync_request(ip, port)
            # Also proactively push our state (fixes one-way discovery / firewall edge cases)
            if len(self.blockchain) > 1 or len(self.transaction_pool) > 0:
                full_state = {
                    "type": "sync_full_state",
                    "blockchain": [b.to_dict() for b in self.blockchain],
                    "validators": [v.to_dict() for v in self.validators],
                    "tx_pool": list(self.transaction_pool)
                }
                self.send_to_peer(ip, port, full_state)

    def send_to_peer(self, ip, port, msg):
        try:
            msg_str = json.dumps(msg, ensure_ascii=False).encode('utf-8')
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(8.0)
            s.connect((ip, int(port)))
            s.sendall(msg_str)
            s.close()
        except:
            pass

    def create_genesis_block(self):
        genesis_block = Block(0, "0", [])
        genesis_block.proposer = "SYSTEM"
        genesis_block.hash = genesis_block.calculate_hash()
        return genesis_block

    # Persistence methods
    def save_blockchain(self):
        data = [b.to_dict() for b in self.blockchain]
        with open(BLOCKCHAIN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _load_chain_from_file(self, path: str) -> Optional[List[Block]]:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            chain = [Block.from_dict(b) for b in data]
            for i in range(1, len(chain)):
                prev = chain[i - 1]
                curr = chain[i]
                if curr.prev_hash != prev.hash:
                    return None
                if curr.hash != curr.calculate_hash():
                    legacy_proposer = curr.proposer
                    curr.proposer = ""
                    legacy_hash = curr.calculate_hash()
                    curr.proposer = legacy_proposer
                    if curr.hash != legacy_hash:
                        return None
                tx_hashes = []
                for tx in curr.transactions:
                    tx_hashes.append(sha256_hash(json.dumps(tx, sort_keys=True)))
                    pk_b64 = ""
                    if tx.get("type") == "vote":
                        pk_b64 = tx.get("payload", {}).get("voter_public_key", "")
                    elif tx.get("type") == "init_meeting":
                        pk_b64 = tx.get("payload", {}).get("initiator_public_key", "")
                    if pk_b64:
                        pk_bytes = base64.b64decode(pk_b64)
                        signed_data = json.dumps({"id": tx.get("transaction_id", ""), "payload": tx.get("payload", {}), "time": tx.get("timestamp", "")}, sort_keys=True)
                        if not verify_signature(pk_bytes, signed_data, tx.get("signature", "")):
                            return None
                calculated_merkle = sha256_hash("".join(tx_hashes))
                if curr.merkle_root != calculated_merkle:
                    return None
                if curr.index > 0 and not curr.proposer_signature:
                    return None
            return chain
        except Exception:
            return None

    def load_blockchain(self):
        chain = self._load_chain_from_file(BLOCKCHAIN_FILE)
        if chain is not None:
            return chain
        return []

    def save_meetings(self):
        with open(MEETINGS_FILE, "w", encoding="utf-8") as f:
            data = {m_id: m.to_dict() for m_id, m in self.meetings.items()}
            json.dump(data, f, indent=2, ensure_ascii=False)

    def load_meetings(self):
        if os.path.exists(MEETINGS_FILE):
            with open(MEETINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {m_id: Meeting.from_dict(m_data) for m_id, m_data in data.items()}
        return {}

    def save_users(self):
        # Store users with keys encoded in b64
        data = {}
        for u_id, u_info in self.users.items():
            data[u_id] = {
                "private_key": base64.b64encode(u_info["private_key"]).decode('utf-8'),
                "public_key": base64.b64encode(u_info["public_key"]).decode('utf-8'),
                "id": u_info["id"],
                "password_hash": u_info.get("password_hash", ""),
                "voted_meetings": u_info.get("voted_meetings", [])
            }
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def load_users(self):
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                users = {}
                for u_id, u_info in data.items():
                    public_key = base64.b64decode(u_info["public_key"])
                    users[u_id] = {
                        "private_key": base64.b64decode(u_info["private_key"]),
                        "public_key": public_key,
                        "id": get_public_key_hash(public_key),
                        "password_hash": u_info.get("password_hash", ""),
                        "voted_meetings": [],
                        "created_meetings": []
                    }
                return users
        return {}

    def save_validators(self):
        with open(VALIDATORS_FILE, "w", encoding="utf-8") as f:
            data = []
            for v in self.validators:
                v_data = v.to_dict()
                if self._validator_private_matches_public(v):
                    v_data["private_key"] = base64.b64encode(v.private_key).decode("utf-8")
                data.append(v_data)
            json.dump(data, f, indent=2, ensure_ascii=False)

    def load_validators(self):
        if os.path.exists(VALIDATORS_FILE):
            with open(VALIDATORS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                validators = []
                for v_data in data:
                    v = Validator(v_data["name"], v_data["organization"])
                    # Restore keys and ID
                    v.public_key = base64.b64decode(v_data["public_key"])
                    if v_data.get("private_key"):
                        v.private_key = base64.b64decode(v_data["private_key"])
                    else:
                        v.private_key = b""
                    v.validator_id = v_data["validator_id"]
                    # For MVP we regenerate private key or mock it if we don't save it
                    # In a real system, the validator would have its own private key
                    v.status = v_data["status"]
                    v.rating = v_data["rating"]
                    validators.append(v)
                return validators
        return []

    def save_pool(self):
        with open(POOL_FILE, "w", encoding="utf-8") as f:
            json.dump(self.transaction_pool, f, indent=2, ensure_ascii=False)

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return []

    # Business Logic
    def add_validator(self, name: str, organization: str):
        validator = Validator(name, organization)
        self.validators.append(validator)
        self.save_validators()
        return validator

    def register_user(self, user_name: str, password: str = ""):
        if user_name in self.users:
            return False, "User already exists"
        
        private_bytes, public_bytes = generate_key_pair()
        self.users[user_name] = {
            "private_key": private_bytes,
            "public_key": public_bytes,
            "id": get_public_key_hash(public_bytes),
            "password_hash": sha256_hash(password) if password else "",
            "voted_meetings": []
        }
        self.save_users()
        return True, self.users[user_name]

    def login_user(self, user_name: str, password: str = ""):
        if user_name not in self.users:
            return False, "Пользователь не найден"
        
        stored_hash = self.users[user_name].get("password_hash", "")
        if stored_hash and sha256_hash(password) != stored_hash:
            return False, "Неверный пароль"
            
        return True, self.users[user_name]

    def create_meeting(self, user_name: str, title: str, agenda: List[Dict[str, Any]], start_time: str, end_time: str, allowed_voter_ids: List[str], validator_ids: List[str]):
        user = self.users[user_name]
        # Generate meeting ID as a hash of title + initiator + timestamp
        meeting_data = f"{title}{user['id']}{time.time()}"
        meeting_id = sha256_hash(meeting_data)[:32] # 32 chars hash for meeting
        meeting = Meeting(meeting_id, title, agenda, user['id'])
        meeting.start_time = start_time
        meeting.end_time = end_time
        meeting.allowed_voter_ids = allowed_voter_ids
        
        payload = {
            "type": "init_meeting",
            "meeting_id": meeting_id,
            "title": title,
            "agenda": agenda,
            "start_time": start_time,
            "end_time": end_time,
            "initiator_id": user['id'],
            "initiator_public_key": base64.b64encode(user['public_key']).decode('utf-8'),
            "allowed_voter_ids": allowed_voter_ids,
            "validators": validator_ids
        }
        tx = Transaction("init_meeting", payload)
        tx.sign(user['private_key'])
        
        # Local validation for meeting creation
        confirmations = []
        signable_validators = 0
        for val_id in validator_ids:
            validator = next((v for v in self.validators if v.validator_id == val_id), None)
            if validator and validator.status == "active" and self._validator_private_matches_public(validator):
                signable_validators += 1
                # Create a confirmation (simplified for init_meeting)
                conf_data = json.dumps({"tx_id": tx.transaction_id, "time": tx.timestamp}, sort_keys=True)
                confirmation = {
                    "transaction_id": tx.transaction_id,
                    "validator_id": validator.validator_id,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "signature": sign_data(validator.private_key, conf_data)
                }
                confirmations.append(confirmation)
        
        tx_dict = tx.to_dict()
        tx_dict["validator_confirmations"] = confirmations
        
        self.meetings[meeting_id] = meeting
        self.add_to_pool(tx_dict)
        self.save_meetings()
        
        if "created_meetings" not in self.users[user_name]:
            self.users[user_name]["created_meetings"] = []
        self.users[user_name]["created_meetings"].append(meeting_id)
        self.save_users()
        
        # If the user selected validators but none of them can sign on this node, we still accept as pending.
        # If some can sign, require at least 1 confirmation to prevent silent failures.
        if signable_validators > 0 and len(confirmations) < 1:
            return False, "Недостаточно подтверждений от валидаторов"
        return True, meeting_id

    def cast_vote(self, user_name: str, meeting_id: str, agenda_item: int, option: str, selected_validator_ids: List[str], threshold: int):
        user = self.users[user_name]
        meeting = self.meetings.get(meeting_id)
        if not meeting:
            return False, "Голосование не найдено"

        # Check if user is allowed
        if meeting.allowed_voter_ids and user['id'] not in meeting.allowed_voter_ids:
            return False, "Вы не в списке разрешенных участников"

        # Check time
        now = datetime.utcnow().isoformat() + "Z"
        if meeting.start_time and now < meeting.start_time:
            return False, f"Голосование еще не началось (Старт: {meeting.start_time})"
        if meeting.end_time and now > meeting.end_time:
            return False, f"Голосование уже окончено (Конец: {meeting.end_time})"

        # Prevent double voting
        voted_list = user.get("voted_meetings", [])
        if any(v['meeting_id'] == meeting_id for v in voted_list):
            return False, "Вы уже проголосовали в этом собрании"

        payload = {
            "type": "vote",
            "meeting_id": meeting_id,
            "agenda_item": agenda_item,
            "voter_id": user['id'],
            "voter_public_key": base64.b64encode(user['public_key']).decode('utf-8'),
            "vote_option": option,
            "selected_validators": selected_validator_ids,
            "validation_threshold": threshold
        }
        tx = Transaction("vote", payload)
        tx.sign(user['private_key'])
        tx_dict = tx.to_dict()
        
        confirmations = []
        signable_validators = 0
        for val_id in selected_validator_ids:
            validator = next((v for v in self.validators if v.validator_id == val_id), None)
            if validator and validator.status == "active" and self._validator_private_matches_public(validator):
                signable_validators += 1
                # Create a simple mapping for validator's validate_transaction
                meeting_map = {meeting_id: self.meetings[meeting_id]}
                confirmation = validator.validate_transaction(tx_dict, meeting_map)
                if confirmation:
                    confirmations.append(confirmation)
                
        # If user selected N validators but only K of them can actually sign on this node (K < threshold),
        # don't block the vote forever with "Threshold not met" — accept as pending/soft-confirmed.
        effective_threshold = threshold
        if signable_validators == 0:
            effective_threshold = 0
        elif signable_validators < threshold:
            effective_threshold = signable_validators

        ok = len(confirmations) >= effective_threshold
        if ok:
            tx_dict["validator_confirmations"] = confirmations
            self.add_to_pool(tx_dict)
            
            if "voted_meetings" not in user:
                user["voted_meetings"] = []
            
            user["voted_meetings"].append({
                "meeting_id": meeting_id,
                "title": self.meetings[meeting_id].title,
                "option": option,
                "transaction_id": tx_dict["transaction_id"],
                "time": datetime.utcnow().isoformat() + "Z",
                "status": "confirmed" if len(confirmations) >= effective_threshold and effective_threshold > 0 else "pending"
            })
            self.save_users()
            
            return True, tx_dict["transaction_id"]
        return False, "Threshold not met"

    def add_to_pool(self, tx_dict: Dict[str, Any]):
        # Check if transaction already exists in pool or blockchain
        tx_id = tx_dict.get("transaction_id")
        
        # 1. Check if already in pool
        if any(tx.get("transaction_id") == tx_id for tx in self.transaction_pool):
            print(f"DEBUG: Transaction {tx_id} already in pool. Skipping.")
            return
        
        # 2. Check if already in any block of the blockchain
        for block in self.blockchain:
            if any(tx.get("transaction_id") == tx_id for tx in block.transactions):
                print(f"DEBUG: Transaction {tx_id} already in blockchain. Skipping.")
                # Remove from pool if somehow it got there
                old_len = len(self.transaction_pool)
                self.transaction_pool = [tx for tx in self.transaction_pool if tx.get("transaction_id") != tx_id]
                if len(self.transaction_pool) != old_len:
                    self.save_pool()
                return

        # 3. Add to pool
        self.transaction_pool.append(tx_dict)
        self.save_pool()
        print(f"DEBUG: Added TX to pool. Current pool size: {len(self.transaction_pool)}")
        
        # 4. Broadcast transaction to peers
        self.broadcast("new_tx", tx_dict)
        
        # 5. Mining Logic: ONLY if we have AT LEAST 3 transactions
        # We use >= 3 because multiple transactions might arrive quickly
        if len(self.transaction_pool) >= 3:
            print(f"DEBUG: Pool reached {len(self.transaction_pool)} TXs. Triggering auto_mine.")
            self.auto_mine()
        else:
            print(f"DEBUG: Pool size {len(self.transaction_pool)} < 3. Waiting for more TXs.")

    def auto_mine(self):
        new_index = len(self.blockchain)
        
        # Double check if someone already mined this block and synced it to us
        if any(b.index == new_index for b in self.blockchain):
            print(f"DEBUG: Block #{new_index} already exists. Skipping auto_mine.")
            return

        # STRICT CHECK: Only mine if we have at least 3 transactions
        if len(self.transaction_pool) < 3:
            print(f"DEBUG: auto_mine aborted. Pool size {len(self.transaction_pool)} < 3.")
            return

        print(f"DEBUG: Starting mining for block #{new_index}...")

        # Avoid forks: if we have peers, only the leader mines
        if self.peers and not self._am_i_leader():
            print("DEBUG: Not leader, skipping mining to avoid forks.")
            return

        # Select first active validator as proposer
        proposer = next((v for v in self.validators if v.status == "active" and self._validator_private_matches_public(v)), None)
        if not proposer:
            print("DEBUG: No signing validator available; cannot mine.")
            return

        prev_hash = self.blockchain[-1].hash
        # Take EXACTLY 3 transactions for the block
        mining_txs = self.transaction_pool[:3]
        
        new_block = Block(new_index, prev_hash, mining_txs)
        new_block.proposer = proposer.validator_id
        new_block.hash = new_block.calculate_hash()
        
        # Proposer signs
        block_data = json.dumps({"index": new_block.index, "hash": new_block.hash}, sort_keys=True)
        new_block.proposer_signature = sign_data(proposer.private_key, block_data)
        
        # Collect signatures from others (simulated consensus)
        for val in self.validators:
            if val.validator_id != proposer.validator_id and self._validator_private_matches_public(val):
                sig = sign_data(val.private_key, block_data)
                new_block.validator_signatures.append({
                    "validator_id": val.validator_id,
                    "signature": sig
                })
        
        self.blockchain.append(new_block)
        
        # Update meetings results
        for tx in new_block.transactions:
            if tx['type'] == 'vote':
                p = tx['payload']
                m_id = p['meeting_id']
                if m_id in self.meetings:
                    self.meetings[m_id].results[str(p['agenda_item'])][p['vote_option']] += 1
            elif tx['type'] == 'init_meeting':
                # Already added in memory, but could re-verify here
                pass
                
        # Remove mined transactions from pool
        mined_ids = [tx['transaction_id'] for tx in new_block.transactions]
        self.transaction_pool = [tx for tx in self.transaction_pool if tx['transaction_id'] not in mined_ids]
        
        self.save_blockchain()
        self.save_meetings()
        self.save_pool()
        # Broadcast block
        self.broadcast("new_block", new_block.to_dict())
        print(f"AUTOMINE: Block #{new_block.index} created!")

    def get_meetings_list(self):
        return [m.to_dict() for m in self.meetings.values()]

    def get_blockchain_height(self):
        return len(self.blockchain)
