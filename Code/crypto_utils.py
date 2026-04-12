
import hashlib
import json
import base64
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

def sha256_hash(data: str) -> str:
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

def generate_key_pair():
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    
    private_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    public_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    )
    
    return private_bytes, public_bytes

def sign_data(private_bytes: bytes, data: str) -> str:
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(private_bytes)
    signature = private_key.sign(data.encode('utf-8'))
    return base64.b64encode(signature).decode('utf-8')

def verify_signature(public_bytes: bytes, data: str, signature_b64: str) -> bool:
    try:
        if not signature_b64:
            return False
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(public_bytes)
        signature = base64.b64decode(signature_b64)
        public_key.verify(signature, data.encode('utf-8'))
        return True
    except Exception:
        return False

def get_public_key_hash(public_bytes: bytes) -> str:
    return sha256_hash(base64.b64encode(public_bytes).decode('utf-8'))
