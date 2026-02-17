from __future__ import annotations

import base64
import hashlib
from pathlib import Path


def _get_fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError(
            "Encryption requested but 'cryptography' is not installed. "
            "Install it with: pip install cryptography"
        ) from exc
    return Fernet


def _derive_fernet_key(secret: str) -> bytes:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def encrypt_file(input_path: str, secret: str, output_path: str | None = None, remove_original: bool = False) -> str:
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Input file not found for encryption: {source}")

    target = Path(output_path) if output_path else source.with_suffix(source.suffix + ".enc")
    target.parent.mkdir(parents=True, exist_ok=True)

    fernet_cls = _get_fernet()
    fernet = fernet_cls(_derive_fernet_key(secret))

    data = source.read_bytes()
    encrypted = fernet.encrypt(data)
    target.write_bytes(encrypted)

    if remove_original:
        source.unlink(missing_ok=True)

    return str(target)


def decrypt_file(input_path: str, secret: str, output_path: str | None = None, remove_original: bool = False) -> str:
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Input file not found for decryption: {source}")

    if source.suffix != ".enc" and output_path is None:
        raise ValueError("Auto output path for decryption requires a .enc file.")

    target = Path(output_path) if output_path else source.with_suffix("")
    target.parent.mkdir(parents=True, exist_ok=True)

    fernet_cls = _get_fernet()
    fernet = fernet_cls(_derive_fernet_key(secret))

    encrypted = source.read_bytes()
    decrypted = fernet.decrypt(encrypted)
    target.write_bytes(decrypted)

    if remove_original:
        source.unlink(missing_ok=True)

    return str(target)
