from __future__ import annotations

import gzip
import shutil
from pathlib import Path


def compress_file(input_path: str, output_path: str | None = None, remove_original: bool = False) -> str:
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Input file not found for compression: {source}")

    target = Path(output_path) if output_path else source.with_suffix(source.suffix + ".gz")
    target.parent.mkdir(parents=True, exist_ok=True)

    with source.open("rb") as src, gzip.open(target, "wb") as dst:
        shutil.copyfileobj(src, dst)

    if remove_original:
        source.unlink(missing_ok=True)

    return str(target)


def decompress_file(input_path: str, output_path: str | None = None, remove_original: bool = False) -> str:
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Input file not found for decompression: {source}")

    if source.suffix != ".gz" and output_path is None:
        raise ValueError("Auto output path for decompression requires a .gz file.")

    if output_path:
        target = Path(output_path)
    else:
        target = source.with_suffix("")

    target.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(source, "rb") as src, target.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    if remove_original:
        source.unlink(missing_ok=True)

    return str(target)
