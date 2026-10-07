"""Build-time only: fetch the exact official release, verify it, keep its source/license."""
import hashlib
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile
import urllib.request

URL = "https://github.com/official-stockfish/Stockfish/releases/download/sf_16.1/stockfish-ubuntu-x86-64-avx2.tar"
SHA256 = "4fc468eb2830db08c7340aa16a9f53f99c1847ddd8083e8c7ae84860fcde6417"
BINARY = "stockfish/stockfish-ubuntu-x86-64-avx2"


def main(destination):
    destination = Path(destination)
    with tempfile.TemporaryFile() as download:
        with urllib.request.urlopen(URL, timeout=60) as response:
            shutil.copyfileobj(response, download)
        download.seek(0)
        if hashlib.file_digest(download, "sha256").hexdigest() != SHA256:
            raise ValueError("Stockfish release checksum mismatch")
        download.seek(0)
        with tarfile.open(fileobj=download) as archive:
            for member in archive:
                parts = PurePosixPath(member.name).parts
                if not member.isfile() or ".." in parts or not parts or parts[0] != "stockfish":
                    continue
                if member.name == BINARY:
                    target = destination / "stockfish"
                elif member.name.startswith("stockfish/src/") or member.name in {
                    "stockfish/Copying.txt", "stockfish/AUTHORS", "stockfish/README.md"
                }:
                    target = destination / "source" / Path(*parts[1:])
                else:
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
        (destination / "stockfish").chmod(0o755)
        (destination / "source" / "PROVENANCE.txt").write_text(f"{URL}\nsha256: {SHA256}\n")


if __name__ == "__main__":
    main(sys.argv[1])
