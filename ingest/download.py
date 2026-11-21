"""Download one month of the REES46 e-commerce dataset into data/raw/.

    python -m ingest.download                          # 2019-Oct.csv from Kaggle (needs credentials)
    python -m ingest.download --file 2019-Nov.csv
    python -m ingest.download --source mirror          # 2019-Oct.csv from an unofficial Hugging Face copy
    python -m ingest.download --sample-mb 100          # first ~100 MB of the gzip -> 2019-Oct-sample.csv

Kaggle credentials come from ~/.kaggle/kaggle.json or KAGGLE_USERNAME / KAGGLE_KEY
(environment or .env). Kaggle serves the file zipped; it is unzipped to a plain
.csv and the zip is deleted.

--sample-mb fetches only the first N MB of the mirror's gzip with an HTTP Range
request and decompresses it on the fly, keeping complete lines only. The rows are
ordered by event_time, so the sample is simply "the first few days".
"""
import argparse
import os
import shutil
import sys
import time
import urllib.request
import zipfile
import zlib
from pathlib import Path

from dotenv import load_dotenv

from ingest.config import PROJECT_DIR

DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
MIRROR_URL = "https://huggingface.co/datasets/skpy/E_Commerce_Behavioral_Analysis/resolve/main/2019-Oct.csv.gz"
MIRROR_FILE = "2019-Oct.csv"
RAW_DIR = PROJECT_DIR / "data" / "raw"
CHUNK = 1024 * 1024


class Progress:
    """Prints "<what>: 123 / 456 MB (27%)" at most every few seconds."""

    def __init__(self, what, total=None, every=5.0):
        self.what, self.total, self.every = what, total, every
        self.done = 0
        self.started = self.last = time.monotonic()

    def update(self, n):
        self.done += n
        now = time.monotonic()
        if now - self.last >= self.every:
            self.last = now
            self.print()

    def print(self):
        mb = self.done / CHUNK
        rate = mb / max(time.monotonic() - self.started, 1e-9)
        if self.total:
            pct = 100 * self.done / self.total
            print(f"  {self.what}: {mb:,.0f} / {self.total / CHUNK:,.0f} MB ({pct:.0f}%), {rate:.1f} MB/s", flush=True)
        else:
            print(f"  {self.what}: {mb:,.0f} MB, {rate:.1f} MB/s", flush=True)


def kaggle_credentials_present():
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True
    config_dir = Path(os.getenv("KAGGLE_CONFIG_DIR") or Path.home() / ".kaggle")
    return (config_dir / "kaggle.json").exists()


def download_kaggle(file_name, out):
    # .env.example ships with empty KAGGLE_USERNAME= / KAGGLE_KEY=. The kaggle client
    # would take those empty strings over kaggle.json, so drop them.
    for var in ("KAGGLE_USERNAME", "KAGGLE_KEY"):
        if var in os.environ and not os.environ[var]:
            del os.environ[var]
    if not kaggle_credentials_present():
        raise SystemExit(
            "No Kaggle credentials found. Either put kaggle.json in ~/.kaggle/ "
            "(kaggle.com -> Settings -> API -> Create New Token) or set KAGGLE_USERNAME and "
            "KAGGLE_KEY in .env.\nWithout an account: python -m ingest.download --source mirror"
        )

    # `import kaggle` authenticates at import time, so it has to come after load_dotenv()
    import kaggle

    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {file_name} from kaggle.com/datasets/{DATASET} ...")
    kaggle.api.dataset_download_file(DATASET, file_name, path=str(out.parent), force=True, quiet=False)

    # the client names the file after the download URL: usually "2019-Oct.csv.zip"
    candidates = [out.parent / (file_name + ".zip"), out.parent / (Path(file_name).stem + ".zip")]
    zipped = next((p for p in candidates if p.exists()), None)
    if zipped is None:
        plain = out.parent / file_name
        if plain.exists():  # small files sometimes come unzipped
            if plain != out:
                plain.replace(out)
            return
        raise SystemExit(f"expected {candidates[0]} after the download, it isn't there")

    print(f"unzipping {zipped.name} ...")
    with zipfile.ZipFile(zipped) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".csv"))
        tmp = out.with_name(out.name + ".part")
        with zf.open(member) as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, CHUNK)
    tmp.replace(out)
    zipped.unlink()


def open_url(url, headers=None):
    request = urllib.request.Request(url, headers={"User-Agent": "warehouse-costs-download", **(headers or {})})
    return urllib.request.urlopen(request, timeout=60)


def download_mirror(out):
    """Stream the whole gzip and decompress it straight to a .csv (no .gz kept on disk)."""
    print(f"downloading {MIRROR_URL}\n  (unofficial mirror of the Kaggle file) ...")
    tmp = out.with_name(out.name + ".part")
    out.parent.mkdir(parents=True, exist_ok=True)
    gunzip = zlib.decompressobj(16 + zlib.MAX_WBITS)
    with open_url(MIRROR_URL) as response, open(tmp, "wb") as dst:
        progress = Progress("downloaded", int(response.headers.get("Content-Length") or 0) or None)
        while True:
            chunk = response.read(CHUNK)
            if not chunk:
                break
            progress.update(len(chunk))
            dst.write(gunzip.decompress(chunk))
        dst.write(gunzip.flush())
        progress.print()
    if not gunzip.eof:
        tmp.unlink()
        raise SystemExit("download ended before the end of the gzip stream - try again")
    tmp.replace(out)


def download_sample(out, megabytes):
    """First `megabytes` MB of the mirror's gzip -> complete CSV lines in `out`."""
    want = int(megabytes * CHUNK)
    print(f"downloading the first {megabytes} MB of {MIRROR_URL}\n  (unofficial mirror of the Kaggle file) ...")
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".part")
    gunzip = zlib.decompressobj(16 + zlib.MAX_WBITS)
    tail = b""  # decompressed bytes after the last newline written so far
    lines = 0
    with open_url(MIRROR_URL, {"Range": f"bytes=0-{want - 1}"}) as response, open(tmp, "wb") as dst:
        progress = Progress("downloaded", want)
        received = 0
        while received < want:  # a server that ignores Range sends everything; stop ourselves
            chunk = response.read(min(CHUNK, want - received))
            if not chunk:
                break
            received += len(chunk)
            progress.update(len(chunk))
            data = tail + gunzip.decompress(chunk)
            cut = data.rfind(b"\n") + 1
            dst.write(data[:cut])
            lines += data.count(b"\n", 0, cut)
            tail = data[cut:]
        progress.print()
    # the truncated gzip never reaches its end, so whatever is left in `tail` is a partial line
    tmp.replace(out)
    print(f"kept {lines - 1:,} complete rows (+ header)")


def main():
    load_dotenv(PROJECT_DIR / ".env")

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", default=MIRROR_FILE, help="dataset file, e.g. 2019-Oct.csv or 2019-Nov.csv (default: %(default)s)")
    parser.add_argument("--source", choices=["kaggle", "mirror"], default="kaggle",
                        help="kaggle (official, needs an API token) or mirror = unofficial Hugging Face copy "
                             "(huggingface.co/datasets/skpy/E_Commerce_Behavioral_Analysis), only 2019-Oct.csv "
                             "(default: %(default)s)")
    parser.add_argument("--sample-mb", type=float, metavar="N",
                        help="only download the first N MB of the mirror's gzip (implies --source mirror); "
                             "writes data/raw/2019-Oct-sample.csv")
    parser.add_argument("--out", type=Path, help="output path (default: data/raw/<file>, or data/raw/2019-Oct-sample.csv with --sample-mb)")
    parser.add_argument("--force", action="store_true", help="overwrite the output file if it exists")
    args = parser.parse_args()

    source = "mirror" if args.sample_mb else args.source
    if source == "mirror" and args.file != MIRROR_FILE:
        raise SystemExit(f"the mirror only has {MIRROR_FILE}; use --source kaggle for {args.file}")

    if args.out:
        out = args.out.resolve()
    elif args.sample_mb:
        out = RAW_DIR / MIRROR_FILE.replace(".csv", "-sample.csv")
    else:
        out = RAW_DIR / args.file
    if out.exists() and not args.force:
        raise SystemExit(f"{out} already exists (use --force to overwrite)")

    started = time.monotonic()
    if args.sample_mb:
        download_sample(out, args.sample_mb)
    elif source == "mirror":
        download_mirror(out)
    else:
        download_kaggle(args.file, out)
    print(f"wrote {out} ({out.stat().st_size / CHUNK:,.0f} MB) in {time.monotonic() - started:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
