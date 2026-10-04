"""
Contract tests for everything committed to this repository.

Two jobs, and the second one is the reason this file exists.

1. The sample a reviewer runs the pipeline against has the shape the pipeline
   expects, so `make`-ing your way through the README actually works.

2. Nothing under data/ looks like a real subscriber record, and data/ cannot
   grow large enough to be a real extract.

This pipeline was built against telecom viewing logs, and the rule is that none of
that leaves the building. The difference between a sample and a production extract
is a number nobody checks by hand, so it is checked here instead: the guard runs in
CI on every push, and an oversized or real-looking file fails the build rather than
quietly becoming public.
"""
import json
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SAMPLE = DATA / "sample" / "example_data.jsonl"

# what the Spark job reads
TOP_LEVEL = {"_index", "_type", "_id", "_score", "_source"}
SOURCE = {"Contract", "Mac", "TotalDuration", "AppName"}

# a committed sample is a sample: a few hundred rows, not an extract
MAX_DATA_BYTES = 5 * 1024 * 1024
MAX_ROWS_PER_FILE = 50_000


def rows():
    with SAMPLE.open(encoding="utf-8") as fh:
        for i, line in enumerate(fh, 1):
            line = line.strip()
            if line:
                yield i, json.loads(line)


# ---------------------------------------------------------------- shape

def test_sample_exists_and_parses():
    assert SAMPLE.exists(), f"missing {SAMPLE.relative_to(ROOT)}"
    assert sum(1 for _ in rows()) > 0, "sample is empty"


def test_every_row_has_the_schema_the_job_reads():
    for n, row in rows():
        missing = TOP_LEVEL - row.keys()
        assert not missing, f"row {n} is missing {sorted(missing)}"
        src = row["_source"]
        missing = SOURCE - src.keys()
        assert not missing, f"row {n} _source is missing {sorted(missing)}"


def test_types_match_what_the_transforms_assume():
    for n, row in rows():
        src = row["_source"]
        assert isinstance(src["Contract"], str), f"row {n}: Contract must be a string"
        assert isinstance(src["AppName"], str), f"row {n}: AppName must be a string"
        assert isinstance(src["TotalDuration"], int), f"row {n}: TotalDuration must be an int"
        assert src["TotalDuration"] >= 0, f"row {n}: negative duration"


def test_appname_values_are_ones_the_job_maps():
    # transfrom_data() buckets AppName into content types; an unmapped value
    # would silently fall through to the else branch
    known = {"CHANNEL", "KPLUS", "KPlus", "VOD", "FIMS_RES", "BHD_RES",
             "DSHD_RES", "SPORT", "KIDS", "TV360", "RELAX", "CHILD"}
    seen = {row["_source"]["AppName"] for _, row in rows()}
    unknown = seen - known
    assert not unknown, f"AppName values the job does not map: {sorted(unknown)}"


# ---------------------------------------------------------------- leak guard

def test_identifiers_are_marked_synthetic():
    for n, row in rows():
        assert str(row["_id"]).startswith("SYN"), (
            f"row {n}: _id {row['_id']!r} is not marked synthetic. "
            "Every committed record must carry a SYN prefix."
        )


def _string_values(path):
    """Yield only the string values a file actually holds.

    Scanning raw file text produces false positives: the digits of adjacent
    numeric fields run together across delimiters and look like a phone number.
    Parse first, then inspect values.
    """
    if path.suffix == ".jsonl":
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            stack = [json.loads(line)]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    stack.extend(node.keys())
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)
                elif isinstance(node, str):
                    yield node
    else:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            for cell in re.split(r"[,;\t|]", line):
                yield cell.strip().strip('"')


def test_no_contact_details_anywhere_under_data():
    email = re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$")
    phone = re.compile(r"^(?:\+?84|0)\d{8,10}$")
    for path in sorted(DATA.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        for value in _string_values(path):
            assert not email.match(value), f"{rel} holds an email address: {value!r}"
            assert not phone.match(value), f"{rel} holds a phone number: {value!r}"


@pytest.mark.parametrize("path", sorted(p for p in DATA.rglob("*") if p.is_file()))
def test_committed_data_stays_sample_sized(path):
    # a sample is a few hundred rows; anything larger is an extract
    size = path.stat().st_size
    rel = path.relative_to(ROOT)
    assert size <= MAX_DATA_BYTES, (
        f"{rel} is {size / 1024 / 1024:.1f} MB. Committed data is capped at "
        f"{MAX_DATA_BYTES // 1024 // 1024} MB — this looks like an extract, not a sample."
    )
    if path.suffix in {".jsonl", ".csv"}:
        with path.open(encoding="utf-8", errors="ignore") as fh:
            n = sum(1 for _ in fh)
        assert n <= MAX_ROWS_PER_FILE, f"{rel} has {n:,} rows, cap is {MAX_ROWS_PER_FILE:,}"
