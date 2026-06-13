import argparse
import csv
import hashlib
from pathlib import Path

import pytest

from scraper import (
    CSV_FIELDS,
    PartRecord,
    parse_bool,
    parse_part_line,
    read_existing_rows,
    upsert_csv,
)

FULL_SCHEME_PATH = (
    "MTD Merged Data Staging - Troy-Bilt - 11-Push Walk-Behind Mowers - "
    "2026 Models - 11A-A2C2066 TB120C (2026) - "
    "Assemblies for 11A-A2C2066 TB120C (2026) - Blade"
)


def record(description: str = "Blade") -> PartRecord:
    return PartRecord(
        full_scheme_path=FULL_SCHEME_PATH,
        year="2026",
        model="11A-A2C2066 TB120C (2026)",
        assembly="Assemblies",
        scheme="Blade",
        oem="942-0741A",
        description=description,
        scraped_at="2026-06-11T10:00:00+00:00",
    )


def test_upsert_adds_new_row(tmp_path: Path) -> None:
    output = tmp_path / "parts.csv"

    stats = upsert_csv(output, [record()])

    rows = read_existing_rows(output)
    assert stats.collected == 1
    assert stats.new == 1
    assert stats.updated == 0
    assert len(rows) == 1


def test_upsert_does_not_duplicate_same_record(tmp_path: Path) -> None:
    output = tmp_path / "parts.csv"

    upsert_csv(output, [record()])
    stats = upsert_csv(output, [record()])

    rows = read_existing_rows(output)
    assert stats.collected == 1
    assert stats.new == 0
    assert stats.updated == 0
    assert len(rows) == 1


def test_writes_expected_csv_header(tmp_path: Path) -> None:
    output = tmp_path / "parts.csv"

    upsert_csv(output, [record()])

    with output.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        assert next(reader) == CSV_FIELDS


def test_unique_key_uses_path_oem_and_description() -> None:
    item = record(description="Blade Assembly")
    expected = hashlib.sha256(
        f"{item.full_scheme_path}|{item.oem}|{item.description}".encode()
    ).hexdigest()

    assert item.unique_key == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("yes", True),
        ("1", True),
        ("headless", True),
        ("false", False),
        ("no", False),
        ("0", False),
        ("headed", False),
    ],
)
def test_parse_bool_accepts_common_values(value: str, expected: bool) -> None:
    assert parse_bool(value) is expected


def test_parse_bool_rejects_unknown_value() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_bool("maybe")


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("1 942-0741A Blade 21 inch", ("942-0741A", "Blade 21 inch")),
        ("Adapter: 748-0376E", ("748-0376E", "Adapter")),
        ("Part Number Description", None),
        ("Subtotal $10.00", None),
    ],
)
def test_parse_part_line(line: str, expected: tuple[str, str] | None) -> None:
    assert parse_part_line(line) == expected
