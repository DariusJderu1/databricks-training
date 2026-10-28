"""Shared helpers for the generator: deterministic RNG, fast value pools,
chunked multi-format writers, and the data-quality (DQ) defect injectors.

This module is pure Python / NumPy / pandas — there is no Spark here. The bulk
data is produced with vectorised NumPy and written with pandas' fast C writers;
Faker is only used to build small *pools* of realistic strings (names, streets)
that we then sample by index, which keeps even the 500 MB run to a couple of
minutes.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from data_generator import config


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #
def make_rng(seed: int) -> np.random.Generator:
    """The single source of randomness — pass this everywhere for reproducibility."""
    return np.random.default_rng(seed)


def make_faker(seed: int) -> Faker:
    fk = Faker("en_US")
    Faker.seed(seed)
    return fk


def build_pools(
    fk: Faker,
    n_first: int = 12_000,
    n_last: int = 12_000,
    n_streets: int = 8_000,
    n_cities: int = 4_000,
) -> dict[str, np.ndarray]:
    """Build small pools of realistic strings once, then sample by index.

    Sampling indices into these pools (rather than calling Faker per row) is what
    makes generating hundreds of thousands of rows fast while staying realistic.
    """
    return {
        "first": np.array([fk.first_name() for _ in range(n_first)], dtype=object),
        "last": np.array([fk.last_name() for _ in range(n_last)], dtype=object),
        "streets": np.array([fk.street_address() for _ in range(n_streets)], dtype=object),
        "cities": np.array([fk.city() for _ in range(n_cities)], dtype=object),
    }


# --------------------------------------------------------------------------- #
# Vectorised id + date helpers
# --------------------------------------------------------------------------- #
def make_ids(prefix: str, start: int, n: int, width: int) -> np.ndarray:
    """Zero-padded business keys, e.g. make_ids('CUST', 1, 3, 8) -> CUST00000001..."""
    nums = np.arange(start, start + n)
    return np.char.add(prefix, np.char.zfill(nums.astype(str), width))


def random_dates(rng: np.random.Generator, start: str, end: str, n: int) -> np.ndarray:
    """n random calendar dates in [start, end) as datetime64[D]."""
    s = np.datetime64(start, "D")
    e = np.datetime64(end, "D")
    span = int((e - s) / np.timedelta64(1, "D"))
    offs = rng.integers(0, max(span, 1), n)
    return s + offs.astype("timedelta64[D]")


def random_datetimes(rng: np.random.Generator, start: str, end: str, n: int) -> np.ndarray:
    """n random timestamps in [start, end) as datetime64[s]."""
    s = np.datetime64(start, "s")
    e = np.datetime64(end, "s")
    span = int((e - s) / np.timedelta64(1, "s"))
    offs = rng.integers(0, max(span, 1), n)
    return s + offs.astype("timedelta64[s]")


def iso_date(arr: np.ndarray) -> np.ndarray:
    return np.datetime_as_string(arr.astype("datetime64[D]"), unit="D")


def iso_ts(arr: np.ndarray) -> np.ndarray:
    return np.datetime_as_string(arr.astype("datetime64[s]"), unit="s")


# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #
def load_reference(name: str) -> pd.DataFrame:
    return pd.read_csv(config.REFERENCE_DIR / name, dtype=str)


def copy_reference_to_landing(out_dir: Path) -> list[Path]:
    """Copy the committed lookup CSVs into landing/reference (batch read_files source)."""
    dst = Path(out_dir) / "reference"
    dst.mkdir(parents=True, exist_ok=True)
    written = []
    for fname in config.REFERENCE_FILES:
        target = dst / fname
        shutil.copyfile(config.REFERENCE_DIR / fname, target)
        written.append(target)
    return written


# --------------------------------------------------------------------------- #
# Chunked writers (many files per source)
# --------------------------------------------------------------------------- #
def _chunk_bounds(n: int, rows_per_file: int):
    for i, start in enumerate(range(0, n, rows_per_file)):
        yield i, start, min(start + rows_per_file, n)


def write_csv_chunks(df: pd.DataFrame, out_dir: Path, prefix: str, rows_per_file: int) -> list[Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, a, b in _chunk_bounds(len(df), rows_per_file):
        p = out_dir / f"{prefix}_{i:04d}.csv"
        df.iloc[a:b].to_csv(p, index=False)
        paths.append(p)
    return paths


def write_parquet_chunks(df: pd.DataFrame, out_dir: Path, prefix: str, rows_per_file: int) -> list[Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, a, b in _chunk_bounds(len(df), rows_per_file):
        p = out_dir / f"{prefix}_{i:04d}.parquet"
        df.iloc[a:b].to_parquet(p, index=False, engine="pyarrow")
        paths.append(p)
    return paths


def write_ndjson_chunks(df: pd.DataFrame, out_dir: Path, prefix: str, rows_per_file: int,
                        ext: str = "json") -> list[Path]:
    """Newline-delimited JSON (one object per line) — Auto Loader's default JSON shape."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, a, b in _chunk_bounds(len(df), rows_per_file):
        p = out_dir / f"{prefix}_{i:04d}.{ext}"
        df.iloc[a:b].to_json(p, orient="records", lines=True, date_format="iso")
        paths.append(p)
    return paths


def write_json_array_chunks(records: list[dict], out_dir: Path, prefix: str,
                            rows_per_file: int) -> list[Path]:
    """Compact JSON arrays per file (a 'JSON export' look) — read with multiLine=true.

    Used for the nested ``policies`` source so participants exercise the
    ``multiLine`` Auto Loader / read_files option and array explosion in silver.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, a, b in _chunk_bounds(len(records), rows_per_file):
        p = out_dir / f"{prefix}_{i:04d}.json"
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(records[a:b], fh, default=str)
        paths.append(p)
    return paths


def dir_size_bytes(path: Path) -> int:
    total = 0
    for p in Path(path).rglob("*"):
        if p.is_file():
            total += p.stat().st_size
    return total


def write_manifest(out_dir: Path, payload: dict) -> Path:
    p = Path(out_dir) / config.MANIFEST_NAME
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    return p


def read_manifest(out_dir: Path) -> dict | None:
    p = Path(out_dir) / config.MANIFEST_NAME
    if not p.exists():
        return None
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------- #
# Data-quality defect injectors
# --------------------------------------------------------------------------- #
# Each function mutates/returns a pandas object with a *configurable fraction* of
# rows corrupted. They are deterministic given the shared rng. Every defect maps
# to a documented silver cleaning task (see docs/03_data_dictionary.md).
def dq_mask(rng: np.random.Generator, n: int, rate: float) -> np.ndarray:
    if rate <= 0 or n == 0:
        return np.zeros(n, dtype=bool)
    return rng.random(n) < rate


def inject_nulls(df: pd.DataFrame, cols, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    for c in cols:
        m = dq_mask(rng, len(df), rate)
        df.loc[m, c] = None
    return df


def inject_casing_whitespace(s: pd.Series, rng: np.random.Generator, rate: float) -> pd.Series:
    s = s.astype(object).copy()
    vals = s.values
    idx = np.flatnonzero(dq_mask(rng, len(s), rate))
    choice = rng.integers(0, 4, idx.size)
    for j, i in enumerate(idx):
        v = vals[i]
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        sv = str(v)
        c = choice[j]
        if c == 0:
            vals[i] = sv.upper()
        elif c == 1:
            vals[i] = sv.lower()
        elif c == 2:
            vals[i] = f"  {sv} "
        else:
            vals[i] = sv.replace(" ", "  ")
    return s


def inject_invalid_category(s: pd.Series, rng: np.random.Generator, rate: float,
                            invalid_pool) -> pd.Series:
    s = s.astype(object).copy()
    vals = s.values
    idx = np.flatnonzero(dq_mask(rng, len(s), rate))
    picks = rng.integers(0, len(invalid_pool), idx.size)
    for j, i in enumerate(idx):
        vals[i] = invalid_pool[picks[j]]
    return s


def inject_numeric_as_string(s: pd.Series, rng: np.random.Generator, rate: float) -> pd.Series:
    s = s.astype(object).copy()
    vals = s.values
    idx = np.flatnonzero(dq_mask(rng, len(s), rate))
    fmts = rng.integers(0, 3, idx.size)
    for j, i in enumerate(idx):
        v = vals[i]
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        f = fmts[j]
        if f == 0:
            vals[i] = f"${float(v):,.2f}"      # "$1,234.50"
        elif f == 1:
            vals[i] = f"  {v} "                  # padded
        else:
            vals[i] = str(v).replace(".", ",")   # comma decimal
    return s


def corrupt_dates(s: pd.Series, rng: np.random.Generator, rate: float) -> pd.Series:
    """Turn a fraction of ISO 'YYYY-MM-DD' strings into mixed/future/malformed dates."""
    s = s.astype(object).copy()
    vals = s.values
    idx = np.flatnonzero(dq_mask(rng, len(s), rate))
    kinds = rng.integers(0, 4, idx.size)
    for j, i in enumerate(idx):
        v = vals[i]
        if v is None:
            continue
        parts = str(v).split("-")
        if len(parts) < 3:
            continue
        y, mo, d = parts[0], parts[1], parts[2]
        k = kinds[j]
        if k == 0:
            vals[i] = f"{mo}/{d}/{y}"            # US M/D/Y
        elif k == 1:
            vals[i] = f"{d}-{mo}-{y}"            # D-M-Y
        elif k == 2:
            vals[i] = f"{int(y) + 5}-{mo}-{d}"   # implausible future date
        else:
            vals[i] = "not_a_date"               # malformed
    return s


def append_exact_duplicates(df: pd.DataFrame, rng: np.random.Generator, rate: float) -> pd.DataFrame:
    k = int(len(df) * rate)
    if k <= 0:
        return df
    idx = rng.integers(0, len(df), k)
    return pd.concat([df, df.iloc[idx]], ignore_index=True)


def append_fuzzy_duplicates(df: pd.DataFrame, rng: np.random.Generator, rate: float,
                            mutate_cols) -> pd.DataFrame:
    """Near-duplicates: same business key, one or more attributes lightly mutated."""
    k = int(len(df) * rate)
    if k <= 0:
        return df
    idx = rng.integers(0, len(df), k)
    dup = df.iloc[idx].copy().reset_index(drop=True)
    for c in mutate_cols:
        if c in dup.columns:
            dup[c] = inject_casing_whitespace(dup[c], rng, 1.0)
    return pd.concat([df, dup], ignore_index=True)


def shuffle(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Intersperse injected duplicates so dedup can't be a naive tail-trim."""
    return df.iloc[rng.permutation(len(df))].reset_index(drop=True)
