#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Folder → (one CSV at a time) Wide→Tall → Postgres (chunked, dedupe, progress, long-name safe)

Highlights:
- Idempotent dedupe: natural_key EXCLUDES CCN & unique_id.
- Ingestion log: fingerprints files; unchanged files SKIP unless --force.
- Windows-safe: long-path prefix for ALL disk reads (CSV + fingerprint).
- Robust chunking: auto-fallback to engine='python' (and drop low_memory there).
"""

import os, re, sys, argparse, hashlib, traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterator

import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, MofNCompleteColumn, TimeElapsedColumn,
    TimeRemainingColumn, TextColumn, SpinnerColumn
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

# ========================= Defaults =========================
DEFAULT_IN_DIR    = r"D:\BANA 650 Projects - HealthCare-Analytics\raw_data"
DEFAULT_SCHEMA    = "pricing"
DEFAULT_TABLE     = "charges"
DEFAULT_PATTERN   = "*.csv"
DEFAULT_ROW_CHUNK = 5000
DEFAULT_VAR_CHUNK = 24
DEFAULT_BATCH_SIZE= 5000

UNIQUE_ID_WIDTH = 6
ATTRIBUTES = {
    "negotiated_dollar",
    "negotiated_percentage",
    "negotiated_algorithm",
    "methodology",
    "estimated_amount",
    "additional_payer_notes",
}
METADATA_FRONT_ORDER = [
    "hospital_name",
    "last_updated_on",
    "version",
    "hospital_location",
    "hospital_address",
    "license_number | FL",
    "To the best of its knowledge and belief, the hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date indicated",
]
SPLIT_RE       = re.compile(r"\s*\|\s*")
_CODE_TYPE_RE  = re.compile(r'^\s*code\s*[-_|\s]*([0-9]+)\s*[-_|\s]*type\s*$', re.I)
_CODE_RE       = re.compile(r'^\s*code\s*[-_|\s]*([0-9]+)\s*$', re.I)
PG_IDENT_LIMIT = 63
ALNUM36        = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
console        = Console()

# ========================= Long-path helper =========================
def to_long_path(p: Path) -> str:
    s = str(p)
    if os.name == "nt":
        if s.startswith("\\\\?\\") or s.startswith("\\\\?\\UNC\\"):
            return s
        ab = str(p.resolve())
        if ab.startswith("\\\\"):
            return "\\\\?\\UNC" + ab[1:]
        return "\\\\?\\" + ab
    return s

# ========================= CSV readers =========================
def read_csv_robust(path: Path, *, skiprows: int = 0,
                    encoding: Optional[str] = None,
                    encoding_errors: Optional[str] = None,
                    nrows: Optional[int] = None,
                    header: Optional[int] = "infer",
                    chunksize: Optional[int] = None,
                    force_python_engine: bool = False):
    """
    Open CSV as DataFrame or iterator, trying multiple encodings.
    If force_python_engine=True, we explicitly set engine='python' and DROP low_memory.
    """
    path_str = to_long_path(path)

    def _try(enc: str, errors: Optional[str], engine: Optional[str] = None):
        # Base kwargs
        kwargs = dict(
            dtype=str,
            keep_default_na=False,
            na_values=[""],
            skiprows=skiprows,
            header=header
        )
        # Pandas C-engine accepts low_memory; Python engine does not.
        if engine is None:
            kwargs["low_memory"] = False
        # Optional knobs
        if nrows is not None: kwargs["nrows"] = nrows
        if chunksize is not None: kwargs["chunksize"] = chunksize
        if errors: kwargs["encoding_errors"] = errors
        if engine: kwargs["engine"] = engine
        return pd.read_csv(path_str, encoding=enc, **kwargs)

    # Order of engines to try
    engines = ["python"] if force_python_engine else [None, "python"]

    # If encoding provided, try it first across engines
    if encoding:
        for eng in engines:
            try:
                return _try(encoding, encoding_errors, engine=eng)
            except (UnicodeDecodeError, OSError, ValueError, pd.errors.ParserError, Exception):
                continue

    # Otherwise try common encodings
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        for eng in engines:
            try:
                return _try(enc, None, engine=eng)
            except UnicodeDecodeError:
                break
            except (OSError, ValueError, pd.errors.ParserError, Exception):
                continue

    # Last resort
    return _try("latin1", "replace", engine="python")

def iter_csv_chunks_with_fallback(path: Path, *, skiprows: int, encoding: Optional[str],
                                  encoding_errors: Optional[str], row_chunk: int) -> Iterator[pd.DataFrame]:
    """
    Yield chunks; if iteration raises errors on C-engine, restart with engine='python'
    (and without low_memory).
    """
    try:
        iterator = read_csv_robust(path, skiprows=skiprows, encoding=encoding,
                                   encoding_errors=encoding_errors, chunksize=row_chunk, header="infer")
        for chunk in iterator:
            yield chunk
        return
    except (OSError, ValueError, pd.errors.ParserError, UnicodeDecodeError) as e:
        console.print(f"[yellow][WARN][/yellow] Chunking fallback to Python engine for {path.name}: {e}")

    iterator = read_csv_robust(path, skiprows=skiprows, encoding=encoding,
                               encoding_errors=encoding_errors, chunksize=row_chunk,
                               header="infer", force_python_engine=True)
    for chunk in iterator:
        yield chunk

# ========================= Header normalization =========================
def normalize_header(c: str, *, drop_standard_charge_prefix: bool = True) -> str:
    s_orig = str(c).strip()
    m = _CODE_TYPE_RE.match(s_orig)
    if m: return f"code | {m.group(1)} | type"
    m = _CODE_RE.match(s_orig)
    if m: return f"code | {m.group(1)}"
    if "|" in s_orig:
        parts = [seg.strip() for seg in s_orig.split("|")]
        parts = [p for p in parts if p != ""]
        if drop_standard_charge_prefix and parts and parts[0].lower() == "standard_charge":
            parts = parts[1:]
        s_norm = " | ".join(parts)
    else:
        s_norm = s_orig
    m = _CODE_TYPE_RE.match(s_norm)
    if m: return f"code | {m.group(1)} | type"
    m = _CODE_RE.match(s_norm)
    if m: return f"code | {m.group(1)}"
    return s_norm

def parse_var_column(col: str) -> Optional[Tuple[str, str, str]]:
    parts = SPLIT_RE.split(str(col))
    if not parts: return None
    head = parts[0]
    if head in ("estimated_amount","additional_payer_notes") and len(parts) >= 3:
        payer, plan = parts[1], parts[2]; return payer, plan, head
    if head == "standard_charge" and len(parts) >= 4:
        payer, plan = parts[1], parts[2]; attr = " | ".join(parts[3:])
        return (payer, plan, attr) if attr in ATTRIBUTES else None
    if len(parts) >= 3:
        payer, plan = parts[0], parts[1]; attr = " | ".join(parts[2:])
        return (payer, plan, attr) if attr in ATTRIBUTES else None
    return None

# ========================= Metadata extraction =========================
def extract_metadata_from_top_two_rows(path: Path, *,
                                       encoding: Optional[str],
                                       encoding_errors: Optional[str]) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    top2 = read_csv_robust(path, skiprows=0, encoding=encoding,
                           encoding_errors=encoding_errors, nrows=2, header=None)
    if isinstance(top2, pd.DataFrame) and not top2.empty:
        for j in range(top2.shape[1]):
            key_raw = top2.iat[0, j] if j < top2.shape[1] else None
            val_raw = top2.iat[1, j] if j < top2.shape[1] else None
            key = normalize_header(key_raw, drop_standard_charge_prefix=False) if key_raw is not None else ""
            if key in METADATA_FRONT_ORDER and val_raw not in (None, ""):
                meta[key] = str(val_raw)
        needed = {k for k in METADATA_FRONT_ORDER if k not in meta}
        if needed:
            for i in range(min(2, top2.shape[0])):
                for j in range(top2.shape[1]):
                    cell = top2.iat[i, j]
                    key = normalize_header(cell, drop_standard_charge_prefix=False) if cell is not None else ""
                    if key in needed:
                        for k in range(j + 1, top2.shape[1]):
                            val = top2.iat[i, k]
                            if val not in (None, ""):
                                meta[key] = str(val); break
                needed = {k for k in METADATA_FRONT_ORDER if k not in meta}
                if not needed: break
    return meta

# ========================= Wide→Tall =========================
def wide_to_tall_keep_base(df: pd.DataFrame, var_chunk: int) -> Tuple[pd.DataFrame, List[str]]:
    df = df.copy()
    df.columns = [normalize_header(c, drop_standard_charge_prefix=True) for c in df.columns]
    if "row_id" not in df.columns:
        df.insert(0, "row_id", range(1, len(df) + 1))

    var_defs: List[Tuple[str, str, str, str]] = []
    for c in df.columns:
        parsed = parse_var_column(c)
        if parsed is not None:
            payer, plan, attr = parsed
            var_defs.append((c, payer, plan, attr))

    if not var_defs:
        return df, [c for c in df.columns if c != "row_id"]

    var_cols = {col for col, _, _, _ in var_defs}
    base_cols = [c for c in df.columns if c not in var_cols and c != "row_id"]

    rid = df["row_id"].values
    long_parts: List[pd.DataFrame] = []
    long_accum: Optional[pd.DataFrame] = None

    def flush_parts():
        nonlocal long_accum, long_parts
        if not long_parts:
            return
        chunk = pd.concat(long_parts, ignore_index=True, copy=False)
        long_parts = []
        if long_accum is None:
            long_accum = chunk
        else:
            long_accum = pd.concat([long_accum, chunk], ignore_index=True, copy=False)

    for i, (col, payer, plan, attr) in enumerate(var_defs, 1):
        long_parts.append(pd.DataFrame({
            "row_id": rid,
            "payer_name": payer,
            "plan_name": plan,
            "attribute": attr,
            "value": df[col].values,
        }))
        if i % var_chunk == 0:
            flush_parts()
    flush_parts()

    long = long_accum if long_accum is not None else pd.DataFrame(
        columns=["row_id","payer_name","plan_name","attribute","value"]
    )

    tall = long.pivot_table(
        index=["row_id", "payer_name", "plan_name"],
        columns="attribute",
        values="value",
        aggfunc="first",
    ).reset_index()

    base_df = df[["row_id"] + base_cols]
    tall = tall.merge(base_df, on="row_id", how="left")
    return tall, base_cols

# ========================= IDs & keys =========================
def base36_pad(n: int, width: int = UNIQUE_ID_WIDTH) -> str:
    if n < 0: raise ValueError("n must be ≥ 0")
    max_n = 36 ** width
    if n >= max_n: raise ValueError(f"Row count {n+1} exceeds 36^{width}={max_n}. Increase UNIQUE_ID_WIDTH.")
    if n == 0: return "0".rjust(width, "0")
    out = ""; x = n
    while x:
        x, r = divmod(x, 36)
        out = ALNUM36[r] + out
    return out.rjust(width, "0")

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def build_natural_key(df: pd.DataFrame) -> pd.Series:
    key_cols = [
        "hospital_name", "last_updated_on",
        "payer_name", "plan_name",
        "description", "billing_class", "setting",
        "modifiers", "drug_unit_of_measurement", "drug_type_of_measurement",
        "source_file"
    ]
    code_cols = [c for c in df.columns if str(c).lower().startswith("code")]
    key_cols.extend(code_cols)
    key_cols = [c for c in key_cols if c in df.columns]

    def norm(v):
        if pd.isna(v): return ""
        return str(v).strip().lower()

    def row_to_key(r):
        parts = [norm(r.get(c, "")) for c in key_cols]
        return "||".join(parts)

    return df.apply(row_to_key, axis=1).map(sha256_hex)

# ========================= DB helpers =========================
REQUIRED_COLS = [
    "CCN","unique_id","hospital_name","last_updated_on","version","hospital_location","hospital_address",
    "license_number | FL",
    "To the best of its knowledge and belief, the hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date indicated",
    "payer_name","plan_name","estimated_amount","negotiated_algorithm","negotiated_percentage",
    "additional_payer_notes","negotiated_dollar","methodology","source_file","description",
    "code | 1","code | 1 | type","code | 2","code | 2 | type","code | 3","code | 3 | type",
    "code | 4","code | 4 | type","code | 5","code | 5 | type","code | 6","code | 6 | type",
    "billing_class","setting","drug_unit_of_measurement","drug_type_of_measurement","modifiers",
    "gross","discounted_cash","min","max","additional_generic_notes",
    "natural_key"
]

def pg_ident_truncate(name: str, limit: int = PG_IDENT_LIMIT) -> str:
    return name[:limit]

def make_engine() -> Engine:
    load_dotenv()
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db   = os.getenv("PGDATABASE", "florida_healthcare")
    user = os.getenv("PGUSER", "postgres")
    pw   = os.getenv("PGPASSWORD", "")
    url = URL.create("postgresql+psycopg2", username=user, password=pw, host=host, port=port, database=db)
    return create_engine(url, pool_pre_ping=True)

def get_existing_columns(engine: Engine, schema: str, table: str) -> List[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema=:schema AND table_name=:table
    ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        return list(conn.execute(text(sql), {"schema": schema, "table": table}).scalars().all())

def ensure_table(engine: Engine, schema: str, table: str, cols: List[str]):
    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        col_defs = []
        for c in cols:
            db_c = pg_ident_truncate(c)
            col_defs.append(f'"{db_c}" TEXT')
        conn.exec_driver_sql(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({", ".join(col_defs)});')
        conn.exec_driver_sql(f'''
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = '{schema}' AND indexname = 'ux_{table}_natural_key'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX ux_{table}_natural_key ON "{schema}"."{table}" ("natural_key")';
            END IF;
        END$$;''')

def add_missing_columns(engine: Engine, schema: str, table: str, want_cols: List[str]):
    have = set(get_existing_columns(engine, schema, table))
    with engine.begin() as conn:
        for logical in want_cols:
            db_name = pg_ident_truncate(logical)
            if db_name not in have:
                conn.exec_driver_sql(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{db_name}" TEXT;')
                have.add(db_name)

def df_to_db_colmap(engine: Engine, schema: str, table: str, df_cols: List[str]) -> Dict[str, str]:
    have = set(get_existing_columns(engine, schema, table))
    mapping: Dict[str, str] = {}
    for c in df_cols:
        if c in have:
            mapping[c] = c
        else:
            t = pg_ident_truncate(c)
            mapping[c] = t if t in have else c
    return mapping

def insert_batches(engine: Engine, schema: str, table: str, df: pd.DataFrame, colmap: Dict[str, str], batch_size: int) -> int:
    from psycopg2.extras import execute_values  # type: ignore
    db_cols = [colmap[c] for c in df.columns]
    total = len(df); inserted = 0
    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur, Progress(
            TextColumn("[bold green]DB Insert[/]"), BarColumn(), MofNCompleteColumn(),
            TextColumn("•"), TimeElapsedColumn(), TextColumn("ETA"), TimeRemainingColumn(),
            console=console, transient=False,
        ) as progress:
            task = progress.add_task("insert", total=total)
            quoted_cols = ", ".join(f'"{c}"' for c in db_cols)
            for start in range(0, total, batch_size):
                batch = df.iloc[start:start+batch_size].copy()
                batch = batch.astype(object).where(pd.notna(batch), None)
                values = [tuple(batch.iloc[i].tolist()) for i in range(len(batch))]
                sql = f'INSERT INTO "{schema}"."{table}" ({quoted_cols}) VALUES %s ON CONFLICT ("natural_key") DO NOTHING RETURNING 1;'
                execute_values(cur, sql, values)
                ins_rows = cur.fetchall() if cur.description else []
                inserted += len(ins_rows)
                progress.update(task, advance=len(batch))
    return inserted

# ========================= Ingestion log (fingerprint) =========================
def ensure_ingestion_log(engine: Engine, schema: str = "pricing"):
    sql = f'''
    CREATE SCHEMA IF NOT EXISTS "{schema}";
    CREATE TABLE IF NOT EXISTS "{schema}"."ingestion_log" (
        source_file TEXT PRIMARY KEY,
        file_size BIGINT,
        file_mtime TIMESTAMPTZ,
        file_hash TEXT,
        inserted_rows BIGINT,
        processed_at TIMESTAMPTZ DEFAULT NOW()
    );
    '''
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def file_fingerprint(path: Path, hash_chunk: int = 1024*1024) -> Tuple[int, float, str]:
    """
    Long-path safe fingerprint. If hashing fails, fall back to size+mtime signature.
    """
    path_str = to_long_path(path)
    stat = os.stat(path_str)
    size = stat.st_size
    mtime = stat.st_mtime
    h = hashlib.sha256()
    try:
        with open(path_str, "rb") as f:
            while True:
                b = f.read(hash_chunk)
                if not b: break
                h.update(b)
        return size, mtime, h.hexdigest()
    except OSError:
        fallback = f"{size}|{mtime}|{os.path.basename(path_str)}"
        return size, mtime, hashlib.sha256(fallback.encode("utf-8")).hexdigest()

def is_already_processed(engine: Engine, schema: str, source_file: str,
                         size: int, mtime: float, file_hash: str) -> bool:
    sql = f'SELECT file_hash FROM "{schema}"."ingestion_log" WHERE source_file=:sf'
    with engine.connect() as conn:
        prev = conn.execute(text(sql), {"sf": source_file}).scalar()
    return prev == file_hash

def record_ingestion(engine: Engine, schema: str, source_file: str,
                     size: int, mtime: float, file_hash: str, inserted_rows: int):
    sql = f'''
    INSERT INTO "{schema}"."ingestion_log"
        (source_file, file_size, file_mtime, file_hash, inserted_rows)
    VALUES (:sf, :sz, to_timestamp(:mt), :fh, :ins)
    ON CONFLICT (source_file) DO UPDATE
      SET file_size = EXCLUDED.file_size,
          file_mtime = EXCLUDED.file_mtime,
          file_hash  = EXCLUDED.file_hash,
          inserted_rows = EXCLUDED.inserted_rows,
          processed_at = NOW();
    '''
    with engine.begin() as conn:
        conn.execute(text(sql), {"sf": source_file, "sz": size, "mt": mtime, "fh": file_hash, "ins": inserted_rows})

# ========================= Per-file processing =========================
def process_one_file_streaming(path: Path, *,
                               skip_rows: int,
                               encoding: Optional[str],
                               encoding_errors: Optional[str],
                               row_chunk: int,
                               var_chunk: int,
                               batch_size: int,
                               engine: Engine,
                               schema: str,
                               table: str) -> int:
    console.print(f"[cyan][INFO][/cyan] Processing: {path.name}")
    meta = extract_metadata_from_top_two_rows(path, encoding=encoding, encoding_errors=encoding_errors)

    # Probe header (best-effort)
    try:
        _ = read_csv_robust(path, skiprows=skip_rows, encoding=encoding,
                            encoding_errors=encoding_errors, nrows=0, header="infer")
    except Exception as e:
        console.print(f"[yellow][WARN][/yellow] Header probe failed for {path.name}: {e}")

    unique_counter = 0
    total_inserted = 0

    with Progress(SpinnerColumn(), TextColumn("[bold blue]Transform chunks[/]"),
                  BarColumn(), MofNCompleteColumn(),
                  TextColumn("•"), TimeElapsedColumn(),
                  console=console, transient=False) as prog:
        task = prog.add_task("chunks", total=None)

        try:
            for chunk in iter_csv_chunks_with_fallback(path, skiprows=skip_rows,
                                                       encoding=encoding, encoding_errors=encoding_errors,
                                                       row_chunk=row_chunk):
                try:
                    chunk = chunk.copy()
                    chunk.columns = [normalize_header(c, drop_standard_charge_prefix=True) for c in chunk.columns]
                    if "row_id" not in chunk.columns:
                        chunk.insert(0, "row_id", range(1, len(chunk) + 1))

                    tall, _ = wide_to_tall_keep_base(chunk, var_chunk=var_chunk)

                    # Metadata + source
                    for col in METADATA_FRONT_ORDER:
                        tall[col] = meta.get(col, pd.NA)
                    tall["source_file"] = path.name

                    # unique_id (trace only)
                    need = len(tall)
                    if need > 0:
                        start = unique_counter
                        ids = [f"{base36_pad(start + i)}" for i in range(need)]
                        tall.insert(0, "unique_id", ids)
                        unique_counter += need

                    # CCN (present but not used for dedupe)
                    if "CCN" not in tall.columns:
                        tall.insert(0, "CCN", pd.NA)

                    # Drop helper
                    if "row_id" in tall.columns:
                        tall = tall.drop(columns=["row_id"])

                    # Column order
                    attr_cols_present = [a for a in ATTRIBUTES if a in tall.columns]
                    meta_front = [c for c in METADATA_FRONT_ORDER if c in tall.columns]
                    front = ["CCN", "unique_id"] + meta_front + ["payer_name", "plan_name"] + attr_cols_present + ["source_file"]
                    remaining = [c for c in tall.columns if c not in front]
                    tall = tall[front + remaining]

                    # Stable natural_key
                    if "natural_key" in tall.columns:
                        tall = tall.drop(columns=["natural_key"])
                    tall["natural_key"] = build_natural_key(tall)

                    # In-chunk dedupe
                    before = len(tall)
                    tall = tall.drop_duplicates(subset=["natural_key"], keep="first").reset_index(drop=True)
                    dropped = before - len(tall)
                    if dropped > 0:
                        console.print(f"[yellow][INFO][/yellow] Dropped {dropped} duplicate row(s) in chunk before DB load.")

                    # Ensure DB cols + map
                    add_missing_columns(engine, schema, table, list(tall.columns))
                    colmap = df_to_db_colmap(engine, schema, table, list(tall.columns))

                    # Insert
                    inserted = insert_batches(engine, schema, table, tall.loc[:, list(colmap.keys())], colmap, batch_size=batch_size)
                    total_inserted += inserted

                except Exception as e:
                    console.print(f"[yellow][WARN][/yellow] Chunk in {path.name} failed: {e}")
                    console.print(traceback.format_exc())

                prog.update(task, advance=1)

        except Exception as e:
            console.print(f"[yellow][WARN][/yellow] Failed iterating chunks for {path.name}: {e}")
            console.print(traceback.format_exc())

    console.print(f"[green]Inserted {total_inserted} new row(s) from {path.name}.[/green]")
    return total_inserted

# ========================= CLI / Main =========================
def collect_files(in_dir: Path, pattern: str, recursive: bool) -> List[Path]:
    return sorted(p for p in (in_dir.rglob(pattern) if recursive else in_dir.glob(pattern)) if p.is_file())

def main():
    ap = argparse.ArgumentParser(description="Folder→(one-at-a-time) Wide→Tall to Postgres with chunking, dedupe, progress.")
    ap.add_argument("--in", dest="in_dir", default=DEFAULT_IN_DIR, help="Folder to scan for CSVs.")
    ap.add_argument("--pattern", default=DEFAULT_PATTERN, help="Glob pattern for CSVs.")
    ap.add_argument("--recursive", action="store_true", help="Recurse subfolders.")
    ap.add_argument("--skip-rows", dest="skip_rows", type=int, default=2)
    ap.add_argument("--encoding", dest="encoding", default=None)
    ap.add_argument("--encoding-errors", dest="encoding_errors", default=None)
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table",  default=DEFAULT_TABLE)
    ap.add_argument("--row-chunk", dest="row_chunk", type=int, default=DEFAULT_ROW_CHUNK)
    ap.add_argument("--var-chunk", dest="var_chunk", type=int, default=DEFAULT_VAR_CHUNK)
    ap.add_argument("--batch-size", dest="batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    ap.add_argument("--force", action="store_true", help="Force re-process files even if fingerprint matches previous run.")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    if not in_dir.exists():
        raise SystemExit(f"[ERROR] Input folder not found: {in_dir}")

    files = collect_files(in_dir, args.pattern, args.recursive)
    if not files:
        raise SystemExit("[ERROR] No matching files. Check --in / --pattern / --recursive.")

    engine = make_engine()
    ensure_table(engine, args.schema, args.table, REQUIRED_COLS)
    ensure_ingestion_log(engine, args.schema)

    total = 0
    console.print(f"[magenta][INFO][/magenta] Found {len(files)} file(s). Processing sequentially…")
    with Progress(TextColumn("[bold blue]Files[/]"), BarColumn(), MofNCompleteColumn(),
                  TextColumn("•"), TimeElapsedColumn(), console=console, transient=False) as p:
        t = p.add_task("files", total=len(files))
        for path in files:
            try:
                size, mtime, fh = file_fingerprint(path)
                if not args.force and is_already_processed(engine, args.schema, path.name, size, mtime, fh):
                    console.print(f"[green][SKIP][/green] {path.name} (unchanged content)")
                    p.update(t, advance=1)
                    continue

                inserted_now = process_one_file_streaming(
                    path,
                    skip_rows=args.skip_rows,
                    encoding=args.encoding,
                    encoding_errors=args.encoding_errors,
                    row_chunk=args.row_chunk,
                    var_chunk=args.var_chunk,
                    batch_size=args.batch_size,
                    engine=engine, schema=args.schema, table=args.table
                )
                record_ingestion(engine, args.schema, path.name, size, mtime, fh, inserted_now)
                total += inserted_now

            except Exception as e:
                path_str = to_long_path(path)
                console.print(f"[yellow][WARN][/yellow] Skipped {path.name} (len={len(path_str)}): {e}")
                console.print(traceback.format_exc())
            finally:
                p.update(t, advance=1)

    console.print(f"[bold green][DONE][/bold green] Inserted {total} row(s) total into {args.schema}.{args.table}.")

if __name__ == "__main__":
    main()
