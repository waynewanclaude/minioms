# minioms — Claude Code Session Notes

## Project Overview

**minioms** is a mini Order Management System (OMS). It manages portfolio positions,
executions, dividend transactions, daily orders, and account reconciliation. Data is
stored as CSV files accessed through an `oms_db` abstraction layer.

---

## Package Structure

```
minioms/
  oms_db/         # CSV I/O abstraction layer (DataFile base class + *_IO subclasses)
  obj/            # Per-object io_utility / br_utility / io_utility wrappers
  util/           # Operational logic and interface files
    external_interface.py   # Market price loader + shared load_market_price*
    helper_debug.py         # Debug utilities (print_oms_io_objects)
    helper_export_to_gspread.py
    helper_report.py
    if_build_daily_orders.py
    if_post_div.py
    if_post_process.py
    op_alloc_div.py
    op_alloc_exec.py
    op_exec_match.py
    op_gen_account_orders.py
    op_gen_portf_orders.py
    op_merge_div_staging.py
```

---

## Comment Conventions (established in this project)

| Tag | Meaning |
|---|---|
| `(CLU) NEED_REVIEW` | Claude's review marker — includes notes and suggested fix |
| `(HUM) pending_rm` | Human marked for removal (not yet tested) |
| `(HUM) REVIEWED;pending_rm` | Human reviewed, approved for removal once tested |
| `(HUM) YOU_DO_IT` | Human delegating a specific code change to Claude |
| `(HUM) RESPONSE` | Human's reply to a CLU NEED_REVIEW comment |
| `(HUM) TODO` | Human's own notes for future work |
| `# -- ref externally --` | Symbol/function is called from outside the util directory |
| `# -- is_API: TRUE --` | Function is part of the external-facing API |

---

## Key Architectural Decisions

### oms_db Abstraction Layer
- All CSV I/O must go through `oms_db.datafile.DataFile` or its subclasses (`*_IO`)
- Direct `pd.read_csv` / `to_csv` in `util/` is considered "illegal" unless justified
- Exception: `helper_export_to_gspread.py` merge utilities operate on raw files as a
  deliberate design choice (documented with NEED_REVIEW and rationale comments)

### `external_interface.py`
- Single source of truth for external service access
- `mktprc_loader()` — returns the market price loader (must be set via `set_mktprc_loader`)
- `load_market_price_impl(req_symbols, cached_data={})` — fetches prices, caches results
- `load_market_price(somepos, cache={}, clear_cache=False)` — joins prices onto a DataFrame
- Previously duplicated in `op_gen_portf_orders.py` and `helper_report.py` — now shared here

### Timestamp Check Design (helper_export_to_gspread.py)
- `with_chk` variants retired (marked `REVIEWED;pending_rm`); `no_chk` variants kept
- Future timestamp check design (if needed): use local OS filesystem mtime as source of
  truth; write to file first; Google Sheets update is secondary and only if file write
  happened; gsheet timestamp = file save time (not gsheet update time)
- `merged_csv_files_save_db_no_chk`: destination must contain `_export_` (enforced by
  runtime guard) — `_export_` is a temp reporting folder, not part of the oms_db

### display() → print()
- `display()` is a Jupyter built-in not available outside notebooks
- Replacement pattern: `print(df.to_string())` for DataFrames, `print(df.tail().to_string())`
  for tailed views, `print(obj)` for plain values
- `print_oms_io_objects(obj)` in `helper_debug.py`: handles single DataFile instances
  (prints `full_path` + `df`) and dicts of DataFile objects (from `load_bulk`)

### `.copy()` Convention
- `.copy()` is used only when the function being called mutates the input
- Do NOT add `.copy()` for "pattern consistency" — use it only when needed

---

## Work Completed (dev52 → dev66+clu)

### Scaffolding / Dead Code Removal
- Removed all `pending_rm` commented-out blocks across util files (dev52)
- Removed `read_db_path` / `db_path` scaffolding; replaced with `AcctPositionReport`
  `io_utility` / `oms_db` abstraction (dev53–dev54)

### Unused Import Cleanup
- `op_gen_portf_orders.py`: removed 9 unused imports (dev55)
- `helper_export_to_gspread.py`: removed pickle, time, copy, defaultdict, exec_u_io,
  dt_to_str, Path, sys, gspread, `__p__` (dev55–dev56)
- `helper_report.py`: removed pprint, retry (dev56–dev58)
- `op_gen_account_orders.py`: removed 10 unused imports (dev65)
- `op_exec_match.py`: removed 9 unused imports (dev66)

### Inlining / Extraction
- Inlined `__load_open_positions__bk_exp_gsp` into `load_open_positions` (dev59)
- Inlined `__load_portf_div_txns__bk_exp_gsp` into `load_dividend` (dev60)
- Extracted `load_market_price_impl` / `load_market_price` from both
  `op_gen_portf_orders.py` and `helper_report.py` into `external_interface.py` (dev60)

### Bug Fixes / Correctness
- Fixed `parse_options` bug in `helper_report.py`: `sys.argv[n]` → `argv[n]` (dev58)
- Fixed positional args in `load_required_objects` (`op_gen_portf_orders.py`) → keyword
  args consistent with rest of file (dev60)
- Fixed bare `except:` → `except Exception:` in `if_post_process.py` (dev63)
- Applied `rename(errors='raise')` in `op_exec_match.py` replacing fragile
  `df.columns = [...]` in-place reassignment (dev66)

### display() Replacements
- `if_post_process.py`: replaced all `display()` with `print_oms_io_objects()` (dev63)
- `if_build_daily_orders.py`: replaced all `display()` with `print(df.to_string())` (dev64)
- `if_post_div.py`: replaced all `display()` with `print(df.tail().to_string())` (dev64)

### New Files
- `helper_debug.py`: created as home for debug/inspection utilities;
  contains `print_oms_io_objects(obj)` (dev63)

---

## Outstanding NEED_REVIEW Items

### `helper_export_to_gspread.py`
- `merge_csv_files_as_df`: `pd.read_csv` direct — justified (raw file concatenation,
  not oms_db data); user reviewed and accepted the rationale
- `merged_csv_files_save_db_no_chk`: `to_csv` direct — justified (`_export_` temp folder,
  not oms_db); user reviewed and accepted

### `helper_report.py`
- `import sys` / `import os`: `os` is only used by the `REVIEWED;pending_rm`
  scaffolding block; remove alongside that block when it is cleaned up
- `load_tbsys_books`: no callers found in util directory; verify external callers before
  removing (and `books_u_io` import alongside it)

### `op_gen_portf_orders.py`
- `load_required_objects` class method: marked `(HUM) TODO: keep this and investigate`
  by user — purpose unclear, may be incomplete work

---

## Outstanding `pending_rm` Blocks

### `helper_report.py`
- `sys.path.append` scaffolding block (4 lines + `import os`) —
  marked `REVIEWED;pending_rm`; deferred pending testing that removing it doesn't
  affect dependent programs

### `helper_export_to_gspread.py`
- `with_chk` group: `merged_csv_files_save_db`, `__merged_csv_files_save_gspread_impl__`,
  `merged_csv_files_save_gspread`, `merge_csv_files_save` — all `REVIEWED;pending_rm`
- `load_tbsys_books` in `helper_report.py` — `REVIEWED;pending_rm`

---

## Files Reviewed in util/ (this session)

| File | Status |
|---|---|
| `op_gen_portf_orders.py` | Reviewed — cleaned, NEED_REVIEWs added |
| `helper_report.py` | Reviewed — cleaned, pending_rm blocks deferred |
| `helper_export_to_gspread.py` | Reviewed — cleaned, with_chk retired |
| `external_interface.py` | Updated — now hosts shared load_market_price* |
| `helper_debug.py` | New file |
| `if_post_process.py` | Reviewed — display() fixed |
| `if_build_daily_orders.py` | Reviewed — display() fixed |
| `if_post_div.py` | Reviewed — display() fixed, typo fixed |
| `op_gen_account_orders.py` | Reviewed — unused imports removed |
| `op_exec_match.py` | Reviewed — unused imports removed, rename fix applied |

| File | Status |
|---|---|
| `op_alloc_exec.py` | Not yet reviewed |
| `op_alloc_div.py` | Not yet reviewed |
| `op_merge_div_staging.py` | Not yet reviewed |

---

## Version History (CLU sessions)

| Version | Summary |
|---|---|
| dev52+clu | Remove all pending_rm blocks |
| dev53+clu | Add NEED_REVIEW markers, fix ValueException bug |
| dev54+clu | Replace read_db_path scaffolding with io_utility |
| dev55+clu | Remove unused imports (op_gen_portf_orders, helper_export_to_gspread) |
| dev56+clu | User cleanup (principle, debug comments, sys/os, banner) |
| dev57+clu | Clean up helper_export_to_gspread (pending_rm, dead code) |
| dev58+clu | Fix parse_options bug, clean up helper_report review comments |
| dev59+clu | Inline __load_open_positions__bk_exp_gsp |
| dev60+clu | Inline __load_portf_div_txns__bk_exp_gsp, extract load_market_price* to external_interface, fix positional args |
| dev61+clu | Mark illegal CSV I/O in helper_export_to_gspread |
| dev62+clu | Add timestamp design TODO comment |
| dev63+clu | Fix display() in if_post_process, create helper_debug.py |
| dev64+clu | Fix display() in if_build_daily_orders, if_post_div |
| dev65+clu | Remove unused imports from op_gen_account_orders |
| dev66+clu | Clean up op_exec_match (unused imports, rename fix) |
