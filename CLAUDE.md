# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Celus Nibbler is a COUNTER-like data reader and processor library. It parses various COUNTER report formats (4, 5, 5.1) and converts them into standardized `CounterRecord` objects.

## Build & Development Commands

The project uses **uv** as the package manager (migrated from Poetry in v12.3.0).

```bash
# Install dependencies
uv sync --all-extras

# Run tests
uv run pytest -v

# Run tests with coverage
uv run pytest -v --cov=celus_nibbler --cov-report=term

# Run single test file
uv run pytest tests/test_counter5.py -v

# Run single test
uv run pytest tests/test_counter5.py::test_name -v

# Lint and format
uv run ruff check --fix
uv run ruff format

# Run pre-commit hooks
uv run pre-commit run --hook-stage push --all-files

# Build package
uv build
```

## Code Style

- Max line length: 100 characters
- Use Ruff for linting and formatting (replaced black/flake8)
- Pre-commit hooks enforce style on push

## Architecture

### Core Pipeline: "Eat & Poop"

1. **`eat()`** - Main entry point that parses files and returns parser results
   - Located in `src/celus_nibbler/__init__.py` (public API) and `eat_and_poop.py` (implementation)
   - Takes file path, platform name, optional parser name filters (regex-matched), `check_platform`, `use_heuristics`, and `dynamic_parsers`
   - Returns a list of `Poop` objects or `NibblerError`s (one per sheet/section)

2. **`Poop`** class - Container for parsed data
   - Contains `CounterRecord` objects (from celus-nigiri library)
   - Methods: `records()`, `records_basic()`, `records_with_counter()`, `records_with_stats()`, `get_stats()`, `get_months()`; properties `metrics`, `dimensions`, `title_ids`, `item_ids`, `months`

### Parser Architecture

```
src/celus_nibbler/
├── parsers/
│   ├── base.py              # BaseParser, BaseArea, BaseTabularArea, BaseJsonArea
│   ├── dynamic.py           # Dynamic parser generator from definitions
│   ├── counter/             # COUNTER format parsers
│   │   ├── c4.py            # COUNTER 4 (BR, DB, PR, JR, MR reports)
│   │   ├── c5.py            # COUNTER 5 tabular (DR, PR, TR, IR, IR_M1)
│   │   ├── c5json.py        # COUNTER 5 JSON format
│   │   ├── c51.py           # COUNTER 5.1 tabular
│   │   └── c51json.py       # COUNTER 5.1 JSON format
│   └── non_counter/         # Base classes for non-COUNTER (generic, celus_format) parsers
├── definitions/             # Declarative parser configurations (base, counter, generic, celus_format)
├── reader.py                # File readers (CSV/TSV, JSON, XLSX; XLS needs the `xls` extra)
├── aggregator.py            # Record aggregation/validation strategies
├── conditions.py            # Conditions used in definitions
├── coordinates.py           # Cell coordinates and ranges
├── data_headers.py          # Data header handling
├── sources.py               # Data extraction sources
├── validators.py            # Input validation
├── errors.py                # NibblerError hierarchy
└── __main__.py              # `nibbler-eat` CLI
```

### Key Concepts

- **Parsers** are registered via entry points in `pyproject.toml` under `[project.entry-points.nibbler_parsers]`
- **Definitions** allow declarative parser configuration for dynamic parsing
- **Heuristics** auto-detect the correct parser by analyzing file content
- **Aggregators** handle duplicate records and validation (`SameAggregator` sums identical records; others check conflicts, ordering, non-negative values, titles/items; `PippedAggregator` chains them)

### Data Flow

1. File → Reader (CSV/XLSX/JSON) → SheetReader
2. Parser.check_heuristics() → matches parser to content
3. Parser.parse() → extracts records from Areas
4. Aggregator → combines/validates records
5. Returns Poop with CounterRecord objects

## CLI Usage

```bash
nibbler-eat [OPTIONS] [FILES]
```

Key options:
- `-p, --parser NAME`: Limit to specific parser(s)
- `-P, --platform NAME`: Platform name
- `-s, --skip-heuristics`: Don't use heuristics to select the parser
- `-N, --no-output`: Don't print records
- `-d, --debug`, `--profile`
- `-D, --definition FILE`: Load dynamic parser definition
- `-S, --show-summary`: Show per-sheet statistics
- `-c, --counter-like-output`: Output in COUNTER format

## Testing

Test data files are in `tests/data/`: `counter/{4,5,51}`, `dynamic` (JSON definitions + inputs), `non_counter`, and `reader`. Input files typically have an expected-output sibling (`<file>.out`).

When adding a new parser:
1. Create parser class inheriting from `BaseParser`
2. Register in `pyproject.toml` entry points
3. Add test data to `tests/data/`
4. Write tests in `tests/test_*.py`

## Commit Style

Format: `type: message` (e.g., `feature:`, `fix:`, `chore:`, `release:`). Releases are tagged `vX.Y.Z`; update `CHANGELOG.md` when releasing.
