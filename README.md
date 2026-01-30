# Driver Override

## Setting Up the Environment with `uv`

This project uses [`uv`](https://docs.astral.sh/uv/) to manage the Python virtual environment and dependencies.

### Prerequisites

Install `uv` if you haven't already:

**macOS / Linux:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh

# or with Homebrew
brew install uv
```

**Windows (PowerShell):**

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Create and activate the virtual environment

```bash
# Create a venv pinned to the Python version in .python-version (3.13)
uv venv
```

Activate it:

**macOS / Linux:**

```bash
source .venv/bin/activate
```

**Windows (PowerShell):**

```powershell
.venv\Scripts\Activate.ps1
```

**Windows (CMD):**

```cmd
.venv\Scripts\activate.bat
```

### Install dependencies

```bash
# Sync all dependencies from uv.lock / pyproject.toml
uv sync
```

### Add a new package

```bash
# This updates pyproject.toml and uv.lock automatically
uv add <package-name>
```

### Run a script without activating the venv

```bash
uv run python my_script.py
```

### Register the venv as a Jupyter kernel

The `ipykernel` dependency is already included. After `uv sync`, register the kernel so Jupyter / VS Code can find it:

```bash
uv run python -m ipykernel install --user --name driver-override
```

Then select the **driver-override** kernel in your notebook.

## Project Structure

```
Driver_Override/
├── pyproject.toml          # Project metadata and dependencies
├── uv.lock                 # Locked dependency versions
├── .python-version         # Pinned Python version (3.13)
├── .gitignore
├── README.md
├── data/                   # Raw input data (compressed parquet files)
│   ├── 5FT0491_2026-01-27.parquet.gzip.7z
│   └── 5FT0491_2026-01-28.parquet.gzip.7z
├── jupyter_notebooks/      # Analysis notebooks
│   └── basic_stats.ipynb
├── output/                 # Generated results (plots, tables, etc.)
└── temp/                   # Temporary / intermediate files
└── scripts/                   # Python Scripts (tba)
└── main.py                  # Main entry point (tba)
```


