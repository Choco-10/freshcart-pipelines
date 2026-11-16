# FreshCart Pipelines

This repository contains the completed Data Engineering assignment:

- FreshCart API-to-Snowflake ELT;
- Mage Iris classification and evaluation;
- RabbitMQ topic routing with ACK/NACK handling;
- FreshCart RabbitMQ orders flowing through the existing ELT path;
- pytest and GitHub Actions CI;
- Part E report evidence and a one-page integration diagram.

## Repository map

| Path | Purpose |
|---|---|
| `elt/freshcart_elt.ipynb` | FreshCart notebook and SQL evidence cells |
| `elt/freshcart_elt/` | Extract, Snowflake load, mock data, and transform SQL |
| `iris_project/` | Mage Iris pipeline, model, and evaluation blocks |
| `stream/` | Part D RabbitMQ dispatcher and driver |
| `bonus/` | FreshCart RabbitMQ publisher and ELT consumer |
| `tests/` | Focused pytest tests |
| `.github/workflows/ci.yml` | Push-based CI workflow |
| `part_e_integration_diagram.html` | Self-contained one-page diagram for the report |
| `EXECUTION_EVIDENCE_GUIDE.md` | Exact execution, evidence, and report instructions |

## Prerequisites

Install or have access to:

1. Python 3.12 (recommended because it matches CI).
2. Git.
3. A Snowflake account with permission to create the FreshCart schemas.
4. RabbitMQ running locally on `localhost:5672`.
5. Docker Desktop, or a native RabbitMQ Server installation.
6. VS Code with the Python and Jupyter extensions for notebook execution.

## Install Python dependencies

From the repository root in PowerShell:

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines"
python -m pip install -r requirements.txt
python -m pytest -q
```

The Mage environment is separate because Mage is not required by the regular
pytest job:

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines"
python -m venv iris_project\venv-mage
iris_project\venv-mage\Scripts\python.exe -m pip install -r iris_project\requirements.txt
```

If `iris_project\venv-mage` already exists, reuse it instead of creating it
again.

## Configure Snowflake

Copy the safe template and edit the values locally:

```powershell
Copy-Item "elt\.env.example" "elt\.env"
notepad "elt\.env"
```

Set at least:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
LANDING_DIR=freshcart_elt/landing
QUARANTINE_MAX_RATE=0.05
```

Never commit `elt\.env`, paste credentials into a notebook, or include them
in screenshots. The existing `.gitignore` excludes environment files,
generated landing data, caches, logs, databases, models, and CSV outputs.

## Run the FreshCart ELT pipeline

The first run creates the Snowflake objects and loads the built-in mock data:

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines\elt\freshcart_elt"
python run_pipeline.py --setup --mock
```

Repeat with mock data:

```powershell
python run_pipeline.py --mock
```

Run against configured APIs instead:

```powershell
python run_pipeline.py
```

Write landing files without connecting to Snowflake:

```powershell
python run_pipeline.py --dry-run --mock
```

Open `elt\freshcart_elt.ipynb` in VS Code, select the same Python
interpreter, and run the notebook cells for query evidence. Snowflake query
cells use this pattern:

```python
df = show("""
SELECT source, COUNT(*) AS bronze_rows
FROM bronze.orders_raw
GROUP BY source
ORDER BY source;
""")
display(df)
```

## Run the Mage Iris pipeline

From the repository root:

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines\iris_project"
.\venv-mage\Scripts\python.exe -m mage_ai.cli.main run `
  iris_project iris_classifier --test --runtime-vars '{"max_depth": 3}'
```

The pipeline order is `load_iris` -> `prepare_data` -> `check_iris` ->
`train_model` -> `save_results`. Run all required depths by changing
`max_depth` to `1`, `2`, `3`, `4`, and `5`. A failed accuracy gate stops
downstream output for that run.

## Run Part D RabbitMQ

Start RabbitMQ locally with Docker:

```powershell
docker run -d --name rabbitmq-local `
  -p 5672:5672 -p 15672:15672 `
  rabbitmq:3-management
```

If the container already exists:

```powershell
docker start rabbitmq-local
Test-NetConnection localhost -Port 5672
```

Open separate PowerShell windows from the repository root:

```powershell
python stream\driver.py Ravi "bangalore.koramangala.*"
python stream\driver.py Arjun "bangalore.*.ride"
python stream\driver.py Control "bangalore.#"
python stream\dispatcher.py
```

Run for at least 60 seconds. Capture the driver binding lines, dispatcher
routing keys, valid `ACK` lines, and the `NACK discarded` output for the
missing-zone message generated every tenth message.

## Run the FreshCart RabbitMQ bonus

This flow uses a separate `freshcart-orders` exchange. It does not use the
Part D `rapido-dispatch` payload or exchange.

Start the consumer in one PowerShell window:

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines"
python bonus\freshcart_rabbitmq_consumer.py
```

Within the 60-second collection window, publish the existing FreshCart mock
orders from a second window:

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines"
python bonus\publish_freshcart_orders.py
```

The consumer validates each FreshCart-shaped message, ACKs valid records,
NACKs malformed records with `requeue=False`, writes
`extract.write_landing("rabbitmq", ...)`, loads Snowflake bronze, and runs the
existing silver, quarantine, reference, and gold SQL. Repeat the same
consumer/publisher run to capture bronze growth and gold idempotency evidence.

## Part E diagram and report

Open [part_e_integration_diagram.html](part_e_integration_diagram.html) in a
browser and take a landscape screenshot or use browser print-to-PDF. It is
designed to fit on one page and shows the repository, CI, FreshCart ELT,
Mage, and RabbitMQ paths.

Use the local
`C:\Users\harshiv\Desktop\data Engineering\EXECUTION_EVIDENCE_GUIDE.md` for
the numbered answers, exact evidence cells, report order, and the ready-to-paste
half-page reflection comparing Colab and VS Code.

## Validation and safe Git checks

```powershell
Set-Location "C:\Users\harshiv\Desktop\data Engineering\freshcart-pipelines"
python -m pytest -q
git status
git diff --check
git diff --cached -- .env
```

The staged `.env` diff must be empty. Do not commit credentials, private keys,
`.env` files, generated landing data, model files, or notebook secrets.
