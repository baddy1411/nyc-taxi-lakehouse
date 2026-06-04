# Makefile — NYC Taxi Lakehouse
# Run `make help` to see all targets.

.DEFAULT_GOAL := help
PYTHON        := python3
PIP           := pip3
VENV          := .venv
DBT_DIR       := models

# ── Help ─────────────────────────────────────────────────────────
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Setup ────────────────────────────────────────────────────────
.PHONY: install
install: ## Install all dependencies
	$(PIP) install -r requirements.txt

.PHONY: venv
venv: ## Create virtualenv
	$(PYTHON) -m venv $(VENV)
	@echo "Activate: source $(VENV)/bin/activate"

# ── Data ─────────────────────────────────────────────────────────
.PHONY: data-generate
data-generate: ## Generate synthetic 500K-row dataset (for dev/CI)
	$(PYTHON) scripts/generate_test_data.py --rows 500000

.PHONY: data-download
data-download: ## Download real TLC data (requires internet, ~500MB)
	$(PYTHON) ingestion/extractors/tlc_extractor.py --year 2024 --month 01

# ── Pipeline ─────────────────────────────────────────────────────
.PHONY: pipeline
pipeline: ## Run full Bronze → Silver → Gold pipeline
	$(PYTHON) run_pipeline.py

.PHONY: pipeline-silver
pipeline-silver: ## Run Silver stage only
	$(PYTHON) run_pipeline.py --stage silver

.PHONY: pipeline-gold
pipeline-gold: ## Run Gold stage only
	$(PYTHON) run_pipeline.py --stage gold

# ── Tests ────────────────────────────────────────────────────────
.PHONY: test
test: ## Run full test suite
	pytest tests/ -v

.PHONY: test-unit
test-unit: ## Run unit tests only (fast, <1s)
	pytest tests/unit/ -v

.PHONY: test-integration
test-integration: ## Run integration tests (requires data)
	pytest tests/integration/ -v

.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	pytest tests/ --cov=pipeline --cov=ingestion --cov-report=html --cov-report=term

# ── Quality ──────────────────────────────────────────────────────
.PHONY: lint
lint: ## Run ruff linter
	ruff check .

.PHONY: format
format: ## Auto-format with ruff
	ruff format .

.PHONY: typecheck
typecheck: ## Run mypy type checker
	mypy pipeline/ ingestion/ --ignore-missing-imports

.PHONY: ge-check
ge-check: ## Run Great Expectations data contract
	$(PYTHON) monitoring/contracts/silver_contract.py --path data/lakehouse/silver

.PHONY: sla-check
sla-check: ## Run SLA monitor against Gold layer
	$(PYTHON) monitoring/sla/sla_monitor.py

# ── dbt ──────────────────────────────────────────────────────────
.PHONY: dbt-run
dbt-run: ## Run dbt models
	cd $(DBT_DIR) && dbt run --profiles-dir .

.PHONY: dbt-test
dbt-test: ## Run dbt schema tests
	cd $(DBT_DIR) && dbt test --profiles-dir .

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs
	cd $(DBT_DIR) && dbt docs generate --profiles-dir . && dbt docs serve

# ── Dashboard ────────────────────────────────────────────────────
.PHONY: dashboard
dashboard: ## Launch Streamlit dashboard
	streamlit run dashboard/app.py

# ── Full dev workflow ─────────────────────────────────────────────
.PHONY: dev
dev: data-generate pipeline test ge-check ## Full dev workflow from scratch
	@echo "✅ Dev environment ready"

.PHONY: ci
ci: data-generate test ge-check ## CI workflow (no dbt)
	@echo "✅ CI checks passed"

# ── Clean ─────────────────────────────────────────────────────────
.PHONY: clean
clean: ## Remove generated data and caches
	rm -rf data/lakehouse/ data/raw/*.parquet
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache htmlcov .coverage coverage.xml
