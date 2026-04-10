PYTHON ?= python
PIP ?= $(PYTHON) -m pip
CONFIG ?= configs/nominal.toml

.PHONY: install test release-check run run-nominal run-weather

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test:
	$(PYTHON) -m pytest

release-check:
	$(PYTHON) scripts/release_check.py

run:
	$(PYTHON) -m avn $(CONFIG)

run-nominal:
	$(PYTHON) scripts/run_nominal.py

run-weather:
	$(PYTHON) scripts/run_weather_case.py
