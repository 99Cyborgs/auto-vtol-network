PYTHON ?= python
PIP ?= $(PYTHON) -m pip
SCENARIO ?= weather_closure

.PHONY: install test run dashboard demo release-check

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) -m avn run $(SCENARIO)

dashboard:
	$(PYTHON) -m avn dashboard

demo:
	$(PYTHON) -m avn demo

release-check:
	$(PYTHON) scripts/release_check.py
