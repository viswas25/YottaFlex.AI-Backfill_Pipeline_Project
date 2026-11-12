PY ?= python3

.PHONY: setup test demo generate-sample generate-large benchmark clean

setup:            ## install test dependency (runtime is stdlib only)
	$(PY) -m pip install -r requirements.txt

test:             ## run the automated test-suite
	$(PY) -m pytest

demo:             ## full end-to-end demonstration + all evidence reports
	bash scripts/run_demo.sh

generate-sample:  ## small reproducible dataset (7 partitions, ~1.8k rows)
	$(PY) -m backfill generate --out data/sample --labels-out data/sample_labels --seed 42 --days 7 --orders-per-day 120

generate-large:   ## ~143k rows, 30 partitions (not committed; written to out/large)
	$(PY) -m backfill generate --out out/large/src --labels-out out/large/labels --seed 42 --days 30 --orders-per-day 1800 \
	    --customers 5000 --products 500 --no-special-partitions

benchmark:        ## performance measurement -> reports/performance.md
	$(PY) scripts/benchmark.py

clean:
	rm -rf out .pytest_cache **/__pycache__
