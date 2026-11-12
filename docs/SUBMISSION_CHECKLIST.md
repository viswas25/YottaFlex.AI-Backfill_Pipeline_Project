# Submission checklist (from the Guidelines + FAQ)

- [ ] 72-hour window: note the time the project was sent; submit before the deadline (repo stays editable until then).
- [ ] Create a **new, dedicated, public** GitHub repository; send the link from the **same email** used in recruitment.
- [ ] Commit in meaningful steps (history should show evolution). Suggested sequence:
  1. `docs: README skeleton, data dictionary, assumptions`
  2. `feat: schemas + validation rules` (`schemas.py`, `validate.py`)
  3. `feat: partition parser/worker with reject path`
  4. `feat: DB schema + idempotent partition loader`
  5. `feat: orchestrator - dry-run, bounded pool, retry, restart`
  6. `feat: synthetic data generator with separate labels`
  7. `test: unit + integration tests`
  8. `feat: reconciliation + sample queries`
  9. `perf: benchmark + findings`
  10. `docs: design note, AI disclosure, prompt history, evidence reports`
- [ ] Run from a **clean environment** exactly as the README says (venv -> `pip install -r requirements.txt` -> `pytest` -> demo).
- [ ] Re-run `bash scripts/run_demo.sh` and `python scripts/benchmark.py` on YOUR machine; commit the refreshed `reports/`.
- [ ] Complete `docs/ai_usage/AI_USAGE_DISCLOSURE.md` truthfully; add all prompts to `PROMPT_HISTORY.md`.
- [ ] No secrets / personal data (none are used: data is synthetic, no credentials).
- [ ] You must be able to walk through every module: use `docs/WALKTHROUGH.md`. The work must be understood and owned by you.
