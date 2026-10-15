# Project coding and delivery rules

These are user requirements, recorded 15 October 2026.

1. Read current primary documentation before coding against a service or library. Record the documents and the behavior relied upon in the sprint handoff.
2. Every code file starts with a concise description of its purpose and responsibilities. Python modules, classes and functions have docstrings. Workflows/SQL use header comments; JSON uses descriptive metadata where appropriate.
3. Include essential inline comments explaining invariants, ordering and non-obvious failure handling. Avoid repetitive comments on obvious statements.
4. Deliver complete NEW and REPLACE files at their repository-relative paths, plus a change manifest. Do not overwrite unrelated work or claim to have reviewed an unseen repository.
5. Put exact installation/configuration/run/verification steps directly in the chat and in the handoff. Future handoffs must build on the current repository state.
6. Report implemented, locally tested, deployed and cloud-validated status separately. Preserve failed-run evidence and do not declare a sprint live-validated without its cloud gates.
7. Keep payloads and secrets out of logs and local persistent files. Offline tests may use in-memory fixtures; no live source response is saved locally.
8. Update `docs/PROJECT_MEMORY.md` at each sprint boundary with decisions, file changes, evidence and remaining gates. Read it and the previous handoff before implementing the next sprint.

Run `python scripts/check_code.py` to check syntax and explanations in the Sprint 1–2 code. This is a minimal convention gate; the failure/replay tests provide the behavioral checks.
