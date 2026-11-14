---
name: authoring-graphs
description: Use when authoring or testing `@graph` workflows and their docs.
---

# Authoring graphs

Canonical recipe module: `tests/engine/processes/graphs/test_authoring_recipe.py`.
Copy its workflow and collaborators into your own module.
Other `tests/engine/processes/graphs/programs/` fixtures include historical edge cases, not just recommended authoring style.
Guide: `docs/source/topics/workflows/graphs.rst` (`Authoring recipe`, `Validating graphs and inspecting runs`).

## Recipe

* Science lives in a `calcfunction` plus a `WorkChain` placing it; the `@graph` only orchestrates.
* `@task` steps have plain bodies; `task(WorkChainSubclass)` places scientific children.
* `@graph` bodies wire names, literals, and inline expressions only; no science in the root.
* Keep the step placement local to the `while` body; branch on a comparison, bind a status literal, then report.
* One meaningful `@task` summary records value, target, attempts, and status; the root returns that existing node.
* Root returns task outputs, graph inputs, or collector containers; never a new inline scalar.
* Stored graph node keeps flat inputs (`root.inputs.initial`); `outputs['result']` reads the report.

## Testing

* First `assert workflow.build() is None`: catches syntax, wiring, and contract errors without tasks.
* Then select one case, e.g. `uv run pytest tests/engine/processes/graphs/test_authoring_recipe.py -k ascending -x --tb=short`.
* Only then run the full test module or suite.
* A passing `build()` does not catch pytest mistakes or runtime failures; inspect runs via outputs and the stored node.
