# Methodology

## Same task and fresh contexts

- Comparison baseline: the immediately previous final pair in `../workchain-vs-graph-eval-after/`, not its predecessor or pilot.
- `task.md`, `collaborators.py`, `test_acceptance.py`, per-arm task prompts and substantive runner settings are unchanged. Both arms still need to implement their assigned style and write their own tests. The visible shared acceptance suite has 13 cases.
- Each arm is a new OS-level Pi process with its own randomly generated UUID session and separate source/Git workspace. No parent conversation, prior session or previous solution is supplied; all `PI_SESSION*` environment variables are removed. Report folders are excluded from copied source.
- New sessions retain ordinary project instructions and skill discovery. The new graph authoring skill/guide is deliberately available as part of this revision. The graph agent actually reads it first; it is not manually appended as a special hint.
- Source snapshots include tracked working-tree contents and relevant untracked source/tests/docs. This preserves the current user's local examples rather than silently switching to a different HEAD-only baseline. Both arms receive identical source hashes; the dirty status is recorded in run.json. The evaluator changes no application source.
- Installed dependencies are shared, but each workspace's own src and repository paths lead PYTHONPATH. AiiDA configuration paths are separate. This is context/workspace isolation, not a filesystem security sandbox.
- Same explicit provider/model/thinking: openai-codex/gpt-6.1-sol, medium. Same read/bash/edit/write tools and 900-second limit. Neither agent delegates to another model. The complete invocations and logs are retained.

## Measurements

- Solve time is monotonic process launch-to-exit time, including exploration, reasoning, code generation, tool use and agent-run tests. Independent post-agent validation is excluded and timed separately.
- Both agents run concurrently on the same host, as in the previous pair. Independent validation can overlap the other arm as it finishes. No full application suite is run concurrently during this measured pair; the previous pair did overlap a large integrated suite. Provider latency, host load and cache availability are uncontrolled.
- Usage is summed once per assistant message_end event and independently checked against persisted session messages. Model turns are completed assistant responses, not scientific process submissions or necessarily all hidden provider retries.
- Total tokens include noncached input, output, cached reads and any cached writes. Dollar values preserve the runner's historical `provider_reported_cost_usd` field name but represent Pi's `usage.cost.total` estimate. No authoritative provider bill is populated (`totalFromProvider` is zero).
- Discovery output is the number of text characters returned by tools before the first write/edit. It includes legitimate collaborator and test-guidance reads, not just wasted searches. Tool-output character counts are not exact token counts.
- Tool durations come from harness tool-result durationMs. Overlapping tool intervals are merged for aggregate tool time; no claim is made that wall time minus tools precisely measures model reasoning.
- Shared collaborators and acceptance tests remain unchanged in both submissions. Their restored copies are used for independent validation. Only each new solution.py and test_solution.py appear in the agent workspace diff. Tests are visible, not held out.

## Additional test timing experiment

After both agents and their independent validations finish, timing_probe.py reruns the frozen graph submission's full 30-test suite in serial / xdist-two-workers / xdist-two-workers / serial order. Every run uses the same source, test cases, backend and profile-fixture machinery. All 30 cases pass on all four runs. Commands, wall times, logs and JUnit XML are retained separately in test-timing-probe/.

No additional model calls or solution changes occur in this probe. It is excluded from all agent solve metrics. The ABBA order reduces simple warmup/order bias but two repeats per mode do not support a statistically powered inference. Estimated end-to-end saving is an extrapolation, not a rerun of an agent using parallel tests.

## Scope of conclusions

One new matched pair is sufficient to inspect concrete behavior (guide adoption, successful public input inspection, zero correction cycles), not to establish statistical significance. The canonical guide intentionally covers this bounded-workflow family. Broader productivity claims require multiple fresh matched pairs and different workflow families. Further reductions must retain the scientific task boundaries, acceptance/provenance checks and correctness bar rather than optimizing the metric by omitting work.
