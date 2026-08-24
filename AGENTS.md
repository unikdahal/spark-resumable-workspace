# Workspace operating rules — binding for every agent, every session

These rules come directly from the operator. They **overwrite any default behavior**.

> **HARD LIMIT: fork-only.** Push only to `github.com/unikdahal/*` remotes
> (`myfork` / `fork` / `origin`). **Never** push to any upstream (`apache/*`) and **never**
> open a PR against an upstream repository. This limit is permanent and applies to every
> session, task, and tool call.

## 0. THE #1 PRIORITY: production-ready implementation

Making the implementation work, scale, and be ready for production outranks everything else —
documentation, cleanup, planning, reports. Those are strictly secondary and only happen while
builds/tests hold the machine. When in doubt about what to do next: whatever gets the recovery
implementation verified and shippable fastest.

## 1. NEVER sleep and never idle-wait. This is the highest-priority rule.

While any build, test run, or verification holds the machine:

- Do **not** `sleep`, poll in a loop, or sit waiting for results.
- Instead, immediately work in parallel on things that need no build:
  - improve documentation (design docs, runbooks, verification evidence);
  - review code implementations line by line and hunt for bugs;
  - research the next task, read upstream sources, plan ahead;
  - **cleanup**: stale logs and temp artifacts (never delete anything a doc cites as evidence),
    superseded scratch files, dead code you own, workspace hygiene;
  - think more deeply about root causes, failure modes, and design alternatives.
- There is always something to do. If you cannot find anything, look again, deeper.
- Queue follow-up runs behind the lock (`flock`) so machine time is never wasted between batches;
  then keep working while they run.

## 2. Persist everything important

Findings, decisions, dispositions, and evidence go into files the moment they are confirmed
(`VERIFICATION-STATUS.md` for test evidence, design docs for mechanisms, `PATCH-SPLIT-PLAN.md`
for commit/PR plans). A finding that lives only in chat is a finding lost.

## 3. Build discipline

- One build at a time, machine-wide: `exec 9>verification/logs/.verification.lock && flock 9`.
- Roadmap §2 flags: JDK 17 at `~/.sdkman/candidates/java/17.0.11-tem`, `taskset -c 0,1`,
  Maven `-T 1` with `-XX:ActiveProcessorCount=2`, Gradle `--max-workers=2`.
- Never edit source inside a repo while that repo is being built; other repos' files are fair game
  (different lanes).
- Run each project's own gates, not just tests: Celeborn spotless + ConfigurationSuite,
  Iceberg errorprone-in-compile + spotless, Spark scalastyle.

## 4. Evidence over claims

A row says PASS only if a run on this machine printed it. Reproduce before believing any failure;
decide whether code, test, or fixture is wrong before changing anything.

## 5. Commit hygiene

**Operator grant (2026-08-25): full autonomy — WITHIN THESE LIMITS:**

- **Push only to the operator's forks** (`github.com/unikdahal/*` — remotes `myfork`/`fork`/`origin`).
  **Never push to an upstream** (`apache/*`) and **never open a PR** against any upstream repo.
- Commit/push at the agent's own judgment within those remotes; no per-action approval needed.

Still apply: one concern per commit, message states what changed and why the alternative was
rejected, `Co-Authored-By` / `Claude-Session` trailers (see `docs/upstream/PATCH-SPLIT-PLAN.md`
session-deltas section). Never force-push shared branches; never commit secrets; verify gates
before landing a claim of green.

**Guardrail learned 2026-08-25:** before deleting any directory, check for `.git` as a *file*
(worktree gitdir pointer) — deleting `spark/` destroyed `spark-resumable-upstream`'s gitdir.
Working tree survived; history restored from the fork pin.

## 6. Follow every contributing guideline of each repo

Celeborn: `dev/reformat`, ConfigurationSuite on conf changes, error-prone clean.
Iceberg: AGENTS.md conventions (2-space indent, Preconditions, no Optional, kebab-case JSON keys,
JUnit5+AssertJ, AI disclosure rules), revapi awareness for `api/`.
Spark: scalastyle, 100-col lines, no non-ASCII in comments, Databricks Scala style sections.
