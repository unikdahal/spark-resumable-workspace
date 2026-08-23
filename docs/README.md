# Resumable Spark driver on Spark 3.5.8 with Apache Celeborn

Verified against `apache/spark` `v3.5.8` (`5a48a37b2dbd`), `apache/celeborn` `main` (`00113daf`),
JDK 21.0.11. Every citation was read out of those trees or executed.

| File | What it is |
|---|---|
| `LLD-resumable-spark-driver.md` | **The design.** §4 = DPP, §5 = the hard cases, §9 = flags, §10 = edge cases, §12 = FMEA, §13 = tests |
| `impl/InputAnchor.scala` | The correctness boundary — four pin strengths, including split replay |
| `impl/DppAnchors.scala` | DPP capture and injection |
| `impl/QueryConstants.scala` | Query-scoped constant capture — moves a whole class of queries out of "nondeterministic" |
| `impl/DeterminismClassifier.scala` | The determinism lattice and the closure rule |
| `impl/BroadcastAnchor.scala` | Broadcast payload anchoring |
| `impl/PlanDigest.scala` | The resume key |
| `impl/ResumeCoordinator.scala` | Phase ordering and the eight-rung acceptance ladder |
| `impl/ResumeConf.scala` | Flags and the ten arm-time preconditions |
| `impl/ResumeAnchorStore.scala`, `impl/IdWatermark.scala` | Store SPI; identifier watermarks |
| `patches/patch-inventory.md` | Verified hunks, Spark and Celeborn |
| `experiments/digest_stability.py` | **Run this first.** Decides whether anchors can ever hit on your connector |
| `experiments/IdCounterReflectionProof.java` | Establishes that ID counters are restorable on stock 3.5.8 |

## The shape of it

Shuffle **data** survives a driver death; the **metadata naming it** does not. So the job is to
rebuild a catalog that points at surviving bytes and to *prove* those bytes still answer the
question being asked. The driver re-runs `main()`, rebuilds the plan naturally, fingerprints each
exchange, and adopts the surviving Celeborn shuffle when eight verification rungs all pass.

Failure mode is wasted work, not corruption. That property is the entire argument, and the
architecture exists to make it true by construction rather than by discipline.

## Cases that a narrower design declares out of scope, and how each is handled

- **DPP** — pinning subquery results is necessary and not sufficient, because DSv2 pruning happens
  inside the connector where Spark cannot see it. Solved by anchoring what the leaf *resolved to*:
  snapshot pinning, or replaying captured splits (`InputPartition` is `Serializable` by contract).
- **Broadcast stages** — the broadcast is a pure function of collected rows and the mode, both
  bounded by construction. Persist the rows; rebuild instead of recomputing the subtree.
- **Unpinnable scans** — split capture replaces "detect that the listing changed" with "make the
  listing irrelevant."
- **Indeterminate stages** — most of the nondeterminism is query-scoped constants assigned once per
  planning pass; capture and replay them. What remains is safe under atomic adoption plus a closure
  uniformity rule.
- **Write stages** — `WriterCommitMessage` is `Serializable`, so partial commits are recoverable for
  atomic DSv2 writers. Default off; it is the highest-value and highest-risk capability here.
- **Non-AQE plans** — simpler than AQE, and easy to forget.
- **In-flight tasks** — not solvable. Bound the loss and say so.

## Before building the bulk of it

1. Run `experiments/digest_stability.py` against a real versioned-table connector. If digests are
   unstable there, the hit rate is zero and the correct decision is to stop. One day.
2. Open the Celeborn conversation (patch 10) before writing Spark-side code. It gates everything,
   and a `LifecycleManager` persistence layer may be a strong contribution regardless.
3. Be able to say why decomposition — materialising intermediates at natural boundaries and making
   the orchestrator idempotent — does not apply to your queries.
