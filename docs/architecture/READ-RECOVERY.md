# Read-path recovery (upstream track)

Written as a map of the intended Spark + Celeborn + Iceberg design. Byte layouts, key names, and
invariants live in [../upstream/PROTOCOL-SPEC.md](../upstream/PROTOCOL-SPEC.md). This file is the
end-to-end story.

## Driver A

There is a durable logical recovery id. Inputs that participate expose an immutable source
anchor. Spark asks the connector for its current anchor, then asks the durable recovery
provider to atomically resolve it. A replacement driver gets the original stored anchor, not
whichever version is current later. `RecoveryAnchorResolver.resolveTable` verifies that the
table accepts the chosen anchor.

```
Iceberg table currently at snapshot 918
        │
currentRecoveryAnchor()
        │
      "918"
        │
durable resolveSourceAnchor
        │
 store "918" once
```

Every replacement must read snapshot 918.

When Spark reaches a shuffle, the recovery provider derives stable identity from durable
execution identity, the canonicalized query, the canonicalized stage, mapper count, and
partition count. Celeborn registers a recovery intent before normal execution. If no old
committed catalog exists, Spark computes normally. Celeborn's shuffle path produces the bytes;
the recovery catalog is the durable external representation of that shuffle.

If Driver A dies, Spark scheduler state disappears. Celeborn bytes and recovery metadata
survive. The application recovery lease prevents premature reclamation.

## Driver B

Driver B starts with the same logical recovery id, reacquires the lease, re-resolves the same
source anchors, reconstructs the query, and arrives at the same semantic stage identity.

```
DAGScheduler
    │
 tryRecover
    │
 Celeborn provider
    │
 committed catalog?
    │
   yes → adoptShuffleFromCatalog()
         make bytes readable under Driver B's Spark shuffleId
         return reducer byte sizes
         registerRecoveredShuffle()
         synthetic MapStatuses
         AQE receives statistics
         map stage skipped (zero map tasks)
```

If lookup is ambiguous, the catalog is corrupt, the provider is unavailable, or adoption
fails, Spark does not compute "just in case". It fails closed.

If a recovered shuffle later becomes unreadable, Spark aborts rather than rerunning the old
map stage. A new physical generation must not be mixed with durable recovered state.

## What this is not

The POC track skipped maps by seeding tracker metadata and patching Celeborn adoption from
outside Spark's contracts. `spark-resume` skipped maps by rewriting the physical plan into a
leaf that reads an `ExchangeStore`. This track makes the scheduler the authority: recovery is
consulted before the shuffle map stage is registered and scheduled.
