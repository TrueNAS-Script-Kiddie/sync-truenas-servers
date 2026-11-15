# Requirements catalogue

What replication must handle, per workload, with the evidence behind every statement. Input for
the spec design; it describes *what is on the hosts and what must happen to it*, never how any
tool does it today.

| File | Content |
|---|---|
| `<workload>.md` | One per workload, following the template below |
| [operations.md](operations.md) | The operations workloads can select |
| [drift.md](drift.md) | Master/backup differences that are not per-host by nature — for the user to converge, never to specify |

## Evidence references

`E:<path>` is a file under `plans/redesign/evidence/evidence-master/` **and**
`…/evidence-backup/` (same relative path; a host name inside the path changes with it). `E(master):`
/ `E(backup):` names one host's copy. Secrets show as `<masked:…>`: the same placeholder on both
hosts means the same value.

A statement without an evidence reference is a hypothesis and is marked **unconfirmed**.

## Host tokens

What makes a value a host reference:

| | truenas-master | truenas-backup |
|---|---|---|
| IP | `192.168.0.10` | `192.168.0.60` |
| hostname | `truenas-master` | `truenas-backup` |
| pools | `ssdmaster-pool`, `master-pool` | `backup-pool` |
| app name suffix (temporary, until the names converge) | `-master` | `-backup` |

## Classification

Every item of a workload gets exactly one:

| Class | Meaning |
|---|---|
| **copy** | Shared content: the target gets the source's, unchanged |
| **per host** | Each host keeps its own value; the source's never reaches the target |
| **rewrite** | Copied, with host tokens replaced by the target's |
| **generated** | Produced by the platform or the app from the definition at start; never handled |
| **regenerable** | A cache the app rebuilds by itself; never handled |
| **runtime** | State of the running instance (locks, timestamps, health); rides along with whatever contains it, needs no rule |
| **never touch** | Must not be read, copied or changed |

A host reference inside app data also states whether it can be made **host-neutral** (an address
that is identical on both hosts, or a value the app derives from its definition). Host-neutral
beats a rewrite.

## Workload template

1. **Identity** — kind, catalog app, train, versions, image, state on each host.
2. **Definition** — every value: class, both hosts' values where they differ, reason, evidence.
3. **Storage** — every mount: what it is, where it lives, ownership, class.
4. **Data items** — files and databases inside the mounts: class, reason, evidence.
5. **Host references** — every token-scan hit, classified; host-neutral option.
6. **Constraints** — order, state, versions.
7. **Operations needed** — from [operations.md](operations.md).
8. **Open** — what the evidence does not settle, and what would settle it.
