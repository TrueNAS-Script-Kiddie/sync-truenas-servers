# Operations

What a workload can ask for. Each entry says what the operation does, what it needs and its
constraints — never how it is built. An operation marked **new** has no existing mechanism.

Grows with every workload's catalogue entry; so far it holds what grafana needs.

| Operation | Does | Needs | Constraints |
|---|---|---|---|
| **Definition replication** | Gives the target the source's definition with every per-host value replaced by the target's own; creates the app when it is missing | The target's host paths exist; the source's catalog version is available on the target | Never silently a different catalog version. Generated values are never sent. After the change, the target's actual definition is compared with the predicted one |
| **Stop / start an app** | Brings an app to the state a step requires and back to the state it had | — | Stopping an app removes its containers; they exist only while it runs. A running target is overwritten only on an explicit flag |
| **Directory copy** | Makes a directory on the target equal to the source's, keeping numeric owner and mode | The app stopped where the content is written while running | Removes what the source no longer has. Access control lists come from the target's dataset, not from the source |
| **Value rewrite inside a SQLite database** — **new** | Replaces host tokens in named columns of a database file on the target | The app stopped on the target; after the copy, before the start | Only named table/column pairs, never a blind replace over the file |
| **Token scan** | Searches a workload's definition, files and database rows for the other host's tokens and reports each hit with its location | The target after a sync | A hit outside what the catalogue classifies as history means the workload is not independent |
