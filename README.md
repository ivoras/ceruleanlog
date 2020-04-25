# ceruleanlog

A tighter and faster log server & analyzer, in Go.

Why? I find Graylog to be cool but extremely slow for the limited functionalities I need:

* Storing (almost) arbitrary flat JSON documents (no nested structures)
* Querying them by time and by simple filtering operations on (some of the) stored fields
* Single-server operation
* Reasonable performance up to 2 TB of stored data

### MVP goals

* Ingests GELF JSON documents
* Stores data in SQLite3 databases auto-sharded by time
* Has configurable SQLite3 journal_mode
* Has configurable shard time
* Has configurable memory buffer time (or 0 for sync mode) 
* Has configurable indexing
* Implements a query API
* Supports simple queries via SQL syntax
* Has a simple web GUI to fetch and display tabular data

