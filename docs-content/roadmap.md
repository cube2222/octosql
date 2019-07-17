@# Project roadmap

- Additional Datasources.
- SQL Constructs:
  - JSON Query
  - Window Functions
  - Polymorphic Table Functions (i.e. RANGE(1, 10) in table position)
  - HAVING, ALL, ANY
- Parallel expression evaluation.
- Custom sql parser, so we can use sane function names, and support new sql constructs.
- Streams support (Kafka, Redis)
- Push down functions, aggregates to databases that support them.
- An in-memory index to save values of subqueries and save on rescanning tables which don't support a given operation, so as not to recalculate them each time.
- MapReduce style distributed execution mode.
- Runtime statistics
- Server mode
- Querying a json or csv table from standard input.
- Integration test suite
- Tuple splitter, returning the row for each tuple element, with the given element instead of the tuple.
- Describe-like functionality as in the diagram above.
