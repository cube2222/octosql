@# Quick insights

@## Architecture

An OctoSQL invocation gets processed in multiple phases.

### SQL AST
First, the SQL query gets parsed into an abstract syntax tree. This phase only rules out syntax errors.

### Logical Plan
The SQL AST gets converted into a logical query plan. This plan is still mostly a syntactic validation. It's the most naive possible translation of the SQL query. However, this plan already has more of a map-filter-reduce form.

If you wanted to add a new query language to OctoSQL, the only problem you'd have to solve is translating it to this logical plan.

### Physical Plan
The logical plan gets converted into a physical plan. This conversion finds any semantic errors in the query. If this phase is reached, then the input is correct and OctoSQL will be able execute it.

This phase already understands the specifics of the underlying datasources. So it's here where the optimizer will iteratively transform the plan, pushing computation nodes down to the datasources, and deduplicating unnecessary parts.

The optimizer uses a pattern matching approach, where it has rules for matching parts of the physical plan tree and how those patterns can be restructured into a more efficient version. The rules are meant to be as simple as possible and make the smallest possible changes. For example, pushing filters under maps, if they don't use any mapped variables. This way, the optimizer just keeps on iterating on the whole tree, until it can't change anything anymore. (each iteration tries to apply each rule in each possible place in the tree) This ensures that the plan reaches a local performance minimum, and the rules should be structured so that this local minimum is equal - or close to - the global minimum. (i.e. one optimization, shouldn't make another - much more useful one - impossible)

Here is an example diagram of an optimized physical plan:
![Physical Plan](images/physical.png)

### Execution Plan
The physical plan gets materialized into an execution plan. This phase has to be able to connect to the actual datasources. It may initialize connections, open files, etc.

### Stream
Starting the execution plan creates a stream, which underneath may hold more streams, or parts of the execution plan to create streams in the future. This stream works in a pull based model.

@## Database Pushdown Operations

|Datasource	|Equality	|In	|> < <= =>	|
|---	|---	|---	|---	|
|MySQL	|supported	|supported	|supported	|
|PostgreSQL	|supported	|supported	|supported	|
|Redis	|supported	|supported	|scan	|
|JSON	|scan	|scan	|scan	|
|CSV	|scan	|scan	|scan	|

Where scan means that the whole table needs to be scanned for each access. We are planning to add an in memory index in the future, which would allow us to store small tables in-memory, saving us a lot of unnecessary reads.

@page roadmap
