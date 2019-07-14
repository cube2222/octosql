OctoSQL
=======
OctoSQL is a data querying tool, allowing you to join, analyze and transform data from multiple data sources and file formats using SQL.

## Table of Contents
- [What is OctoSQL?](#what-is-octosql)
- [Quickstart](#quickstart)
- [Installation](#installation)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Architecture](#architecture)
- [Datasource Pushdown Operations](#datasource-pushdown-operations)
- [Roadmap](#roadmap)

## What is OctoSQL?
OctoSQL is a SQL query engine which allows you to write standard SQL queries on data stored in multiple SQL databases, NoSQL databases and files in various formats trying to push down as much of the work as possible to the source databases, not transferring unnecessary data. 

OctoSQL does that by creating an internal representation of your query and later translating parts of it into the query languages or APIs of the source databases. Whenever a datasource doesn't support a given operation, OctoSQL will execute it in memory, so you don't have to worry about the specifics of the underlying datasources. 

With OctoSQL you don't need O(n) client tools or a large data analysis system deployment. Everything's contained in a single binary.

### Why the name?
OctoSQL stems from Octopus SQL.

Octopus, because octopi have many arms, so they can grasp and manipulate multiple objects, like OctoSQL is able to handle multiple datasources simultaneously.

## Quickstart
Let's say we have a csv file with cats, and a redis database with people (potential cat owners). Now we want to get a list of cities with the number of distinct cat names in them and the cumulative number of cat lives (as each cat has up to 9 lives left).

First, create a configuration file ([Configuration Syntax](#configuration))
For example:
```yaml
dataSources:
  - name: cats
    type: csv
    config:
      path: "~/Documents/cats.csv"
  - name: people
    type: redis
    config:
      address: "localhost:6379"
      password: ""
      databaseIndex: 0
      databaseKeyName: "id"
```

Then, set the **OCTOSQL_CONFIG** environment variable to point to the configuration file.
```bash
export OCTOSQL_CONFIG=~/octosql.yaml
```

Finally, query to your hearts desire:
```bash
octosql "SELECT p.city, FIRST(c.name), COUNT(DISTINCT c.name) cats, SUM(c.livesleft) catlives
FROM cats c JOIN people p ON c.ownerid = p.id
GROUP BY p.city
ORDER BY catlives DESC
LIMIT 9"
```
Example output:
```
+---------+--------------+------+----------+
| p.city  | c.name_first | cats | catlives |
+---------+--------------+------+----------+
| Warren  | Zoey         |   68 |      570 |
| Gadsden | Snickers     |   52 |      388 |
| Staples | Harley       |   54 |      383 |
| Buxton  | Lucky        |   45 |      373 |
| Bethany | Princess     |   46 |      366 |
| Noxen   | Sheba        |   49 |      361 |
| Yorklyn | Scooter      |   45 |      359 |
| Tuttle  | Toby         |   57 |      356 |
| Ada     | Jasmine      |   49 |      351 |
+---------+--------------+------+----------+
```
You can choose between table, tabbed, json and csv output formats.

## Installation
Either download the binary for your operating system (Linux, OS X and Windows are supported) from the [Releases page](https://github.com/cube2222/octosql/releases), or install using the go command line tool:
```bash
go get github.com/cube2222/octosql/cmd/octosql
```
## Configuration
The configuration file has the following form
```yaml
dataSources:
  - name: <table_name_in_octosql>
    type: <datasource_type>
    config:
      <datasource_specific_key>: <datasource_specific_value>
      <datasource_specific_key>: <datasource_specific_value>
      ...
  - name: <table_name_in_octosql>
    type: <datasource_type>
    config:
      <datasource_specific_key>: <datasource_specific_value>
      <datasource_specific_key>: <datasource_specific_value>
      ...
    ...
```
### Supported Datasources
#### JSON
JSON file in one of the following forms:
- one record per line, no commas
- JSON list of records
##### options:
- path - path to file containing the data, required
- arrayFormat - if the JSON list of records format should be used, defaults to false

---
#### CSV
CSV file seperated using commas. The first row should contain column names.
##### options:
- path - path to file containing the data, required

---
#### PostgreSQL
Single PostgreSQL database table.
##### options:
- address - address including port number, defaults to localhost:5432
- user - required
- password - required
- databaseName - required
- tableName - required
---
#### MySQL
Single MySQL database table.
##### options:
- address - address including port number, defaults to localhost:3306
- user - required
- password - required
- databaseName - required
- tableName - required
---
#### Redis
Redis database with the given index. Currently only hashes are supported.
##### options:
- address - address including port number, defaults to localhost:6379
- password - defaults to ""
- databaseIndex - index number of Redis database, defaults to 0
- databaseKeyName - column name of Redis key in OctoSQL records, defaults to "key"

## Documentation
Documentation for the available functions: https://github.com/cube2222/octosql/wiki/Function-Documentation

Documentation for the available aggregates: https://github.com/cube2222/octosql/wiki/Aggregate-Documentation

The SQL dialect documentation: TODO ;) in short though:

Available SQL constructs: Select, Where, Order By, Group By, Offset, Limit, Left Join, Right Join, Inner Join, Distinct, Union, Union All, Subqueries, Operators.

Available SQL types: Int, Float, String, Bool, Time, Duration, Tuple (array), Object (e.g. JSON)

## Architecture
An OctoSQL invocation gets processed in multiple phases.

### SQL AST
First, the SQL query gets parsed into an abstract syntax tree. This phase only rules out syntax errors.

### Logical Plan
The SQL AST gets converted into a logical query plan. This plan is still mostly a syntactic validation. It's the most naive possible translation of the SQL query. However, this plan already has more of a map-filter-reduce form.

If you wanted to add a new query language to OctoSQL, the only problem you'd have to solve is translating it to this logical plan.

### Physical Plan
The logical plan gets converted into a physical plan. This conversion finds any semantic errors in the query. If this phase is reached, then the input is correct and OctoSQL will be able execute it.

This phase already understands the specifics of the underlying datasources. So it's here where the optimizer will iteratively transform the plan, pushing computiation nodes down to the datasources, and deduplicating unnecessary parts.

The optimizer uses a pattern matching approach, where it has rules for matching parts of the physical plan tree and how those patterns can be restructured into a more efficient version. The rules are meant to be as simple as possible and make the smallest possible changes. This way, the optimizer just keeps on iterating on the whole tree, until it can't change anything anymore. This ensures that the plan reaches a local performance minimum, and the rules should be structured so that this local minimum is equal - or close to - the global minimum.

Here is an example diagram of an optimized physical plan:
![Physical Plan](images/physical.png)

### Execution Plan
The physical plan gets materialized into an execution plan. This phase has to be able to connect to the actual datasources. It may initialize connections, open files, etc.

### Stream
Starting the execution plan creates a stream, which underneath may hold more streams, or parts of the execution plan to create streams in the future. This stream works in a pull based model.

## Database Pushdown Operations
|Datasource	|Equality	|In	|> < <= =>	|
|---	|---	|---	|---	|
|MySQL	|supported	|supported	|supported	|
|PostgreSQL	|supported	|supported	|supported	|
|Redis	|supported	|supported	|scan	|
|JSON	|scan	|scan	|scan	|
|CSV	|scan	|scan	|scan	|

Where scan means that the whole table needs to be scanned for each access. We are planning to add an in memory index in the future, which would allow us to store small tables in-memory, saving us a lot of unnecessary reads.

## Roadmap
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