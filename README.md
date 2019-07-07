OctoSQL
=======
OctoSQL is a data query tool, allowing you to join, analyze and transform data from multiple data sources and file formats using SQL.

## Table of Contents
- [What is OctoSQL?](#what-is-octosql)
- [Quickstart](#quickstart)
- [Installation](#installation)
- [Configuration](#configuration)
- [Documentation](#documentation)
- Supported Databases (with an optimal functionality matrix)
- [Architecture](#architecture)
- Some examples of query diagrams
- [Roadmap](#roadmap)

## What is OctoSQL?
OctoSQL is a SQL query engine which allows you to write standard SQL queries on data in multiple SQL databases, NoSQL databases and files in various formats trying to push down as much of the work as possible to the source databases, not transferring unnecessary data. 

OctoSQL does that by creating an internal representation of your query and later translating parts of it into the query languages or APIs of the source databases. Whenever a datasource doesn't support a given operation, OctoSQL will execute it in memory, so you don't have to worry about the specifics of the underlying datasources. 

With OctoSQL you don't need O(n) client tools or a large data analysis system deployment. Everything's contained in a single binary.

**Why the name?**
TODO

## Quickstart
Let's say we have a csv file with cats, and a redis database with people (potential cat owners). Now we want to get a list of cats with the cities their owners live in.

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
octosql "SELECT c.name, c.livesleft, p.city
FROM cats c JOIN people p ON c.ownerid = p.id"
```
Example output:
```
+----------+-------------+----------------+
|  c.name  | c.livesleft |     p.city     |
+----------+-------------+----------------+
| Buster   |           6 | Ivanhoe        |
| Tiger    |           4 | Brambleton     |
| Lucy     |           1 | Dunlo          |
| Pepper   |           3 | Alleghenyville |
| Tiger    |           2 | Katonah        |
| Molly    |           6 | Babb           |
| Precious |           8 | Holcombe       |
+----------+-------------+----------------+
```
You can choose between table, tabbed, json and csv output formats.

## Installation
Either download the binary for your operating system (Linux, OS X and Windows are supported) from the [Releases page](https://github.com/cube2222/octosql/releases), or install using the go command line tool:
```bash
go get github.com/cube2222/octosql/cmd/octosql
```
## Configuration
The configuration file has the form
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
CSV file seperated using commas.
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
The SQL dialect documentation:

Function documentation:

## Roadmap
- Add arithmetic operators.
- Write custom sql parser, so we can use sane function names.
- Push down functions to supported databases.
- Implement an in-memory index to save values of subqueries, so as not to recalculate them each time.
- MapReduce style distributed execution mode.
- Function Tables (RANGE(1, 10) for example)
- Better nested JSON support.
- HAVING clause.

## Architecture
An OctoSQL invocation gets work done in multiple stages.

### SQL AST
First, the SQL query gets parsed into an abstract syntax tree. This stage only rules out syntax errors.

### Logical Plan
The SQL AST gets converted into a logical query plan. This plan is still mostly a syntactic validation. It's the most naive possible translation of the SQL query. However, this plan already has more of a map-filter-reduce form.

If you wanted to add a new query language to OctoSQL, the only problem you'd have to solve is translating it to this logical plan.

TODO: diagram

### Physical Plan
The logical plan gets converted into a physical plan. This conversion finds any logical errors in the query. If this stage is reached, then the input is correct and OctoSQL can execute it.

This stage also already understands the specifics of the underlying datasources. So it's here where the optimizer will iteratively transform the plan, pushing computiation nodes down to the datasources, and deduplicating unnecessary parts.

The optimizer uses a pattern matching approach, where it has rules for matching parts of the physical plan tree and how those patterns can be restructured into a more efficient version. The rules are meant to be as simple as possible and make the smallest possible changes. This way, the optimizer just keeps on iterating on the whole tree, until it can't change anything anymore. This ensures that the plan reaches a local performance minimum, and the rules should be structured so that this local minimum is equal - or close to - the global minimum.

TODO: diagram

### Execution Plan
The physical plan gets materialized into an execution plan. This stage has to be able to connect to the actual datasources. It may initialize connections, open files, etc.

TODO: diagram

### Stream
Starting the execution plan creates a stream, which underneath may hold more streams, or parts of the execution plan to create streams in the future. This stream works in a pull based model.

TODO: diagram