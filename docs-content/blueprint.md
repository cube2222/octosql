@# OctoSQL

### Superpowered for joining, analysing and transforming data from multiple databases using SQL.

OctoSQL is a SQL query engine which allows you to write standard SQL queries on data stored in multiple SQL databases, NoSQL databases and files in various formats trying to push down as much of the work as possible to the source databases, not transferring unnecessary data.

OctoSQL does that by creating an internal representation of your query and later translating parts of it into the query languages or APIs of the source databases. Whenever a datasource doesn't support a given operation, OctoSQL will execute it in memory, so you don't have to worry about the specifics of the underlying datasources.

With OctoSQL you don't need O(n) client tools or a large data analysis system deployment. Everything's contained in a single binary.

#### Name ethymology

OctoSQL stems from Octopus SQL.

Octopus, because octopi have many arms, so they can grasp and manipulate multiple objects, like OctoSQL is able to handle multiple datasources simultaneously.

@## Quick start

### Install

Either download the binary for your operating system (Linux, OS X and Windows are supported) from the Releases page, or install using the go command line tool:
```sh
go get -u github.com/cube2222/octosql/cmd/octosql
```

### Setup your datasources

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
You can also use the --config command line argument.

### First query

You can now query your datasources. Let's type the following command:

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


@## Datasources support

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

@page quick-insights
