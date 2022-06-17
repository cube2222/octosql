---
sidebar_position: 1
---

# Introduction

OctoSQL is a CLI that lets you use SQL to query various kinds of data - files, databases, streams and other sources that have been implemented as plugins.

## Basic querying

Let's start with querying a simple CSV file.

```csv title="invoices.csv"
id,customer_id,amount
42,42,979.99
43,7,1979.99
44,1,1239.99
45,15,400.00
46,7,420.00
```

In order to query that file, we can just use the filename as a table name:
```shell
~> octosql "SELECT * FROM invoices.csv"
+-------------+----------------------+-----------------+
| invoices.id | invoices.customer_id | invoices.amount |
+-------------+----------------------+-----------------+
|          42 |                   42 |          979.99 |
|          43 |                    7 |         1979.99 |
|          44 |                    1 |         1239.99 |
|          45 |                   15 |             400 |
|          46 |                    7 |             420 |
+-------------+----------------------+-----------------+
```
