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

We can also do grouping and ordering:
```shell
~>  octosql "SELECT id, SUM(amount) as amount_sum
             FROM ./invoices.csv
             GROUP BY id 
             ORDER BY amount_sum DESC"
+----+------------+
| id | amount_sum |
+----+------------+
| 42 |     979.99 |
| 43 |    1979.99 |
| 44 |    1239.99 |
| 45 |        400 |
| 46 |        420 |
+----+------------+
```

If you want to find out more about quering files, take a look at [Advanced File Querying](/docs/advanced-concepts/advanced-file-querying).

## Querying Databases

Crucially though, OctoSQL also lets you use **plugins** which add support for various other datasources. One of these is the PostgreSQL plugin, which we can easily install:
```shell
~> octosql plugin install postgres
Downloading core/postgres@0.3.0...
```

Now, we'll need to configure the specific PostgreSQL database we want to use:
```shell
~> echo "databases:
  - name: store
    type: postgres
    config:
      host: localhost
      port: 5432
      database: postgres
      schema: public
      user: postgres
      password: postgres" > ~/.octosql/octosql.yml
```

With this, we'll be able to query the `store` PostgreSQL database. We can, for example, get the email of each invoice, by joining with the customers table:

```shell
~> octosql "SELECT invoices.id, email, amount
            FROM invoices.csv
              JOIN store.customers ON invoices.customer_id = customers.id
            ORDER BY amount DESC"
+-------------+------------------------------+-----------------+
| invoices.id |       customers.email        | invoices.amount |
+-------------+------------------------------+-----------------+
|          42 | 'donetta.cummings@email.com' |          979.99 |
|          43 | 'lindsay.kozey@email.com'    |         1979.99 |
|          44 | 'caterina.ebert@email.com'   |         1239.99 |
|          45 | 'angelo.friesen@email.com'   |             400 |
|          46 | 'lindsay.kozey@email.com'    |             420 |
+-------------+------------------------------+-----------------+
```

To see more details about the `customers` table, we can describe it:
```shell
~> octosql "SELECT * FROM store.customers" --describe
+--------------------------+-----------------+------------+
|           name           |      type       | time_field |
+--------------------------+-----------------+------------+
| 'customers.email'        | 'String'        | false      |
| 'customers.first_name'   | 'String'        | false      |
| 'customers.id'           | 'Int'           | false      |
| 'customers.last_name'    | 'String'        | false      |
| 'customers.phone_number' | 'NULL | String' | false      |
+--------------------------+-----------------+------------+
```

## Summary

This document has hopefully shown you how to use OctoSQL for various simple use cases, and given you an intuition of what you can expect from it, and when it might be the right tool to use.
