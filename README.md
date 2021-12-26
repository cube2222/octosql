![OctoSQL](images/octosql.svg)OctoSQL
=======

OctoSQL is predominantly a CLI tool which lets you query a plethora of databases and file formats using SQL through a unified interface, even do JOINs between them. (Ever needed to join a JSON file with a PostgreSQL table? OctoSQL can help you with that.)

At the same time it's an easily extensible full-blown dataflow engine, and you can use it to add a SQL interface to your own applications.

<GIF>: invoices.csv and customers table in PostgreSQL.
![Demo](images/octosql-demo.gif)

## Usage

```
octosql "SELECT * FROM ./myfile.json"
octosql "SELECT * FROM ./myfile.json" --describe  # Show the schema of the file.
octosql "SELECT invoices.id, address, amount
         FROM invoices.csv JOIN db.customers ON invoices.customer_id = customers.id
         ORDER BY amount DESC"
octosql "SELECT customer_id, SUM(amount)
         FROM invoices.csv
         GROUP BY customer_id"
```

OctoSQL supports JSON and CSV files out of the box, but you can additionally install plugins to add support for other databases.
```
octosql "SELECT * FROM plugins.available_plugins"
octosql plugin install postgres
echo "databases:
  - name: mydb
    type: postgres
    config:
      host: localhost
      port: 5443
      database: mydb
      user: postgres
      password: postgres" > octosql.yml
```

## Installation

Either download the binary for your operating system from the [Releases page](https://github.com/cube2222/octosql/releases), or install using the go command line tool:
```bash
go install -u github.com/cube2222/octosql
```

## Contributing

OctoSQL doesn't accept external contributions to the code right now, but you can raise issues or develop external plugins for database types you'd like OctoSQL to support. Create a Pull Request to add a plugin to the core plugins repository (which is contained in the plugin_repository.json file).
