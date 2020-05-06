# Contributing a new data source

There are a few parts to implementing a data source. Usually it's best to copy an existing one and modify it.

1. Your database has to advertise which filters can be pushed down to it. Either on primary or secondary keys. You also have to advertise what the primary keys are. Relations on secondary keys may contain the key on both sides, primary keys will only be present on at most one side.
2. You need to create a *NewDataSourceBuilderFactory* function, which takes arguments that are necessary to know when creating the query plan and optimizing it (like the key name for a redis database). Other configuration options should be read using the closure you then pass to *physical.NewDataSourceBuilderFactory* (like the database password). In this closure is the place where you may actually connect to databases, and where you can transform the filters you received into a custom representation. (i.e. stored procedures for MySQL, a nice tree like predicate structure for Redis). Check out the existing datasources for more information.
3. You need to provide a *NewDataSourceBuilderFactoryFromConfig* function, which reads the configuration necessary for the query plan creation, and passes it to *NewDataSourceBuilderFactory*. You also have to call this in the main function of OctoSQL where datasources are registered.
4. A materialized datasource (that's what the closure is used for) should provide a *Get* function returning a stream of records. It receives the *variables* argument, which provides values for any Variables which may be present in the pushed down filters.
5. The datasource should be concurrency-safe. Multiple goroutines may call Get on it simultaneously with different variables. Returned streams don't have to be thread safe.
6. Create integration tests for your datasource. Modify the circle ci configuration to add a stage for your datasource.
7. Other than that, follow the types.
8. If something is unclear or you need help, feel free to shoot us a message or create an issue!

## Important

* Datasource PR's without proper tests will not be accepted.
* It's best if you create an issue to notify everybody about what you're working on, even if you're not sure if you are able to finish it. Because no work will be duplicated.