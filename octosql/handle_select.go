package octosql

import (
	"context"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/dgraph-io/badger/v2"
	"log"
)

// This function executes all SQL select statements
func (e *OctosqlExecutor) handleSelectStatement(ctx context.Context, statement sqlparser.SelectStatement) error {
	// Parse input statement
	plan, err := parser.ParseNode(statement)
	if err != nil {
		log.Fatal("couldn't parse query: ", err)
	}

	opts := badger.DefaultOptions("")
	opts.Dir = ""
	opts.ValueDir = ""
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal("couldn't open in-memory badger database: ", err)
	}
	stateStorage := storage.NewBadgerStorage(db)

	// Run query
	return e.RunPlan(ctx, stateStorage, plan)
}