package octosql

import (
	"context"
	"fmt"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/pkg/errors"
	"log"
	"reflect"
)

// Runs query plan
func (e *OctosqlExecutor) RunPlan(ctx context.Context, stateStorage storage.Storage, plan logical.Node) error {
	phys, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(e.dataSources))
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(e.cfg, stateStorage))
	if err != nil {
		return errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	stream, err := exec.Get(ctx, variables)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	var rec *execution.Record
	for {
		tx := stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		rec, err = stream.Next(ctx)
		if err == execution.ErrEndOfStream {
			err := tx.Commit()
			if err != nil {
				return errors.Wrap(err, "couldn't commit transaction")
			}
			break
		} else if errors.Cause(err) == execution.ErrNewTransactionRequired {
			log.Println("main new transaction required: ", err)
			err := tx.Commit()
			if err != nil {
				log.Println("main couldn't commit: ", err)
				continue
			}
			continue
		} else if waitableError := execution.GetErrWaitForChanges(err); waitableError != nil {
			log.Println("main wait for changes: ", err)
			err := tx.Commit()
			if err != nil {
				log.Println("main couldn't commit: ", err)
				continue
			}
			log.Println("main listening for changes")
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("couldn't close subscription: ", err)
			}
			log.Println("main received change")
			continue
		} else if err != nil {
			tx.Abort()
			log.Println(err)
			break // TODO: Error propagation?
		}

		err := tx.Commit()
		if err != nil {
			return errors.Wrap(err, "couldn't commit transaction")
		}

		err = e.output.WriteRecord(rec)
		if err != nil {
			return errors.Wrap(err, "couldn't write record")
		}
	}
	if err != execution.ErrEndOfStream {
		return errors.Wrap(err, "couldn't get next record")
	}

	err = e.output.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close output writer")
	}

	return nil
}

// Helper to run parsed AST node
func (e *OctosqlExecutor) runParsedQuery(ctx context.Context, program *sqlparser.Program) error {
	if program == nil {
		return errors.New("Empty program cannot be executed.")
	}

	if program.Command.Statement != nil {
		selectStmt, ok := program.Command.Statement.(sqlparser.SelectStatement)
		if !ok {
			ddlStmt, ok := ((program.Command.Statement).(interface{})).(*sqlparser.DDL)
			if !ok {
				return fmt.Errorf("Invalid statement type: %v", reflect.TypeOf(program.Command.Statement))
			}
			err := e.handleDDLStatement(ctx, ddlStmt)
			if err != nil {
				return err
			}
		} else {
			err := e.handleSelectStatement(ctx, selectStmt)
			if err != nil {
				return err
			}
		}
	}

	// Run next query
	if program.Command.Next != nil {
		return e.runParsedQuery(ctx, program.Command.Next)
	}
	return nil
}

// Run SQL query
func (e *OctosqlExecutor) RunQuery(ctx context.Context, query string) error {
	// Parse query
	program, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}

	return e.runParsedQuery(ctx, program)
}