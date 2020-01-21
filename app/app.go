package app

import (
	"context"
	"log"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

type App struct {
	cfg                  *config.Config
	dataSourceRepository *physical.DataSourceRepository
	out                  output.Output
}

func NewApp(cfg *config.Config, dataSourceRepository *physical.DataSourceRepository, out output.Output) *App {
	return &App{
		cfg:                  cfg,
		dataSourceRepository: dataSourceRepository,
		out:                  out,
	}
}

func (app *App) RunPlan(ctx context.Context, stateStorage storage.Storage, plan logical.Node) error {
	phys, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository))
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(app.cfg, stateStorage))
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
			log.Println("main new transaction required")
		} else if waitableError := execution.GetErrWaitForChanges(err); waitableError != nil {
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit: ", err)
				continue
			}
			log.Println("main listening for changes")
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
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

		err = app.out.WriteRecord(rec)
		if err != nil {
			return errors.Wrap(err, "couldn't write record")
		}
	}
	if err != execution.ErrEndOfStream {
		return errors.Wrap(err, "couldn't get next record")
	}

	err = app.out.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close output writer")
	}

	return nil
}
