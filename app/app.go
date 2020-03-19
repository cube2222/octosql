package app

import (
	"context"
	"fmt"
	"log"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
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
	describe             bool
}

func NewApp(cfg *config.Config, dataSourceRepository *physical.DataSourceRepository, out output.Output, describe bool) *App {
	return &App{
		cfg:                  cfg,
		dataSourceRepository: dataSourceRepository,
		out:                  out,
		describe:             describe,
	}
}

func (app *App) RunPlan(ctx context.Context, stateStorage storage.Storage, plan logical.Node) error {
	sourceNodes, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository))
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}

	// We only want one partition at the end, to print the output easily.
	shuffled := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)

	// Only the first partition is there.
	var phys physical.Node = shuffled[0]

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	if app.describe {
		fmt.Print(graph.Show(phys.Visualize()).String())
		return nil
	}

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(app.cfg, stateStorage))
	if err != nil {
		return errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	programID := &execution.StreamID{Id: "root"}

	tx := stateStorage.BeginTransaction()
	stream, execOutput, err := exec.Get(storage.InjectStateTransaction(ctx, tx), variables, programID)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "couldn't commit transaction to get record stream from execution plan")
	}

	var rec *execution.Record
	for {
		tx := stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		watermark, err := execOutput.WatermarkSource.GetWatermark(ctx, tx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("current output watermark:", watermark)

		rec, err = stream.Next(ctx)
		if err == execution.ErrEndOfStream {
			log.Println("main end of stream")
			err := tx.Commit()
			if err != nil {
				log.Printf("couldn't commit transaction: %s", err)
				continue
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
				err = waitableError.Close()
				if err != nil {
					log.Println("couldn't close subscription: ", err)
				}
				continue
			}
			log.Println("main listening for changes")
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
			log.Println("main received change")
			err = waitableError.Close()
			if err != nil {
				log.Println("couldn't close subscription: ", err)
			}
			log.Println("main received change")
			continue
		} else if err != nil {
			tx.Abort()
			return errors.Wrap(err, "couldn't get next record")
		}

		err = tx.Commit()
		if err != nil {
			log.Printf("couldn't commit transaction: %s", err)
			continue
		}

		err = app.out.WriteRecord(rec)
		if err != nil {
			return errors.Wrap(err, "couldn't write record")
		}
	}

	err = app.out.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close output writer")
	}

	return nil
}
