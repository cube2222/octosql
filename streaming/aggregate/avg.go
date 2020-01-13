package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var currentAvgPrefix = []byte("$current_avg$")
var currentAvgNumPrefix = []byte("$current_avg_num$")

type Average struct {
}

func NewAverageAggregate() *Average {
	return &Average{}
}

func (agg *Average) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentAvgStorage := storage.NewValueState(tx.WithPrefix(currentAvgPrefix))
	currentAvgNumStorage := storage.NewValueState(tx.WithPrefix(currentAvgNumPrefix))

	currentAvgVal, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current avg")
	}

	var currentAvgNum octosql.Value
	err = currentAvgNumStorage.Get(&currentAvgNum)
	if err == storage.ErrKeyNotFound {
		currentAvgNum = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current elements number for avg from storage")
	}

	newScalar := octosql.MakeFloat(float64(currentAvgNum.AsInt()) / float64(currentAvgNum.AsInt()+1))
	currentAvgValScaled := octosql.MakeFloat(currentAvgVal.AsFloat() * newScalar.AsFloat())

	var newComponent octosql.Value

	switch valueType := value.GetType(); valueType {
	case octosql.TypeInt:
		newComponent = octosql.MakeFloat(float64(value.AsInt()) / float64(currentAvgNum.AsInt()+1))
	case octosql.TypeFloat:
		newComponent = octosql.MakeFloat(value.AsFloat() / float64(currentAvgNum.AsInt()+1))
	default:
		return errors.Errorf("unsupported value type passed to avg: %s", valueType)
	}

	currentAvgVal = octosql.MakeFloat(currentAvgValScaled.AsFloat() + newComponent.AsFloat())
	currentAvgNum = octosql.MakeInt(currentAvgNum.AsInt() + 1)

	err = currentAvgStorage.Set(&currentAvgVal)
	if err != nil {
		return errors.Wrap(err, "couldn't set current avg in storage")
	}

	err = currentAvgNumStorage.Set(&currentAvgNum)
	if err != nil {
		return errors.Wrap(err, "couldn't set current elements number for avg in storage")
	}

	return nil
}

func (agg *Average) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	currentAvgStorage := storage.NewValueState(tx.WithPrefix(currentAvgPrefix))
	currentAvgNumStorage := storage.NewValueState(tx.WithPrefix(currentAvgNumPrefix))

	currentAvgVal, err := agg.GetValue(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current avg")
	}

	var currentAvgNum octosql.Value
	err = currentAvgNumStorage.Get(&currentAvgNum)
	if err == storage.ErrKeyNotFound {
		currentAvgNum = octosql.MakeInt(0) // TODO - should this return error?
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current elements number for avg from storage")
	}

	if currentAvgNum.AsInt() <= 1 { // Retracting last element
		err = currentAvgStorage.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current avg in storage")
		}

		err = currentAvgNumStorage.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't clear current elements number for avg in storage")
		}

		return nil
	}

	newScalar := octosql.MakeFloat(float64(currentAvgNum.AsInt()) / float64(currentAvgNum.AsInt()-1))
	currentAvgValScaled := octosql.MakeFloat(currentAvgVal.AsFloat() * newScalar.AsFloat())

	var retractedComponent octosql.Value

	switch valueType := value.GetType(); valueType {
	case octosql.TypeInt:
		retractedComponent = octosql.MakeFloat(float64(value.AsInt()) / float64(currentAvgNum.AsInt()-1))
	case octosql.TypeFloat:
		retractedComponent = octosql.MakeFloat(value.AsFloat() / float64(currentAvgNum.AsInt()-1))
	default:
		return errors.Errorf("unsupported value type passed to avg: %s", valueType)
	}

	currentAvgVal = octosql.MakeFloat(currentAvgValScaled.AsFloat() - retractedComponent.AsFloat())
	currentAvgNum = octosql.MakeInt(currentAvgNum.AsInt() - 1)

	err = currentAvgStorage.Set(&currentAvgVal)
	if err != nil {
		return errors.Wrap(err, "couldn't set current avg in storage")
	}

	err = currentAvgNumStorage.Set(&currentAvgNum)
	if err != nil {
		return errors.Wrap(err, "couldn't set current elements number for avg in storage")
	}

	return nil
}

func (agg *Average) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	currentAvgStorage := storage.NewValueState(tx.WithPrefix(currentAvgPrefix))

	var currentAvg octosql.Value

	err := currentAvgStorage.Get(&currentAvg)
	if err == storage.ErrKeyNotFound {
		return octosql.MakeInt(0), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current avg from storage")
	}

	return currentAvg, nil
}

func (agg *Average) String() string {
	return "average"
}
