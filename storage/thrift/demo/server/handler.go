package server

import (
	"context"
	"fmt"
	"github.com/cube2222/octosql/storage/thrift/demo/gen-go/model"
	"strconv"
	"sync"
)

type CalculatorHandler struct {
	iterators map[int32]int32
	nextFreeIterator int32
	shouldClose bool
	serverLiveWG sync.WaitGroup
}

func NewCalculatorHandler() *CalculatorHandler {
	return &CalculatorHandler{
		nextFreeIterator: 0,
		iterators: map[int32]int32{},
		serverLiveWG: sync.WaitGroup{},
	}
}

func (p *CalculatorHandler) Ping(ctx context.Context) (err error) {
	fmt.Print("ping()\n")
	return nil
}

func (p *CalculatorHandler) Close(ctx context.Context) (err error) {
	fmt.Print("close()\n")
	p.serverLiveWG.Done()
	return nil
}

func (p *CalculatorHandler) FindSimilar(ctx context.Context, image []byte) (retval17 int32, err error) {
	fmt.Print("findSimilar()\n")
	return 42, nil
}

func (p *CalculatorHandler) OpenRecords(ctx context.Context) (r int32, err error) {
	fmt.Print("OpenRecords()\n")
	p.nextFreeIterator++
	p.iterators[p.nextFreeIterator] = 0
	return p.nextFreeIterator, nil
}

func (p *CalculatorHandler) GetRecord(ctx context.Context, streamID int32) (r *model.Record, err error) {
	fmt.Print("getRecord()\n")
	if p.iterators[streamID] < 5 {
		var r model.Record = model.Record{
			A: "record" + strconv.Itoa(int(p.iterators[streamID])),
			B: "B",
			Bar: nil,
			Lst: []string{
				"elo", "hehs",
			},
			Foo: &model.Foo{
				FooString: "acab",
				FooInt16: 16,
				FooInt32: 32,
				FooInt64: 64,
				FooBool: true,
				FooDouble: 0.77,
				FooByte: 42,
			},
		}
		p.iterators[streamID]++
		return &r, nil
	}
	//var rec model.Record = model.Record{A: "stringa", B: "string"}
	return nil, nil
}