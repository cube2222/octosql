package batch

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/google/btree"
	"github.com/gosuri/uilive"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Format interface {
	SetSchema(physical.Schema)
	Write([]octosql.Value) error
	Close() error
}

type OutputPrinter struct {
	source                Node
	keyExprs              []Expression
	directionMultipliers  []int
	limit                 *int64
	noRetractionsPossible bool

	schema physical.Schema
	format func(io.Writer) Format
	live   bool
}

func NewOutputPrinter(source Node, keyExprs []Expression, directionMultipliers []int, limit *int64, noRetractionsPossible bool, schema physical.Schema, format func(io.Writer) Format, live bool) *OutputPrinter {
	return &OutputPrinter{
		source:                source,
		keyExprs:              keyExprs,
		directionMultipliers:  directionMultipliers,
		limit:                 limit,
		noRetractionsPossible: noRetractionsPossible,
		schema:                schema,
		format:                format,
		live:                  live,
	}
}

type outputItem struct {
	Key                  []octosql.Value
	Values               []octosql.Value
	Count                int
	DirectionMultipliers []int
}

func (item *outputItem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*outputItem)
	if !ok {
		panic(fmt.Sprintf("invalid order by key comparison: %T", than))
	}

	for i := 0; i < len(item.Key); i++ {
		if comp := item.Key[i].Compare(thanTyped.Key[i]); comp != 0 {
			return comp*item.DirectionMultipliers[i] == -1
		}
	}

	// If keys are equal, differentiate by values.
	for i := 0; i < len(item.Values); i++ {
		if comp := item.Values[i].Compare(thanTyped.Values[i]); comp != 0 {
			return comp == -1
		}
	}

	return false
}

func (o *OutputPrinter) Run(execCtx ExecutionContext) error {
	recordCounts := btree.New(BTreeDefaultDegree)
	watermark := time.Time{}
	liveWriter := uilive.New()
	lastUpdate := time.Now()

	onlyZeroEventTimesSeen := true

	printTable := func() {
		lastUpdate = time.Now()
		var buf bytes.Buffer

		format := o.format(&buf)
		format.SetSchema(o.schema)

		i := int64(0)
		recordCounts.Ascend(func(item btree.Item) bool {
			itemTyped := item.(*outputItem)
			for j := 0; j < itemTyped.Count; j++ {
				if o.limit != nil && i == *o.limit {
					return false
				}
				i++

				format.Write(itemTyped.Values)
			}
			return true
		})

		format.Close()

		if !watermark.IsZero() {
			fmt.Fprintf(&buf, "watermark: %s\n", watermark.Format(time.RFC3339Nano))
		}

		buf.WriteTo(liveWriter)
		liveWriter.Flush()
	}

	if err := o.source.Run(
		execCtx,
		func(ctx ProduceContext, record Record) error {
			key := make([]octosql.Value, len(o.keyExprs))
			for i := range o.keyExprs {
				keyValue, err := o.keyExprs[i].Evaluate(execCtx.WithRecord(record))
				if err != nil {
					return fmt.Errorf("couldn't evaluate order by %d key expression: %w", i, err)
				}
				key[i] = keyValue
			}

			item := recordCounts.Get(&outputItem{Key: key, Values: record.Values, DirectionMultipliers: o.directionMultipliers})
			var itemTyped *outputItem
			if item == nil {
				itemTyped = &outputItem{
					Key:                  key,
					Values:               record.Values,
					Count:                0,
					DirectionMultipliers: o.directionMultipliers,
				}
			} else {
				var ok bool
				itemTyped, ok = item.(*outputItem)
				if !ok {
					panic(fmt.Sprintf("invalid order by item: %v", item))
				}
			}
			if !record.Retraction {
				itemTyped.Count++
			} else {
				itemTyped.Count--
			}
			if itemTyped.Count < 0 {
				panic("received retraction before value")
			}
			if itemTyped.Count > 0 {
				recordCounts.ReplaceOrInsert(itemTyped)
			} else {
				recordCounts.Delete(itemTyped)
			}
			if onlyZeroEventTimesSeen && !record.EventTime.IsZero() {
				onlyZeroEventTimesSeen = false
			}
			if o.limit != nil && o.noRetractionsPossible && int64(recordCounts.Len()) > *o.limit {
				// This doesn't mean we'll always keep just the records that are needed, because tree nodes might have count > 1.
				// That said, it's a good approximation, and we'll definitely not lose something that we need to have.
				recordCounts.DeleteMax()
			}
			if o.live && onlyZeroEventTimesSeen && time.Since(lastUpdate) > time.Second/4 && !record.Retraction /*This last bit just makes the output less jittery*/ {
				printTable()
			}
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error {
			watermark = msg.Watermark

			// Print table
			if o.live && time.Since(lastUpdate) > time.Second/4 {
				printTable()
			}
			return nil
		},
	); err != nil {
		return err
	}

	var buf bytes.Buffer
	format := o.format(&buf)
	format.SetSchema(o.schema)
	i := int64(0)
	recordCounts.Ascend(func(item btree.Item) bool {
		itemTyped := item.(*outputItem)
		for j := 0; j < itemTyped.Count; j++ {
			if o.limit != nil && i == *o.limit {
				return false
			}
			i++

			format.Write(itemTyped.Values)
		}
		return true
	})
	format.Close()
	buf.WriteTo(liveWriter)
	liveWriter.Flush()

	return nil
}
