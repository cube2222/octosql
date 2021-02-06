package physical

type Trigger struct {
	TriggerType TriggerType
	// Only one of the below may be non-null.
	CountingTrigger    *CountingTrigger
	EndOfStreamTrigger *EndOfStreamTrigger
	WatermarkTrigger   *WatermarkTrigger
	MultiTrigger       *MultiTrigger
}

type TriggerType int

const (
	TriggerTypeCounting TriggerType = iota
	TriggerTypeEndOfStream
	TriggerTypeWatermark
	TriggerTypeMulti
)

type CountingTrigger struct {
}

type EndOfStreamTrigger struct {
}

type WatermarkTrigger struct {
}

type MultiTrigger struct {
	Triggers []Trigger
}
