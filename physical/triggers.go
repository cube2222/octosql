package physical

type Trigger struct {
	TriggerType TriggerType
	// Only one of the below may be non-null.
	CountingTrigger  *CountingTrigger
	WatermarkTrigger *WatermarkTrigger
}

type TriggerType int

const (
	TriggerTypeCounting TriggerType = iota
	TriggerTypeWatermark
)

type CountingTrigger struct {
}

type WatermarkTrigger struct {
}
