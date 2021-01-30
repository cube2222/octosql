package execution

import (
	"time"
)

type Trigger interface {
	EndOfStreamReached()
	WatermarkReceived(watermark time.Time)
	KeyReceived(key GroupKey)
	Poll() []GroupKey
}
