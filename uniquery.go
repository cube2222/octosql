package octosql

type Datatype string

const (
	DatatypeInt     Datatype = "int"
	DatatypeFloat32 Datatype = "float32"
	DatatypeFloat64 Datatype = "float64"
	DatatypeString  Datatype = "string"
)

func OneOf(datatype Datatype, tab []Datatype) bool {
	for _, dt := range tab {
		if datatype == dt {
			return true
		}
	}
	return false
}
