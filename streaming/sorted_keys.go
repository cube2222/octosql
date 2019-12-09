package streaming

import "github.com/cube2222/octosql"

//TODO: Should these (especially marshals) throw errors?

func SortedMarshal(v *octosql.Value) ([]byte, error) {
	switch v.GetType() {
	case octosql.TypeString:
		return SortedMarshalString(v.AsString())
	default: //TODO: add other types
		return nil, nil
	}
}

func SortedUnmarshal(b []byte, v *octosql.Value) error {
	switch v.GetType() {
	case octosql.TypeString:
		s, err := SortedUnmarshalString(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeString(s)
		return nil
	default: //TODO: add other types
		return nil
	}
}

func SortedMarshalString(s string) ([]byte, error) {
	return []byte(s), nil
}

func SortedUnmarshalString(b []byte) (string, error) {
	return string(b), nil
}
