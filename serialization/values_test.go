package serialization

import (
	"math"
	"testing"
	"time"

	"github.com/cube2222/octosql"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		value octosql.Value
	}{
		{
			value: octosql.MakeObject(map[string]octosql.Value{
				"name":      octosql.MakeString("red"),
				"age":       octosql.MakeInt(3),
				"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
				"address": octosql.MakeObject(map[string]octosql.Value{
					"city":   octosql.MakeString("purple"),
					"street": octosql.MakeString("fuchsia"),
				}),
				"time":     octosql.MakeTime(time.Now()),
				"duration": octosql.MakeDuration(time.Hour * 3),
				"phantom":  octosql.MakePhantom(),
				"null":     octosql.MakeNull(),
				"bool":     octosql.MakeBool(true),
				"float":    octosql.MakeFloat(math.Pi),
			}),
		},
		{
			value: octosql.MakeObject(map[string]octosql.Value{
				"name":      octosql.MakeString("red"),
				"age":       octosql.MakeInt(3),
				"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
				"address": octosql.MakeObject(map[string]octosql.Value{
					"city":   octosql.MakeString("purple"),
					"street": octosql.MakeString("fuchsia"),
				}),
				"time":     octosql.MakeTime(time.Now()),
				"duration": octosql.MakeDuration(time.Hour * 3),
				"phantom":  octosql.MakePhantom(),
				"null":     octosql.MakeNull(),
				"bool":     octosql.MakeBool(true),
				"float":    octosql.MakeFloat(math.Pi),
				"nested": octosql.MakeObject(map[string]octosql.Value{
					"name":      octosql.MakeString("red"),
					"age":       octosql.MakeInt(3),
					"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
					"address": octosql.MakeObject(map[string]octosql.Value{
						"city":   octosql.MakeString("purple"),
						"street": octosql.MakeString("fuchsia"),
					}),
					"time":     octosql.MakeTime(time.Now()),
					"duration": octosql.MakeDuration(time.Hour * 3),
					"phantom":  octosql.MakePhantom(),
					"null":     octosql.MakeNull(),
					"bool":     octosql.MakeBool(true),
					"float":    octosql.MakeFloat(math.Pi),
					"nested": octosql.MakeObject(map[string]octosql.Value{
						"name":      octosql.MakeString("red"),
						"age":       octosql.MakeInt(3),
						"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
						"address": octosql.MakeObject(map[string]octosql.Value{
							"city":   octosql.MakeString("purple"),
							"street": octosql.MakeString("fuchsia"),
						}),
						"time":     octosql.MakeTime(time.Now()),
						"duration": octosql.MakeDuration(time.Hour * 3),
						"phantom":  octosql.MakePhantom(),
						"null":     octosql.MakeNull(),
						"bool":     octosql.MakeBool(true),
						"float":    octosql.MakeFloat(math.Pi),
						"nested": octosql.MakeObject(map[string]octosql.Value{
							"name":      octosql.MakeString("red"),
							"age":       octosql.MakeInt(3),
							"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
							"address": octosql.MakeObject(map[string]octosql.Value{
								"city":   octosql.MakeString("purple"),
								"street": octosql.MakeString("fuchsia"),
							}),
							"time":     octosql.MakeTime(time.Now()),
							"duration": octosql.MakeDuration(time.Hour * 3),
							"phantom":  octosql.MakePhantom(),
							"null":     octosql.MakeNull(),
							"bool":     octosql.MakeBool(true),
							"float":    octosql.MakeFloat(math.Pi),
							"nested": octosql.MakeObject(map[string]octosql.Value{
								"name":      octosql.MakeString("red"),
								"age":       octosql.MakeInt(3),
								"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
								"address": octosql.MakeObject(map[string]octosql.Value{
									"city":   octosql.MakeString("purple"),
									"street": octosql.MakeString("fuchsia"),
								}),
								"time":     octosql.MakeTime(time.Now()),
								"duration": octosql.MakeDuration(time.Hour * 3),
								"phantom":  octosql.MakePhantom(),
								"null":     octosql.MakeNull(),
								"bool":     octosql.MakeBool(true),
								"float":    octosql.MakeFloat(math.Pi),
								"nested": octosql.MakeObject(map[string]octosql.Value{
									"name":      octosql.MakeString("red"),
									"age":       octosql.MakeInt(3),
									"countries": octosql.MakeTuple([]octosql.Value{octosql.MakeString("blue"), octosql.MakeString("gren")}),
									"address": octosql.MakeObject(map[string]octosql.Value{
										"city":   octosql.MakeString("purple"),
										"street": octosql.MakeString("fuchsia"),
									}),
									"time":     octosql.MakeTime(time.Now()),
									"duration": octosql.MakeDuration(time.Hour * 3),
									"phantom":  octosql.MakePhantom(),
									"null":     octosql.MakeNull(),
									"bool":     octosql.MakeBool(true),
									"float":    octosql.MakeFloat(math.Pi),
								}),
							}),
						}),
					}),
				}),
			}),
		},
	}
	for _, tt := range tests {
		t.Run("serialization", func(t *testing.T) {
			data := Serialize(tt.value)
			value, err := Deserialize(data)
			if err != nil {
				t.Error("error deserializing proto: ", err)
			}

			if !octosql.AreEqual(value, tt.value) {
				t.Errorf("Serialize() = %v, want %v", value, tt.value)
			}
		})
	}
}
