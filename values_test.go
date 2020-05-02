package octosql

import (
	"encoding/base32"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
)

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  Value
	}{
		{
			name: "normalize something complex",
			value: map[string]interface{}{
				"name": []byte("Jakub"),
				"age":  uint8(3),
				"city": map[string]interface{}{
					"name":       "warsaw",
					"population": float32(1700000),
				},
				"array": []interface{}{[]interface{}{float32(1), uint8(2), int64(3)}, true},
			},
			want: MakeObject(map[string]Value{
				"name": MakeString(base32.StdEncoding.EncodeToString([]byte("Jakub"))),
				"age":  MakeInt(3),
				"city": MakeObject(map[string]Value{
					"name":       MakeString("warsaw"),
					"population": MakeFloat(1700000.0),
				}),
				"array": MakeTuple([]Value{
					MakeTuple([]Value{
						MakeFloat(1),
						MakeInt(2),
						MakeInt(3),
					}),
					MakeBool(true),
				}),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeType(tt.value); !AreEqual(got, tt.want) {
				t.Errorf("NormalizeType() = %s, want %s", got.String(), tt.want.String())
			}
		})
	}
}

func TestAreEqual(t *testing.T) {
	type args struct {
		left  Value
		right Value
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "compare ints",
			args: args{
				left:  MakeInt(1),
				right: MakeInt(1),
			},
			want: true,
		},
		{
			name: "compare ints",
			args: args{
				left:  MakeInt(1),
				right: MakeInt(2),
			},
			want: false,
		},
		{
			name: "compare times",
			args: args{
				left:  MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
				right: MakeTime(time.Date(2019, 03, 17, 20, 44, 16, 0, time.FixedZone("anything", 3600*5))),
			},
			want: true,
		},
		{
			name: "compare tuples",
			args: args{
				left: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
						MakeTuple(
							[]Value{
								MakeInt(1),
								MakeInt(2),
								MakeInt(3),
							},
						),
					},
				),
				right: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
						MakeTuple(
							[]Value{
								MakeInt(1),
								MakeInt(2),
								MakeInt(3),
							},
						),
					},
				),
			},
			want: true,
		},
		{
			name: "unequal tuples",
			args: args{
				left: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
					},
				),
				right: MakeTuple(
					[]Value{
						MakeInt(12),
						MakeString("something else"),
					},
				),
			},
			want: false,
		},
		{
			name: "unequal records",
			args: args{
				left: MakeTuple(
					[]Value{
						MakeInt(15),
					},
				),
				right: MakeTuple(
					[]Value{
						MakeInt(15),
						MakeString("something"),
					},
				),
			},
			want: false,
		},
		{
			name: "compare nil to non-nil",
			args: args{
				left:  ZeroValue(),
				right: MakeInt(7),
			},
			want: false,
		},
		{
			name: "compare non-nil to nil",
			args: args{
				left:  MakeInt(3),
				right: ZeroValue(),
			},
			want: false,
		},
		{
			name: "compare zero values",
			args: args{
				left:  ZeroValue(),
				right: ZeroValue(),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AreEqual(tt.args.left, tt.args.right); got != tt.want {
				t.Errorf("AreEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSerialize(t *testing.T) {
	tests := []struct {
		value Value
	}{
		{
			value: MakeObject(map[string]Value{
				"name":      MakeString("red"),
				"age":       MakeInt(3),
				"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
				"address": MakeObject(map[string]Value{
					"city":   MakeString("purple"),
					"street": MakeString("fuchsia"),
				}),
				"time":     MakeTime(time.Now()),
				"duration": MakeDuration(time.Hour * 3),
				"phantom":  MakePhantom(),
				"null":     MakeNull(),
				"bool":     MakeBool(true),
				"float":    MakeFloat(math.Pi),
			}),
		},
		{
			value: MakeObject(map[string]Value{
				"name":      MakeString("red"),
				"age":       MakeInt(3),
				"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
				"address": MakeObject(map[string]Value{
					"city":   MakeString("purple"),
					"street": MakeString("fuchsia"),
				}),
				"time":     MakeTime(time.Now()),
				"duration": MakeDuration(time.Hour * 3),
				"phantom":  MakePhantom(),
				"null":     MakeNull(),
				"bool":     MakeBool(true),
				"float":    MakeFloat(math.Pi),
				"nested": MakeObject(map[string]Value{
					"name":      MakeString("red"),
					"age":       MakeInt(3),
					"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
					"address": MakeObject(map[string]Value{
						"city":   MakeString("purple"),
						"street": MakeString("fuchsia"),
					}),
					"time":     MakeTime(time.Now()),
					"duration": MakeDuration(time.Hour * 3),
					"phantom":  MakePhantom(),
					"null":     MakeNull(),
					"bool":     MakeBool(true),
					"float":    MakeFloat(math.Pi),
					"nested": MakeObject(map[string]Value{
						"name":      MakeString("red"),
						"age":       MakeInt(3),
						"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
						"address": MakeObject(map[string]Value{
							"city":   MakeString("purple"),
							"street": MakeString("fuchsia"),
						}),
						"time":     MakeTime(time.Now()),
						"duration": MakeDuration(time.Hour * 3),
						"phantom":  MakePhantom(),
						"null":     MakeNull(),
						"bool":     MakeBool(true),
						"float":    MakeFloat(math.Pi),
						"nested": MakeObject(map[string]Value{
							"name":      MakeString("red"),
							"age":       MakeInt(3),
							"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
							"address": MakeObject(map[string]Value{
								"city":   MakeString("purple"),
								"street": MakeString("fuchsia"),
							}),
							"time":     MakeTime(time.Now()),
							"duration": MakeDuration(time.Hour * 3),
							"phantom":  MakePhantom(),
							"null":     MakeNull(),
							"bool":     MakeBool(true),
							"float":    MakeFloat(math.Pi),
							"nested": MakeObject(map[string]Value{
								"name":      MakeString("red"),
								"age":       MakeInt(3),
								"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
								"address": MakeObject(map[string]Value{
									"city":   MakeString("purple"),
									"street": MakeString("fuchsia"),
								}),
								"time":     MakeTime(time.Now()),
								"duration": MakeDuration(time.Hour * 3),
								"phantom":  MakePhantom(),
								"null":     MakeNull(),
								"bool":     MakeBool(true),
								"float":    MakeFloat(math.Pi),
								"nested": MakeObject(map[string]Value{
									"name":      MakeString("red"),
									"age":       MakeInt(3),
									"countries": MakeTuple([]Value{MakeString("blue"), MakeString("gren")}),
									"address": MakeObject(map[string]Value{
										"city":   MakeString("purple"),
										"street": MakeString("fuchsia"),
									}),
									"time":     MakeTime(time.Now()),
									"duration": MakeDuration(time.Hour * 3),
									"phantom":  MakePhantom(),
									"null":     MakeNull(),
									"bool":     MakeBool(true),
									"float":    MakeFloat(math.Pi),
								}),
							}),
						}),
					}),
				}),
			}),
		},
		{
			value: MakeObject(map[string]Value{
				"name": MakeString("red"),
				"age":  MakeInt(3),
				"countries": MakeTuple([]Value{
					MakeString("blue"),
					MakeString("gren"),
					MakeObject(map[string]Value{
						"name": MakeString("red"),
						"age":  MakeInt(3),
						"countries": MakeTuple([]Value{
							MakeString("blue"),
							MakeString("gren"),
							MakeObject(map[string]Value{
								"name": MakeString("red"),
								"age":  MakeInt(3),
								"countries": MakeTuple([]Value{
									MakeString("blue"),
									MakeString("gren"),
									MakeObject(map[string]Value{
										"name": MakeString("red"),
										"age":  MakeInt(3),
										"countries": MakeTuple([]Value{
											MakeString("blue"),
											MakeString("gren"),
											MakeObject(map[string]Value{
												"name": MakeString("red"),
												"age":  MakeInt(3),
												"countries": MakeTuple([]Value{
													MakeString("blue"),
													MakeString("gren"),
													MakeObject(map[string]Value{
														"name": MakeString("red"),
														"age":  MakeInt(3),
														"countries": MakeTuple([]Value{
															MakeString("blue"),
															MakeString("gren"),
														}),
														"address": MakeObject(map[string]Value{
															"city":   MakeString("purple"),
															"street": MakeString("fuchsia"),
														}),
														"time":     MakeTime(time.Now()),
														"duration": MakeDuration(time.Hour * 3),
														"phantom":  MakePhantom(),
														"null":     MakeNull(),
														"bool":     MakeBool(true),
														"float":    MakeFloat(math.Pi),
													}),
												}),
												"address": MakeObject(map[string]Value{
													"city":   MakeString("purple"),
													"street": MakeString("fuchsia"),
												}),
												"time":     MakeTime(time.Now()),
												"duration": MakeDuration(time.Hour * 3),
												"phantom":  MakePhantom(),
												"null":     MakeNull(),
												"bool":     MakeBool(true),
												"float":    MakeFloat(math.Pi),
											}),
										}),
										"address": MakeObject(map[string]Value{
											"city":   MakeString("purple"),
											"street": MakeString("fuchsia"),
										}),
										"time":     MakeTime(time.Now()),
										"duration": MakeDuration(time.Hour * 3),
										"phantom":  MakePhantom(),
										"null":     MakeNull(),
										"bool":     MakeBool(true),
										"float":    MakeFloat(math.Pi),
									}),
								}),
								"address": MakeObject(map[string]Value{
									"city":   MakeString("purple"),
									"street": MakeString("fuchsia"),
								}),
								"time":     MakeTime(time.Now()),
								"duration": MakeDuration(time.Hour * 3),
								"phantom":  MakePhantom(),
								"null":     MakeNull(),
								"bool":     MakeBool(true),
								"float":    MakeFloat(math.Pi),
							}),
						}),
						"address": MakeObject(map[string]Value{
							"city":   MakeString("purple"),
							"street": MakeString("fuchsia"),
						}),
						"time":     MakeTime(time.Now()),
						"duration": MakeDuration(time.Hour * 3),
						"phantom":  MakePhantom(),
						"null":     MakeNull(),
						"bool":     MakeBool(true),
						"float":    MakeFloat(math.Pi),
					}),
				}),
				"address": MakeObject(map[string]Value{
					"city":   MakeString("purple"),
					"street": MakeString("fuchsia"),
				}),
				"time":     MakeTime(time.Now()),
				"duration": MakeDuration(time.Hour * 3),
				"phantom":  MakePhantom(),
				"null":     MakeNull(),
				"bool":     MakeBool(true),
				"float":    MakeFloat(math.Pi),
			}),
		},
	}
	for _, tt := range tests {
		t.Run("serialization", func(t *testing.T) {
			data, err := proto.Marshal(&tt.value)
			if err != nil {
				t.Error("error serializing proto: ", err)
			}

			var value Value
			err = proto.Unmarshal(data, &value)
			if err != nil {
				t.Error("error deserializing proto: ", err)
			}

			if !AreEqual(value, tt.value) {
				t.Errorf("Serialize() = %v, want %v", value, tt.value)
			}
		})
	}
}
