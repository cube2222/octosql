package analyzer

import (
	"fmt"
	"github.com/samuel/go-thrift/parser"
	"strings"
)

type ThriftMeta struct {
	typesFieldsNames     map[int][]string
	structsIds           map[string]int
	methodsReturnTypes   map[string]int
	nextFreeStructID     int
}

func parseThriftSpecs(contents string) (*parser.Thrift, error) {
	parserInstance := &parser.Parser{}
	thrift, err := parserInstance.Parse(strings.NewReader(contents))
	return thrift, err
}

func (meta* ThriftMeta) GetFieldName(structID int, fieldID int) string {
	fmt.Printf("get struct %v field %v", structID, fieldID)
	fmt.Printf("1 -> %v\n", meta.typesFieldsNames)
	if fieldID >= len(meta.typesFieldsNames[structID]) {
		return ""
	}
	fmt.Printf("2 -> %v\n", meta.typesFieldsNames[structID])
	return meta.typesFieldsNames[structID][fieldID]
}

func (meta* ThriftMeta) GetMetaResultStructID(methodName string) int {
	return meta.methodsReturnTypes[methodName]
}

func (meta* ThriftMeta) analyzeStructRec(structDef *parser.Struct, thriftSpecs *parser.Thrift, currentID *int, fields *[]string) {
	fmt.Printf("parse %v\n", structDef.Name)
	*currentID++
	for _, field := range structDef.Fields {
		if fieldTypeStruct, ok := thriftSpecs.Structs[field.Type.Name]; ok {
			// This field is struct
			meta.analyzeStructRec(fieldTypeStruct, thriftSpecs, currentID, fields)
		} else {
			// This field is primitive or something else
			fmt.Printf("add field %v\n", field.Name)
			*fields = append(*fields, field.Name)
		}
	}
}

func (meta *ThriftMeta) analyzeStruct(structDef *parser.Struct, thriftSpecs *parser.Thrift) int {
	structID := meta.nextFreeStructID
	meta.nextFreeStructID = meta.nextFreeStructID+1

	meta.structsIds[structDef.Name] = structID
	fields := []string{}
	currentID := 0

	meta.analyzeStructRec(structDef, thriftSpecs, &currentID, &fields)
	meta.typesFieldsNames[structID] = fields
	return structID
}

func (meta *ThriftMeta) analyzeEmpty(name string, thriftSpecs *parser.Thrift) int {
	structID := meta.nextFreeStructID
	meta.nextFreeStructID = meta.nextFreeStructID+1

	meta.structsIds[name] = structID
	meta.typesFieldsNames[structID] = []string{}
	return structID
}

func (meta *ThriftMeta) Analyze(thriftSpecs *parser.Thrift) {
	for _, service := range thriftSpecs.Services {
		for _, method := range service.Methods {
			if method.ReturnType != nil {
				if returnTypeStruct, ok := thriftSpecs.Structs[method.ReturnType.Name]; ok {
					id := meta.analyzeStruct(returnTypeStruct, thriftSpecs)
					meta.methodsReturnTypes[method.Name] = id
				} else {
					// Return type is a struct that is not available in the Thrift sources
					id := meta.analyzeEmpty(method.ReturnType.Name, thriftSpecs)
					meta.methodsReturnTypes[method.Name] = id
				}
			} else {
				// Return type is void
				id := meta.analyzeEmpty("void", thriftSpecs)
				meta.methodsReturnTypes[method.Name] = id
			}
		}
	}
}

func AnalyzeThriftSpecs(content string) (error, *ThriftMeta) {
	thriftSpecs, err := parseThriftSpecs(content)
	meta := ThriftMeta{
		typesFieldsNames:     map[int][]string{},
		structsIds:           map[string]int{},
		methodsReturnTypes:   map[string]int{},
	}

	if err != nil {
		return err, nil
	}

	fmt.Printf("thrift parsed = %v\n\n", thriftSpecs)
	meta.Analyze(thriftSpecs)
	fmt.Printf("DEBUG jp2gmd here typie1234\n")
	fmt.Printf("methods = %v\n", meta.methodsReturnTypes)
	fmt.Printf("fields = %v\n", meta.typesFieldsNames)

	return nil, &meta
}