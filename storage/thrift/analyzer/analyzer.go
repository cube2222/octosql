package analyzer

import (
	"fmt"
	"github.com/samuel/go-thrift/parser"
	"strings"
)

// Meta information used to deserialize Thrift payload
type ThriftMeta struct {
	thriftSpecs *parser.Thrift
}

// View used to traverse Thrift specification to extract information about fields
type ThriftMetaView struct {
	// Parsed thrift specs
	meta *ThriftMeta
	// Current structure
	currentStruct *string
	// Top level method for which we try to determine types and names of the result
	calledMethod string
}

// Returns the meta pointer that can traverse the AST of thrift specification
// This pointer starts in return type of the given method of some Thrift services
// and runs through the AST to determine metadata connected with deserialized Thrift payload
func ThriftViewCalledMethod(meta *ThriftMeta, methodName string) (*ThriftMetaView) {
	if meta == nil || meta.thriftSpecs == nil {
		return &ThriftMetaView{
			meta:          meta,
			currentStruct: nil,
			calledMethod:  methodName,
		}
	}
	for _, service := range meta.thriftSpecs.Services {
		for _, method := range service.Methods {
			if method.Name == methodName {
				structName := method.ReturnType.Name
				return &ThriftMetaView{
					meta:          meta,
					currentStruct: &structName,
					calledMethod:  method.Name,
				}
			}
		}
	}
	return nil
}

// Checks if the current structure has field with a given id
func (view *ThriftMetaView) HasFieldID(fieldId int16) bool {
	if view.currentStruct == nil {
		return false
	}
	if structDef, ok := view.meta.thriftSpecs.Structs[*view.currentStruct]; ok {
		for _, fieldDef := range structDef.Fields {
			if int16(fieldDef.ID) == fieldId {
				return true
			}
		}
	}
	return false
}

// Returns ids of all fields of a current structure
func (view *ThriftMetaView) GetAllFieldsIds() ([]int16, bool) {
	result := []int16{}
	if view.currentStruct == nil {
		return result, false
	}
	if structDef, ok := view.meta.thriftSpecs.Structs[*view.currentStruct]; ok {
		for _, fieldDef := range structDef.Fields {
			result = append(result, int16(fieldDef.ID))
		}
	} else {
		return result, false
	}
	return result, true
}

// Returns field name for a given field id for the current structure
func (view *ThriftMetaView) GetFieldName(fieldId int16) string {
	if view.currentStruct == nil {
		return ""
	}
	if structDef, ok := view.meta.thriftSpecs.Structs[*view.currentStruct]; ok {
		for _, fieldDef := range structDef.Fields {
			if fieldId == int16(fieldDef.ID) {
				return fieldDef.Name
			}
		}
	}
	return ""
}

// Get human readable information about current iterator position
func (view *ThriftMetaView) DescribeCurrentPosition() string {
	if view == nil {
		return "<view is nil>"
	}
	if view.currentStruct != nil {
		return "called from: " + view.calledMethod + " to struct: " + *view.currentStruct
	}
	return "called from: " + view.calledMethod + " to unknown>"
}

// Returns new iterator for field with given field id of the current structure
func (view *ThriftMetaView) ViewField(fieldId int16) (*ThriftMetaView) {
	if fieldId == 0 {
		return &ThriftMetaView{
			meta:          view.meta,
			currentStruct: view.currentStruct,
			calledMethod:  view.calledMethod,
		}
	}
	if view.currentStruct == nil {
		return &ThriftMetaView{
			meta:          view.meta,
			currentStruct: nil,
			calledMethod: view.calledMethod,
		}
	}
	if structDef, ok := view.meta.thriftSpecs.Structs[*view.currentStruct]; ok {
		for _, fieldDef := range structDef.Fields {
			if fieldId == int16(fieldDef.ID) {
				newStructName := fieldDef.Type.Name
				return &ThriftMetaView{
					meta:          view.meta,
					currentStruct: &newStructName,
					calledMethod:  view.calledMethod,
				}
			}
		}
	}
	fmt.Printf("cannot get field %v of %v!!! jp2gmd\n", fieldId, *view.currentStruct)
	return &ThriftMetaView{
		meta:          view.meta,
		currentStruct: nil,
		calledMethod: view.calledMethod,
	}
}

// Helper to run thrift specification parser
func parseThriftSpecs(contents string) (*parser.Thrift, error) {
	parserInstance := &parser.Parser{}
	thrift, err := parserInstance.Parse(strings.NewReader(contents))
	return thrift, err
}

// Returns new ThriftMeta information object for a given string containing valid
// Thrift specification
func AnalyzeThriftSpecs(content string) (error, *ThriftMeta) {
	thriftSpecs, err := parseThriftSpecs(content)
	meta := ThriftMeta{
		thriftSpecs: nil,
	}

	if err != nil {
		return err, nil
	}

	meta.thriftSpecs = thriftSpecs
	return nil, &meta
}