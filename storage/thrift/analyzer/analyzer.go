package analyzer

import (
	"github.com/samuel/go-thrift/parser"
	"strings"
)

type ThriftMeta struct {
	thriftSpecs *parser.Thrift
}

type ThriftMetaView struct {
	meta *ThriftMeta
	currentStruct *string
	calledMethod string
}

func ThriftViewCalledMethod(meta *ThriftMeta, methodName string) (*ThriftMetaView) {
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

func (view *ThriftMetaView) GetFieldName(fieldId int16) string {
	if view.currentStruct == nil {
		return ""
	}
	if structDef, ok := view.meta.thriftSpecs.Structs[*view.currentStruct]; ok {
		if fieldId >= 1 && int(fieldId) <= len(structDef.Fields) {
			return structDef.Fields[fieldId-1].Name
		}
	}
	return ""
}

func (view *ThriftMetaView) DescribeCurrentPosition() string {
	if view == nil {
		return "<view is nil>"
	}
	if view.currentStruct != nil {
		return "called from: " + view.calledMethod + " to struct: " + *view.currentStruct
	}
	return "called from: " + view.calledMethod + " to unknown>"
}

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
		if fieldId >= 1 && int(fieldId) <= len(structDef.Fields) {
			newStructName := structDef.Fields[fieldId-1].Type.Name
			return &ThriftMetaView{
				meta:   view.meta,
				currentStruct: &newStructName,
				calledMethod: view.calledMethod,
			}
		}
	}
	return &ThriftMetaView{
		meta:          view.meta,
		currentStruct: nil,
		calledMethod: view.calledMethod,
	}
}

func parseThriftSpecs(contents string) (*parser.Thrift, error) {
	parserInstance := &parser.Parser{}
	thrift, err := parserInstance.Parse(strings.NewReader(contents))
	return thrift, err
}

func (meta *ThriftMeta) Analyze(thriftSpecs *parser.Thrift) {
	meta.thriftSpecs = thriftSpecs
}

func AnalyzeThriftSpecs(content string) (error, *ThriftMeta) {
	thriftSpecs, err := parseThriftSpecs(content)
	meta := ThriftMeta{
		thriftSpecs: nil,
	}

	if err != nil {
		return err, nil
	}

	meta.Analyze(thriftSpecs)
	return nil, &meta
}