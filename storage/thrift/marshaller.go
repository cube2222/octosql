//
//
//
//
//
package thrift

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage/thrift/analyzer"
	"strconv"
	"time"
)

// Value that is unmarshalled when receiving Thrift data
// It stores additional metadata as Thrift have Read/Write interface that do not allow us
// to pass additional parameters when reading objects
type CallResult struct {
	// Fields will store the deserialized values after Read() call occurs
	Fields map[octosql.VariableName]octosql.Value
	Alias string
	RecordStream *RecordStream
	ThriftMeta *analyzer.ThriftMeta
	// Name of the method of the Thrift service we are calling
	CalledMethodName string
}

// Create empty CallResult object
func EmptyCallResult(rs *RecordStream, calledMethodName string) CallResult {
	return CallResult{
		Fields: map[octosql.VariableName]octosql.Value{},
		Alias:  rs.source.alias,
		RecordStream: rs,
		ThriftMeta: rs.thriftMeta,
		CalledMethodName: calledMethodName,
	}
}

// Function to normalize Thrift values when unmarshalling
func NormalizeEntry(v interface{}) (interface{}) {
	if str, ok := v.(string); ok {
		parsed, err := time.Parse(time.RFC3339, str)
		if err == nil {
			v = parsed
		}
	}
	return v
}

// Thrift interface to let the client deserialize CallResult object
func (p *CallResult) Read(iprot thrift.TProtocol) error {
	view := analyzer.ThriftViewCalledMethod(p.ThriftMeta, p.CalledMethodName)
	return p.ReadEx(iprot, true, true, "", view)
}

// Recursively parses the nulled optional field to create empty columns for that field
// For example if you have:
// struct Record {
//    1: string a,
//    3: optional Bar bar,
//    4: required Foo foo,
//    5: list<string> lst,
// }
//
// and:
// struct Bar {
//    1: string barString,
// }
//
// And then you will nullify bar field inside Record struct you will receive table.bar column with <null>
// Instead of that you should receive table.bar.barstring column with <null>
//
func (p *CallResult) unmarshallFieldAsNulled(fieldName string, currentPrefix string, view *analyzer.ThriftMetaView) {
	fieldsIds, hasFields := view.GetAllFieldsIds()
	if !hasFields {
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(nil)
	} else {
		for _, subfieldId := range fieldsIds {
			subFieldName, recPrefix := p.getNameForField(int(subfieldId), subfieldId, currentPrefix, view, nil, true)
			var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
			p2.unmarshallFieldAsNulled(subFieldName, recPrefix, view.ViewField(subfieldId))
			for k2, v2 := range p2.Fields {
				p.Fields[k2] = octosql.NormalizeType(v2)
			}
		}
	}
}

// Tries to determine name of the field using its id and metadata (if it's available)
func (p *CallResult) getNameForField(fieldNo int, fieldId int16, currentPrefix string, view *analyzer.ThriftMetaView, overrideFieldName *string, isRoot bool) (string, string) {
	defaultFieldName := "field_" + strconv.Itoa(fieldNo)
	if fieldId > 0 {
		defaultFieldName = "field_" + strconv.Itoa(int(fieldId))
	}
	fieldName := ""
	recPrefix := ""

	if len(p.CalledMethodName) > 0 {
		fieldName = view.GetFieldName(fieldId)
		recPrefix = fmt.Sprintf("%s%s.", currentPrefix, fieldName)
	}

	if len(fieldName) == 0 {
		fieldName = defaultFieldName
		if isRoot {
			recPrefix = currentPrefix
		} else {
			recPrefix = fmt.Sprintf("%s%s.", currentPrefix, defaultFieldName)
		}
	}

	if overrideFieldName != nil {
		fieldName = *overrideFieldName
		recPrefix = currentPrefix
	}

	fieldName = currentPrefix + fieldName

	return fieldName, recPrefix
}

// Function to unmarshall single structure field
func (p *CallResult) unmarshallField(iprot thrift.TProtocol, view *analyzer.ThriftMetaView, fieldNo int, fieldId int16, fieldTypeId thrift.TType, currentPrefix string, overrideFieldName *string, isRoot bool) (error, bool) {
	// There is nothing left, we stop
	if fieldTypeId == thrift.STOP {
		return nil, false
	}

	fieldName, recPrefix := p.getNameForField(fieldNo, fieldId, currentPrefix, view, overrideFieldName, isRoot)
	switch fieldTypeId {
	// Each lsit item gets its own column
	case thrift.LIST:
		listType, listSize, err := iprot.ReadListBegin()
		if err != nil {
			return err, false
		}
		var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
		for itemNo := 0; itemNo < listSize; itemNo++ {
			overridenName := "item_" + strconv.Itoa(itemNo)
			err, cont := p2.unmarshallField(iprot, view, itemNo, int16(itemNo+1), listType, fieldName + ".", &overridenName, false)
			if err != nil {
				return err, true
			}
			if !cont {
				return nil, false
			}
			if err := iprot.ReadFieldEnd(); err != nil {
				return err, true
			}
		}
		listItemNo := 0
		for k2, v2 := range p2.Fields {
			p.Fields[k2] = octosql.NormalizeType(v2)
			listItemNo++
		}
		err = iprot.ReadListEnd()
		if err != nil {
			return err, false
		}
	// Void can be just a void or we executed the function with void type to signal that
	// the optional field is ommited and we require to fill the missing fields with nulls
	// (see unmarshallFieldAsNulled)
	case thrift.VOID:
		// Return null filled struct
		var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
		p2.unmarshallFieldAsNulled(fieldName, currentPrefix, view.ViewField(fieldId))
		for k2, v2 := range p2.Fields {
			p.Fields[k2] = octosql.NormalizeType(v2)
		}
	// Recursively parsing nested structs (we flatten them)
	case thrift.STRUCT:
		var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
		err := p2.ReadEx(iprot, false, false, recPrefix, view.ViewField(fieldId))
		if err != nil {
			return err, false
		}
		for k2, v2 := range p2.Fields {
			p.Fields[k2] = octosql.NormalizeType(v2)
		}
	//
	// Basic Thrift data types
	//
	case thrift.BOOL:
		val, err := iprot.ReadBool()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
	case thrift.BYTE:
		val, err := iprot.ReadByte()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
	case thrift.DOUBLE:
		val, err := iprot.ReadDouble()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
	case thrift.I16:
		val, err := iprot.ReadI16()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(int32(val)))
	case thrift.I32:
		val, err := iprot.ReadI32()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
	case thrift.I64:
		val, err := iprot.ReadI64()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
	case thrift.UTF8:
	case thrift.UTF16:
	case thrift.STRING:
		val, err := iprot.ReadString()
		if err != nil {
			return err, true
		}
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(NormalizeEntry(val))
	// We found a field that cannot be deserialized
	default:
		// TODO: Think if it's good to panic or not in that case?
		// panic("Found type that is not supported by a Thrift datasource: " + strconv.Itoa(int(fieldTypeId)))
		if err := iprot.Skip(fieldTypeId); err != nil {
			return err, false
		}
		return nil, false
	}

	return nil, true
}

// Function to deserialize Thrift values from input of the transport layer
func (p *CallResult) ReadEx(iprot thrift.TProtocol, addAliasPrefix bool, isRoot bool, currentPrefix string, view *analyzer.ThriftMetaView) error {

	if isRoot && addAliasPrefix {
		currentPrefix = fmt.Sprintf("%s%s.", currentPrefix, p.Alias)
	}

	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	i := 0
	var lastFieldId int16 = 0
	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}

		err, cont := p.unmarshallField(iprot, view, i, fieldId, fieldTypeId, currentPrefix, nil, isRoot)
		if err != nil {
			return err
		}
		if !cont {
			break
		}

		if fieldId != 0 && fieldId != lastFieldId + 1 {
			// Missing fields detected
			for missingFieldId := lastFieldId + 1; missingFieldId < fieldId; missingFieldId++ {
				if view.HasFieldID(missingFieldId) {
					err, cont := p.unmarshallField(iprot, view, i, missingFieldId, thrift.VOID, currentPrefix, nil, isRoot)
					if err != nil {
						return err
					}
					if !cont {
						break
					}
				}
			}
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}

		lastFieldId = fieldId
		i++
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

// This function implements Write method for serializable Thrift interfaces
func (p *CallResult) Write(oprot thrift.TProtocol) error {
	// TODO: Do something better here
	// This function should be never called because we receive Thrift objects not send them
	panic("Thrift Write() operation is not implemented for a database records.")
	return nil
}

// This function implements Read method for serializable Thrift interfaces
func (p *CallArgs) Read(iprot thrift.TProtocol) error {
	// TODO: Do something better here
	// This function should be never called because we serialize CallArgs object to
	// pass the parameters into Thrift method call and we don't need to deserialize it
	panic("Thrift Read() operation is not implemented for a call arguments object.")
	return nil
}

// This function implements Write method for serializable Thrift interfaces
func (p *CallArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("ping_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
	if p != nil {
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err) }
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err) }
	return nil
}