package thrift

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage/thrift/analyzer"
	"strconv"
)

type CallResult struct {
	Fields map[octosql.VariableName]octosql.Value
	Alias string
	RecordStream *RecordStream
	ThriftMeta *analyzer.ThriftMeta
	CalledMethodName string
}

func EmptyCallResult(rs *RecordStream, calledMethodName string) CallResult {
	return CallResult{
		Fields: map[octosql.VariableName]octosql.Value{},
		Alias:  rs.source.alias,
		RecordStream: rs,
		ThriftMeta: rs.thriftMeta,
		CalledMethodName: calledMethodName,
	}
}

func (p *CallResult) Read(iprot thrift.TProtocol) error {
	view := analyzer.ThriftViewCalledMethod(p.ThriftMeta, p.CalledMethodName)
	return p.ReadEx(iprot, true, true, "", view)
}

func (p *CallResult) unmarshallFieldAsNulled(fieldName string, currentPrefix string, view *analyzer.ThriftMetaView) {
	// Return null filled struct
	fmt.Printf("nullify %v\n", fieldName)
	fieldsIds, hasFields := view.GetAllFieldsIds()
	if !hasFields {
		p.Fields[octosql.NewVariableName(fieldName)] = octosql.NormalizeType(nil)
	} else {
		for _, subfieldId := range fieldsIds {
			k := "field_" + strconv.Itoa(int(subfieldId))
			subFieldName := ""
			recPrefix := ""
			if len(p.CalledMethodName) > 0 {
				subFieldName = view.GetFieldName(subfieldId)
				recPrefix = fmt.Sprintf("%s%s.",currentPrefix, subFieldName)
			}

			if len(subFieldName) == 0 {
				subFieldName = k
				recPrefix = currentPrefix
			}

			subFieldName = currentPrefix + subFieldName
			var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
			p2.unmarshallFieldAsNulled(subFieldName, recPrefix, view.ViewField(subfieldId))
			for k2, v2 := range p2.Fields {
				p.Fields[k2] = octosql.NormalizeType(v2)
			}
		}
	}
}

func (p *CallResult) unmarshallField(iprot thrift.TProtocol, view *analyzer.ThriftMetaView, fieldNo int, fieldId int16, fieldTypeId thrift.TType, currentPrefix string, overrideFieldName *string) (error, bool) {
	if fieldTypeId == thrift.STOP {
		return nil, false
	}

	k := "field_" + strconv.Itoa(fieldNo)
	fieldName := ""
	recPrefix := ""

	if len(p.CalledMethodName) > 0 {
		fieldName = view.GetFieldName(fieldId)
		recPrefix = fmt.Sprintf("%s%s.",currentPrefix, fieldName)
	}

	if len(fieldName) == 0 {
		fieldName = k
		recPrefix = currentPrefix
	}

	if overrideFieldName != nil {
		fieldName = *overrideFieldName
		recPrefix = currentPrefix
	}

	fieldName = currentPrefix + fieldName

	switch fieldTypeId {
	case thrift.LIST:
		listType, listSize, err := iprot.ReadListBegin()
		if err != nil {
			return err, false
		}
		var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
		for itemNo := 0; itemNo < listSize; itemNo++ {
			overridenName := "item_" + strconv.Itoa(itemNo)
			err, cont := p2.unmarshallField(iprot, view, itemNo, int16(itemNo+1), listType, fieldName + ".", &overridenName)
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
		iprot.ReadListEnd()
	case thrift.VOID:
		// Return null filled struct
		var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
		p2.unmarshallFieldAsNulled(fieldName, currentPrefix, view.ViewField(fieldId))
		for k2, v2 := range p2.Fields {
			p.Fields[k2] = octosql.NormalizeType(v2)
		}
	case thrift.STRUCT:
		var p2 CallResult = EmptyCallResult(p.RecordStream, p.CalledMethodName)
		p2.ReadEx(iprot, false, false, recPrefix, view.ViewField(fieldId))
		for k2, v2 := range p2.Fields {
			p.Fields[k2] = octosql.NormalizeType(v2)
		}
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
	default:
		fmt.Printf("[%v]\n", fieldTypeId)
		panic("HUJ!")
		if err := iprot.Skip(fieldTypeId); err != nil {
			return err, false
		}
		return nil, false
	}

	return nil, true
}

func (p *CallResult) ReadEx(iprot thrift.TProtocol, addAliasPrefix bool, isRoot bool, currentPrefix string, view *analyzer.ThriftMetaView) error {

	fmt.Printf("current pos := [%v]\n\n", view.DescribeCurrentPosition())

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

		err, cont := p.unmarshallField(iprot, view, i, fieldId, fieldTypeId, currentPrefix, nil)
		if err != nil {
			return err
		}
		if !cont {
			break
		}

		if fieldId != 0 && fieldId != lastFieldId + 1 {
			// Missing fields detected
			for missingFieldId := lastFieldId + 1; missingFieldId < fieldId; missingFieldId++ {
				err, cont := p.unmarshallField(iprot, view, i, missingFieldId, thrift.VOID, currentPrefix, nil)
				if err != nil {
					return err
				}
				if !cont {
					break
				}
			}
		}

		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}

		lastFieldId = fieldId
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *CallResult) Write(oprot thrift.TProtocol) error {
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


func (p *CallArgs) Read(iprot thrift.TProtocol) error {

	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP { break; }
		if err := iprot.Skip(fieldTypeId); err != nil {
			return err
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

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