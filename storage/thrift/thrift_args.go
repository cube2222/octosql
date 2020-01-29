package thrift

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
)

// Thrift args passed to openRecords()
type CallArgs struct {}

// Thrift args passed to getRecord()
type GetRecordCallArgs struct {
	StreamID int32 `thrift:"streamID,1" db:"streamID" json:"streamID"`
}

func NewGetRecordCallArgs() *GetRecordCallArgs {
	return &GetRecordCallArgs{}
}

func (p *GetRecordCallArgs) GetStreamID() int32 {
	return p.StreamID
}

// Implementation of Read method for Thrift serializable interfaces for GetRecordCallArgs
func (p *GetRecordCallArgs) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP { break; }
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.I32 {
				if err := p.ReadField1(iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
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

// Helper to read GetRecordCallArgs first field
func (p *GetRecordCallArgs) ReadField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadI32(); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.StreamID = v
	}
	return nil
}

// Implementation of Write method for Thrift serializable interfaces for GetRecordCallArgs
func (p *GetRecordCallArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("getRecord_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
	if p != nil {
		if err := p.writeField1(oprot); err != nil { return err }
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err) }
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err) }
	return nil
}

// Helper to write GetRecordCallArgs first field
func (p *GetRecordCallArgs) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("streamID", thrift.I32, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:streamID: ", p), err) }
	if err := oprot.WriteI32(int32(p.StreamID)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.streamID (1) field write error: ", p), err) }
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:streamID: ", p), err) }
	return err
}

// Helper to get string representation of GetRecordCallArgs for debugging purposes
func (p *GetRecordCallArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("GetRecordCallArgs(%+v)", *p)
}


