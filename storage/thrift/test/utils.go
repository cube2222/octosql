package test

import (
	"bufio"
	"bytes"
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	thrifter "github.com/thrift-iterator/go"
	"github.com/thrift-iterator/go/general"
	"github.com/thrift-iterator/go/protocol"
	"io"
)

// Structure to simplify creation of fixed data responses
type SimpleOctosqlData struct {
	headers     []string
	recordsData [][]interface{}
}

// Test case specification
type ThriftTestCase struct {
	name        string
	ip          string
	protocol    string
	secure      bool
	alias       string
	want        SimpleOctosqlData
	thriftMeta  string
	data        []general.Struct
}

func (testCase ThriftTestCase) TestCaseName() string {
	return "Test " + testCase.name
}

func (testCase ThriftTestCase) WantedOutput() execution.RecordStream {
	return SimpleOctosqlDataToStream(testCase.want)
}

// Create binary Thrift response for openRecords() call
func createThriftAnswerOpenRecords(ctx context.Context, output []byte) (error, []byte) {
	serverOutputStreamBuf := bytes.NewBuffer(make([]byte, 27, 27))
	serverOutputStream := bufio.NewWriter(serverOutputStreamBuf)

	serverTransport := thrift.NewStreamTransportW(serverOutputStream)
	oprot := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(serverTransport)

	err := oprot.WriteMessageBegin("openRecords", thrift.REPLY, 1);
	if err != nil {
		return err, nil
	}
	err = oprot.WriteStructBegin("openRecords_result")
	if err != nil {
		return err, nil
	}
	err = oprot.WriteFieldBegin("success", thrift.I32, 0)
	if err != nil {
		return err, nil
	}
	err = oprot.WriteI32(24)
	if err != nil {
		return err, nil
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		panic(err)
	}
	err = oprot.WriteFieldStop()
	if err != nil {
		return err, nil
	}
	err = oprot.WriteStructEnd()
	if err != nil {
		return err, nil
	}
	err = oprot.WriteMessageEnd()
	if err != nil {
		return err, nil
	}
	err = oprot.Flush(ctx)
	if err != nil {
		return err, nil
	}
	err = serverTransport.Flush(ctx)
	if err != nil {
		return err, nil
	}
	err = serverOutputStream.Flush()
	if err != nil {
		return err, nil
	}

	binaryInput := serverOutputStreamBuf.Bytes()
	newLen := 31
	newBinaryinput := make([]byte, newLen)
	for i := 0; i < newLen; i++ {
		newBinaryinput[newLen-1-i] = binaryInput[len(binaryInput)-1-i]
	}
	return nil, append(output, newBinaryinput...)
}

// Creates fake response for getRecord()
func createThriftFakeRecordResponse(ctx context.Context, recordData general.Struct, output []byte, seqId int32) (error, []byte) {
	args := map[protocol.FieldId]interface{}{
		0: recordData,
	}

	v := general.Message{
		MessageHeader: protocol.MessageHeader{
			MessageName: "getRecord",
			MessageType: protocol.MessageTypeReply,
			SeqId:       protocol.SeqId(seqId),
		},
		Arguments: args,
	}
	thriftEncodedBytes, err := thrifter.MarshalMessage(v)
	if err != nil {
		return err, nil
	}
	return nil, append(output, thriftEncodedBytes...)
}

// Creates fake binary Thrift message that noramlly should be returned by a server
// This function is used to force Thrift to use the byte buffers instead of real HTTP connection
func CreateThriftFakeIO(ctx context.Context, records []general.Struct) (error, io.Reader, io.Writer) {

	output := []byte{}
	err, output := createThriftAnswerOpenRecords(ctx, output)
	if err != nil {
		return err, nil, nil
	}

	seqId := int32(2)
	for _, recordData := range records {
		err, output = createThriftFakeRecordResponse(ctx, recordData, output, seqId)
		if err != nil {
			return err, nil, nil
		}
		seqId++
	}

	err, output = createThriftFakeRecordResponse(ctx, nil, output, seqId)
	if err != nil {
		return err, nil, nil
	}

	outputStreamBuf := make([]byte, 1024, 1024)
	inputStream := bufio.NewReader(bytes.NewReader(output))
	outputStreamBuffer := bytes.NewBuffer(outputStreamBuf)
	outputStream := bufio.NewWriter(outputStreamBuffer)

	return nil, inputStream, outputStream
}

// Create a stream from the simple data object
func SimpleOctosqlDataToStream(data SimpleOctosqlData) *execution.InMemoryStream {
	records := []*execution.Record{}
	for _, recordDataRow := range data.recordsData {
		fields := []octosql.VariableName{}
		for _, headerColumnName := range data.headers {
			fields = append(fields, octosql.VariableName(headerColumnName))
		}
		record := execution.NewRecordFromSliceWithNormalize(fields, recordDataRow)
		records = append(records, record)
	}
	return execution.NewInMemoryStream(records)
}