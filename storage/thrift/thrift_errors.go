package thrift

import "github.com/apache/thrift/lib/go/thrift"

type ThriftClientError struct {
	description string
	contextDescription string
}

func (err *ThriftClientError) WithContextDescription(description string) {
	err.contextDescription = description
}

func (err *ThriftClientError) Error() string {
	if len(err.contextDescription) > 0 {
		return err.contextDescription + " is " + err.description
	}
	return err.description
}

func ThriftClientNewError(description string) *ThriftClientError {
	return &ThriftClientError{
		description: description,
		contextDescription: "",
	}
}

func ThriftWrapError(err error) *ThriftClientError {
	terr, ok := err.(thrift.TTransportException)
	if ok {
		switch terr.TypeId() {
		case thrift.NOT_OPEN:
			return &ThriftClientError{
				description: "unable to connect to the service: " + terr.Error(),
				contextDescription: "",
			}
		case thrift.ALREADY_OPEN:
			return &ThriftClientError{
				description: "already connected to the service: " + terr.Error(),
				contextDescription: "",
			}
		}
		return &ThriftClientError{
			description: terr.Error(),
			contextDescription: "",
		}
	} else {
		return &ThriftClientError{
			description: err.Error(),
			contextDescription: "",
		}
	}
}
