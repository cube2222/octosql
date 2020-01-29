package source

import "github.com/apache/thrift/lib/go/thrift"

// Thrift Error wrapper
type ThriftClientError struct {
	description string
	contextDescription string
}

// Set the context description of an error
func (err *ThriftClientError) WithContextDescription(description string) {
	err.contextDescription = description
}

// Stringify error
func (err *ThriftClientError) Error() string {
	if len(err.contextDescription) > 0 {
		return err.contextDescription + " is " + err.description
	}
	return err.description
}

// Create empty error
func ThriftClientNewError(description string) *ThriftClientError {
	return &ThriftClientError{
		description: description,
		contextDescription: "",
	}
}

// Wrap standard go error into Thrift error
// If the error is instance of thrift.TTransportException then additional description is injeted
// In other cases the error is wrapped "as is".
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
