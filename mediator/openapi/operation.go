package openapi

// Operation is the documentation a request declares through
// Describe() openapi.Operation. Everything else about the operation comes
// from the registry: the path from Route(), the schemas from the struct, and
// the security scopes from Requires().
type Operation struct {
	// OperationID overrides the derived operationId, which is otherwise the
	// request name in lowerCamelCase. A collision with another operation
	// gets a numeric suffix like any derived identifier.
	OperationID string
	Summary     string
	Description string
	// Tags groups the operation in the document. Without tags the first
	// path segment after the prefix is used, for example "orders" or "rpc".
	Tags       []string
	Deprecated bool
	// Errors lists the additional error codes the handler documents, such as
	// mediator.CodeNotFound. Each becomes a response with the Problem schema.
	// A string that is not a mediator.Code is a Generate error.
	Errors []string
}

// Describer is the trait a request implements to document its operation.
// It is detected at generation time with RequestInfo.Implements and invoked
// on the zero value, so it must be a value-receiver method returning a
// constant.
type Describer interface{ Describe() Operation }
