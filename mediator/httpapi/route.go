package httpapi

// Route is the REST override a request declares through Route() httpapi.Route.
// Without it, commands are POST /rpc/{Name} and queries GET /rpc/{Name}.
type Route struct {
	// Method is the HTTP method, upper case. Required.
	Method string
	// Path is a net/http.ServeMux pattern such as "/orders/{orderId}".
	// Path parameters bind to fields tagged path:"orderId".
	Path string
	// Status overrides the success status. Zero means 200, or 204 for Void.
	Status int
}
