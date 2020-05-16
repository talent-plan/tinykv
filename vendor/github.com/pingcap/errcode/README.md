# errcode

This package extends go errors via interfaces to have error codes.
The goal is that clients can reliably understand errors by checking against error codes

``` go
// First define a normal error type
type PathBlocked struct {
	start     uint64 `json:"start"`
	end       uint64 `json:"end"`
	obstacle  uint64 `json:"end"`
}

func (e PathBlocked) Error() string {
	return fmt.Sprintf("The path %d -> %d has obstacle %d", e.start, e.end, e.obstacle)
}

// Define a code. These often get re-used between different errors
// Note that codes use a hierarchy to share metadata
var PathBlockedCode = errcode.StateCode.Child("state.blocked")

// Now attach the code to your custom error type
func (e PathBlocked) Code() Code {
	return PathBlockedCode
}

// Given just a type of error, give an error code to a client if it is present
if errCode := errcode.CodeChain(err); errCode != nil {
	w.Header().Set("X-Error-Code", errCode.Code().CodeStr().String())
	rd.JSON(w, errCode.Code().HTTPCode(), errcode.NewJSONFormat(errCode))
}
```

There are other packages that add capabilities including error codes to errors.
There is a fundamental difference in architectural decision that this package makes.
Other packages use a single error struct. A code or other annotation is a field on that struct.
errcode follows the model of pkg/errors to use wrapping and interfaces.
You are encouraged to make your own error structures and then fulfill the ErrorCode interface by adding a function.
Additional features (for example annotating the operation) are done via wrapping.

This package integrates and relies on pkg/errors (actually the pingcap/errors enhancement of it).

See the [go docs](https://godoc.org/github.com/pingcap/errcode) for extensive API documentation.


## About Error codes

Error codes are particularly useful to reliably communicate the error type across program (network) boundaries.
A program can reliably respond to an error if it can be sure to understand its type.
