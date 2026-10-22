module github.com/lcbasu/pagerless-go/grpc

go 1.24

require (
	github.com/lcbasu/pagerless-go v0.2.0
	google.golang.org/grpc v1.64.0
)

require (
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)

// Local development only: consumers resolve the published core version (Go ignores a
// dependency's replace directives).
replace github.com/lcbasu/pagerless-go => ../
