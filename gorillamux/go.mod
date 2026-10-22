module github.com/lcbasu/pagerless-go/gorillamux

go 1.24

require (
	github.com/gorilla/mux v1.8.1
	github.com/lcbasu/pagerless-go v0.2.0
)

// Local development only: consumers resolve the published core version (Go ignores a
// dependency's replace directives).
replace github.com/lcbasu/pagerless-go => ../
