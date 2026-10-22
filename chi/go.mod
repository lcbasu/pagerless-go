module github.com/lcbasu/pagerless-go/chi

go 1.24

require (
	github.com/go-chi/chi/v5 v5.0.12
	github.com/lcbasu/pagerless-go v0.2.0
)

// Local development only: consumers resolve the published core version (Go ignores a
// dependency's replace directives).
replace github.com/lcbasu/pagerless-go => ../
