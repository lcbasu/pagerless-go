module github.com/lcbasu/pagerless-go/echov5

go 1.25.0

require (
	github.com/labstack/echo/v5 v5.0.0
	github.com/lcbasu/pagerless-go v0.2.0
)

require golang.org/x/time v0.14.0 // indirect

// Local development only: consumers resolve the published core version (Go ignores a
// dependency's replace directives).
replace github.com/lcbasu/pagerless-go => ../
