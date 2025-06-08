module github.com/c12s/monoceros

go 1.24.2

require (
	github.com/c12s/hyparview v1.0.0
	github.com/c12s/plumtree v1.0.0
	github.com/caarlos0/env v3.5.0+incompatible
)

require github.com/stretchr/testify v1.10.0 // indirect

replace github.com/c12s/plumtree => ../plumtree

replace github.com/c12s/hyparview => ../hyparview
