module github.com/c12s/monoceros

go 1.24.2

require (
	github.com/c12s/hyparview v1.0.0
	github.com/c12s/plumtree v1.0.0
	github.com/caarlos0/env v3.5.0+incompatible
	github.com/json-iterator/go v1.1.12
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.64.0
)

require (
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/c12s/plumtree => ../plumtree

replace github.com/c12s/hyparview => ../hyparview
