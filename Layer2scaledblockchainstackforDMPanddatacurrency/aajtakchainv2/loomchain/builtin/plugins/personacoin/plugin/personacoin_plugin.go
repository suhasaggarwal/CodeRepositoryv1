package main

import (
	"github.com/loomnetwork/go-loom/plugin"
)

var Contract = personacoin.Contract

func main() {
	plugin.Serve(Contract)
}
