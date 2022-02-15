package main

import (
	"github.com/loomnetwork/go-loom/plugin"
	"github.com/loomnetwork/loomchain/builtin/plugins/coin"
)

var Contract = aajtakcoin.Contract

func main() {
	plugin.Serve(Contract)
}
