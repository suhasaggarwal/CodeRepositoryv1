package coin

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	loom "github.com/loomnetwork/go-loom"
	"github.com/loomnetwork/go-loom/plugin"
	"github.com/loomnetwork/go-loom/plugin/contractpb"
	"github.com/loomnetwork/go-loom/types"
	"github.com/loomnetwork/loomchain"
)

var (
	addr1 = loom.MustParseAddress("chain:0xb16a379ec18d4093666f8f38b11a3071c920207d")
	addr2 = loom.MustParseAddress("chain:0xfa4c7920accfd66b86f5fd0e69682a79f762d49e")
	addr3 = loom.MustParseAddress("chain:0x5cecd1f7261e1f4c684e297be3edf03b825e01c4")
)

type mockAajtakCoinGateway struct {
}

//Digital Rupee

func (m *mockAajtakCoinGateway) Meta() (plugin.Meta, error) {
	return plugin.Meta{
		Name:    "loomcoin-gateway",
		Version: "0.1.0",
	}, nil           
	
}

func (m *mockAajtakCoinGateway) DummyMethod(ctx contractpb.Context, req *MintToGatewayRequest) error {
	return nil
}

func TestTransfer(t *testing.T) {
	ctx := contractpb.WrapPluginContext(
		plugin.CreateFakeContext(addr1, addr1),
	)

	amount := loom.NewBigUIntFromInt(100)
	contract := &AajtakCoin{}
	err := contract.Transfer(ctx, &TransferRequest{
		To:     addr2.MarshalPB(),
		Amount: &types.BigUInt{Value: *amount},
	})
	assert.NotNil(t, err)

	acct := &Account{
		Owner: addr1.MarshalPB(),
		Balance: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(100),
		},
	}
	err = saveAccount(ctx, acct)
	require.Nil(t, err)

	err = contract.Transfer(ctx, &TransferRequest{
		To:     addr2.MarshalPB(),
		Amount: &types.BigUInt{Value: *amount},
	})
	assert.Nil(t, err)

	resp, err := contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: addr1.MarshalPB(),
	})
	require.Nil(t, err)
	assert.Equal(t, 0, int(resp.Balance.Value.Int64()))

	resp, err = contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: addr2.MarshalPB(),
	})
	require.Nil(t, err)
	assert.Equal(t, 100, int(resp.Balance.Value.Int64()))
}

func sciNot(m, n int64) *loom.BigUInt {
	ret := loom.NewBigUIntFromInt(10)
	ret.Exp(ret, loom.NewBigUIntFromInt(n), nil)
	ret.Mul(ret, loom.NewBigUIntFromInt(m))
	return ret
}

// Verify Aajtak Coin.Transfer works correctly when the to & from addresses are the same.
func TestTransferToSelf(t *testing.T) {
	pctx := plugin.CreateFakeContext(addr1, addr1)
	// Test using the v1.1 contract, this test will fail if this feature is not enabled
	pctx.SetFeature(loomchain.AajtakCoinVersion1_1Feature, true)

	contract := &AajtakCoin{}
	err := contract.Init(
		contractpb.WrapPluginContext(pctx),
		&InitRequest{
			Accounts: []*InitialAccount{
				&InitialAccount{
					Owner:   addr2.MarshalPB(),
					Balance: uint64(100),
				},
			},
		},
	)
	require.NoError(t, err)

	amount := sciNot(100, 18)
	resp, err := contract.BalanceOf(
		contractpb.WrapPluginContext(pctx),
		&BalanceOfRequest{
			Owner: addr2.MarshalPB(),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, *amount, resp.Balance.Value)

	err = contract.Transfer(
		contractpb.WrapPluginContext(pctx.WithSender(addr2)),
		&TransferRequest{
			To:     addr2.MarshalPB(),
			Amount: &types.BigUInt{Value: *amount},
		},
	)
	assert.NoError(t, err)

	resp, err = contract.BalanceOf(
		contractpb.WrapPluginContext(pctx),
		&BalanceOfRequest{
			Owner: addr2.MarshalPB(),
		},
	)
	require.NoError(t, err)
	// the transfer was from addr2 to addr2 so the balance of addr2 should remain unchanged
	assert.Equal(t, *amount, resp.Balance.Value)
}

func TestApprove(t *testing.T) {
	contract := &AajtakCoin{}

	ctx := contractpb.WrapPluginContext(
		plugin.CreateFakeContext(addr1, addr1),
	)
	acct := &Account{
		Owner: addr1.MarshalPB(),
		Balance: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(100),
		},
	}
	err := saveAccount(ctx, acct)
	require.Nil(t, err)

	err = contract.Approve(ctx, &ApproveRequest{
		Spender: addr3.MarshalPB(),
		Amount: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(40),
		},
	})
	assert.Nil(t, err)

	allowResp, err := contract.Allowance(ctx, &AllowanceRequest{
		Owner:   addr1.MarshalPB(),
		Spender: addr3.MarshalPB(),
	})
	require.Nil(t, err)
	assert.Equal(t, 40, int(allowResp.Amount.Value.Int64()))
}

func TestTransferFrom(t *testing.T) {
	contract := &AajtakCoin{}

	pctx := plugin.CreateFakeContext(addr1, addr1)
	ctx := contractpb.WrapPluginContext(pctx)
	acct := &Account{
		Owner: addr1.MarshalPB(),
		Balance: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(100),
		},
	}
	err := saveAccount(ctx, acct)
	require.Nil(t, err)

	err = contract.Approve(ctx, &ApproveRequest{
		Spender: addr3.MarshalPB(),
		Amount: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(40),
		},
	})
	assert.Nil(t, err)

	ctx = contractpb.WrapPluginContext(pctx.WithSender(addr3))
	err = contract.TransferFrom(ctx, &TransferFromRequest{
		From: addr1.MarshalPB(),
		To:   addr2.MarshalPB(),
		Amount: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(50),
		},
	})
	assert.NotNil(t, err)

	err = contract.TransferFrom(ctx, &TransferFromRequest{
		From: addr1.MarshalPB(),
		To:   addr2.MarshalPB(),
		Amount: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(30),
		},
	})
	assert.Nil(t, err)

	allowResp, err := contract.Allowance(ctx, &AllowanceRequest{
		Owner:   addr1.MarshalPB(),
		Spender: addr3.MarshalPB(),
	})
	require.Nil(t, err)
	assert.Equal(t, 10, int(allowResp.Amount.Value.Int64()))

	balResp, err := contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: addr1.MarshalPB(),
	})
	require.Nil(t, err)
	assert.Equal(t, 70, int(balResp.Balance.Value.Int64()))

	balResp, err = contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: addr2.MarshalPB(),
	})
	require.Nil(t, err)
	assert.Equal(t, 30, int(balResp.Balance.Value.Int64()))
}

// Verify Aajtak Coin.Transfer From works correctly when the to & from addresses are the same.
func TestTransferFromSelf(t *testing.T) {
	pctx := plugin.CreateFakeContext(addr1, addr1)
	// Test using the v1.1 contract, this test will fail if this feature is not enabled
	pctx.SetFeature(loomchain.AajtakCoinVersion1_1Feature, true)

	contract := &AajtakCoin{}
	err := contract.Init(
		contractpb.WrapPluginContext(pctx),
		&InitRequest{
			Accounts: []*InitialAccount{
				&InitialAccount{
					Owner:   addr2.MarshalPB(),
					Balance: uint64(100),
				},
			},
		},
	)
	require.NoError(t, err)

	amount := sciNot(100, 18)
	resp, err := contract.BalanceOf(
		contractpb.WrapPluginContext(pctx),
		&BalanceOfRequest{
			Owner: addr2.MarshalPB(),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, *amount, resp.Balance.Value)

	err = contract.Approve(
		contractpb.WrapPluginContext(pctx.WithSender(addr2)),
		&ApproveRequest{
			Spender: addr2.MarshalPB(),
			Amount: &types.BigUInt{
				Value: *amount,
			},
		},
	)
	assert.NoError(t, err)

	err = contract.TransferFrom(
		contractpb.WrapPluginContext(pctx.WithSender(addr2)),
		&TransferFromRequest{
			From: addr2.MarshalPB(),
			To:   addr2.MarshalPB(),
			Amount: &types.BigUInt{
				Value: *amount,
			},
		},
	)
	assert.NoError(t, err)

	resp, err = contract.BalanceOf(
		contractpb.WrapPluginContext(pctx),
		&BalanceOfRequest{
			Owner: addr2.MarshalPB(),
		},
	)
	require.NoError(t, err)
	// the transfer was from addr2 to addr2 so the balance of addr2 should remain unchanged
	assert.Equal(t, *amount, resp.Balance.Value)
}

func TestMintToGateway(t *testing.T) {
	contract := &AajtakCoin{}

	mockAajtakCoinGatewayContract := contractpb.MakePluginContract(&mockAajtakCoinGateway{})

	pctx := plugin.CreateFakeContext(addr1, addr1)

	aajtakcoinTGAddress := pctx.CreateContract(mockAajtakCoinGatewayContract)
	pctx.RegisterContract("loomcoin-gateway", aajtakcoinTGAddress, aajtakcoinTGAddress)

	ctx := contractpb.WrapPluginContext(pctx)

	contract.Init(ctx, &InitRequest{
		Accounts: []*InitialAccount{
			&InitialAccount{
				Owner:   aajtakcoinTGAddress.MarshalPB(),
				Balance: uint64(29),
			},
			&InitialAccount{
				Owner:   addr1.MarshalPB(),
				Balance: uint64(31),
			},
		},
	})

	multiplier := big.NewInt(10).Exp(big.NewInt(10), big.NewInt(18), big.NewInt(0))
    aajtakcoinTGBalance := big.NewInt(0).Mul(multiplier, big.NewInt(29))
	addr1Balance := big.NewInt(0).Mul(multiplier, big.NewInt(31))
	totalSupply := big.NewInt(0).Add(aajtakcoinTGBalance, addr1Balance)

	totalSupplyResponse, err := contract.TotalSupply(ctx, &TotalSupplyRequest{})
	require.Nil(t, err)
	require.Equal(t, totalSupply, totalSupplyResponse.TotalSupply.Value.Int)

	gatewayBalanceResponse, err := contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: aajtakcoinTGAddress.MarshalPB(),
	})
	require.Nil(t, err)
	require.Equal(t, aajtakCoinTGBalance, gatewayBalanceResponse.Balance.Value.Int)

	require.Nil(t, contract.MintToGateway(
		contractpb.WrapPluginContext(pctx.WithSender(aajtakCoinTGAddress)),
		&MintToGatewayRequest{
			Amount: &types.BigUInt{
				Value: *loom.NewBigUIntFromInt(59),
			},
		},
	))

	newTotalSupply := big.NewInt(0).Add(totalSupply, big.NewInt(59))
	aajtakCoinTGBalance := big.NewInt(0).Add(aajtakCoinTGBalance, big.NewInt(59))

	totalSupplyResponse, err = contract.TotalSupply(ctx, &TotalSupplyRequest{})
	require.Nil(t, err)
	require.Equal(t, newTotalSupply, totalSupplyResponse.TotalSupply.Value.Int)

	gatewayBalanceResponse, err = contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: aajtakCoinTGAddress.MarshalPB(),
	})
	require.Nil(t, err)
	require.Equal(aajtakCoinTGBalance, gatewayBalanceResponse.Balance.Value.Int)
}

func TestBurn(t *testing.T) {
	contract := &AajtakCoin{}

	mockAajtakCoinGatewayContract := contractpb.MakePluginContract(&mockAajtakCoinGateway{})

	pctx := plugin.CreateFakeContext(addr1, addr1)

	aajtakcoinTGAddress := pctx.CreateContract(mockAajtakCoinGatewayContract)
	pctx.RegisterContract("loomcoin-gateway", aajtakcoinTGAddress, aajtakcoinTGAddress)

	ctx := contractpb.WrapPluginContext(pctx)

	contract.Init(ctx, &InitRequest{
		Accounts: []*InitialAccount{
			&InitialAccount{
				Owner:   addr2.MarshalPB(),
				Balance: uint64(29),
			},
			&InitialAccount{
				Owner:   addr1.MarshalPB(),
				Balance: uint64(31),
			},
		},
	})

	multiplier := big.NewInt(10).Exp(big.NewInt(10), big.NewInt(18), big.NewInt(0))
	addr2Balance := big.NewInt(0).Mul(multiplier, big.NewInt(29))
	addr1Balance := big.NewInt(0).Mul(multiplier, big.NewInt(31))
	totalSupply := big.NewInt(0).Add(addr2Balance, addr1Balance)

	totalSupplyResponse, err := contract.TotalSupply(ctx, &TotalSupplyRequest{})
	require.Nil(t, err)
	require.Equal(t, totalSupply, totalSupplyResponse.TotalSupply.Value.Int)

	addr2BalanceResponse, err := contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: addr2.MarshalPB(),
	})

	require.Nil(t, err)
	require.Equal(t, addr2Balance, addr2BalanceResponse.Balance.Value.Int)

	require.Nil(t, contract.Burn(
		contractpb.WrapPluginContext(pctx.WithSender(aajtakcoinTGAddress)),
		&BurnRequest{
			Owner: addr2.MarshalPB(),
			Amount: &types.BigUInt{
				Value: *loom.NewBigUIntFromInt(2),
			},
		},
	))

	newTotalSupply := big.NewInt(0).Sub(totalSupply, big.NewInt(2))
	newAddr2Balance := big.NewInt(0).Sub(addr2Balance, big.NewInt(2))

	totalSupplyResponse, err = contract.TotalSupply(ctx, &TotalSupplyRequest{})
	require.Nil(t, err)
	require.Equal(t, newTotalSupply, totalSupplyResponse.TotalSupply.Value.Int)

	addr2BalanceResponse, err = contract.BalanceOf(ctx, &BalanceOfRequest{
		Owner: addr2.MarshalPB(),
	})
	require.Nil(t, err)
	require.Equal(t, newAddr2Balance, addr2BalanceResponse.Balance.Value.Int)
}

func TestBurnAccess(t *testing.T) {
	contract := &AajtakCoin{}

	mockAajtakCoinGatewayContract := contractpb.MakePluginContract(&mockAajtakCoinGateway{})

	pctx := plugin.CreateFakeContext(addr1, addr1)

	aajtakcoinTGAddress := pctx.CreateContract(mockAajtakCoinGatewayContract)
	pctx.RegisterContract("loomcoin-gateway", aajtakcoinTGAddress, aajtakcoinTGAddress)

	ctx := contractpb.WrapPluginContext(pctx)

	contract.Init(ctx, &InitRequest{
		Accounts: []*InitialAccount{
			{
				Owner:   addr1.MarshalPB(),
				Balance: 100,
			},
			{
				Owner:   addr2.MarshalPB(),
				Balance: 0,
			},
		},
	})

	require.EqualError(t, contract.Burn(ctx, &BurnRequest{
		Owner: addr1.MarshalPB(),
		Amount: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(10),
		},
	}), "not authorized to burn Loom coin", "only loomcoin gateway can call Burn")

	require.Nil(t, contract.Burn(
		contractpb.WrapPluginContext(pctx.WithSendercoinTGAddress)),
		&BurnRequest{
			Owner: addr1.MarshalPB(),
			Amount: &types.BigUInt{
				Value: *loom.NewBigUIntFromInt(10),
			},
		},
	), "loomcoin gateway should be allowed to call Burn")

	require.EqualError(t, contract.Burn(
		contractpb.WrapPluginContext(pctx.WithSender(aajtakcoinTGAddress)),
		&BurnRequest{
			Owner: addr2.MarshalPB(),
			Amount: &types.BigUInt{
				Value: *loom.NewBigUIntFromInt(10),
			},
		},
	), "cant burn coins more than available balance: 0", "only burn coin owned by you")
}

func TestMintToGatewayAccess(t *testing.T) {
	contract := &Coin{}

	mockAajtakCoinGatewayContract := contractpb.MakePluginContract(&mockAajtakCoinGateway{})

	pctx := plugin.CreateFakeContext(addr1, addr1)

	aajtakcoinTGAddress := pctx.CreateContract(mockAajtakCoinGatewayContract)
	pctx.RegisterContract("loomcoin-gateway", loomcoinTGAddress, loomcoinTGAddress)

	ctx := contractpb.WrapPluginContext(pctx)

	require.EqualError(t, contract.MintToGateway(ctx, &MintToGatewayRequest{
		Amount: &types.BigUInt{
			Value: *loom.NewBigUIntFromInt(10),
		},
	}), "not authorized to mint Loom coin", "only loomcoin gateway can call MintToGateway")

	require.Nil(t, contract.MintToGateway(
		contractpb.WrapPluginContext(pctx.WithSender(loomcoinTGAddress)),
		&MintToGatewayRequest{
			Amount: &types.BigUInt{
				Value: *loom.NewBigUIntFromInt(10),
			},
		},
	), "loomcoin gateway should be allowed to call MintToGateway")

}
