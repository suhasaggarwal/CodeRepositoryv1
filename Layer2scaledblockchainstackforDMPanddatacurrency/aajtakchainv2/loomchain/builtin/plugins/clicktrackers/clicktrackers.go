package deployer_whitelist

import (
	"github.com/gogo/protobuf/proto"
	loom "github.com/loomnetwork/go-loom"
	clicktypes "github.com/loomnetwork/go-loom/builtin/types/clicktracker"
	"github.com/loomnetwork/go-loom/util"
	"github.com/pkg/errors"
)

type (	
	Clicktracker = clicktypes.Clicktracker
)


//ALl the properties corresponding to click tracker gets stored in prefix kv store corresponding to user addresses

const (
	// AllowEVMDeployFlag indicates that a deployer is permitted to deploy EVM contract.
	AllowEVMDeployFlag = dwtypes.Flags_EVM
	// AllowGoDeployFlag indicates that a deployer is permitted to deploy GO contract.
	AllowGoDeployFlag = dwtypes.Flags_GO
	// AllowMigrationFlag indicates that a deployer is permitted to migrate GO contract.
	AllowMigrationFlag = dwtypes.Flags_MIGRATION
)

var (
	// ErrrNotAuthorized indicates that a contract method failed because the caller didn't have
	// the permission to execute that method.
	ErrNotAuthorized = errors.New("[DeployerWhitelist] not authorized")
	// ErrInvalidRequest is a geperic error that's returned when something is wrong with the
	// request message, e.g. missing or invalid fields.
	ErrInvalidRequest = errors.New("[DeployerWhitelist] invalid request")
	// ErrOwnerNotSpecified returned if init request does not have owner address
	ErrOwnerNotSpecified = errors.New("[DeployerWhitelist] owner not specified"p

)

const (
	ownerRole      = "owner"
	ClickPrefix = "click"
)

var (
	modifyPerm = []byte("modp")
)

func clicktrackerKey(addr loom.Address) []byte {
	return util.PrefixKey([]byte(ClickPrefix), addr.Bytes())
}

type ClickTracker struct {
}

func (cl *ClickTracker) Meta() (plugin.Meta, error) {
	return plugin.Meta{
		Name:    "clicktracker",
		Version: "1.0.0",
	}, nil
}

func (cl *ClickTracker) AddClickTracker(ctx contract.Context, req *AddClickTrackerRequest) error {
	trackerOperations, err := ctx.Resolve("trackeroperations")
	if err != nil {
		return errors.Wrap(err, "unable to resolve user_deployer_whitelist contract")
	}

	if ctx.Message().Sender.Compare(trackerOperations) != 0 {
		return ErrNotAuthorized
	}

	if req.AddClickTrackerRequest == nil {
		return ErrInvalidRequest
	}

	userAddr := loom.UnmarshalAddressPB(req.userAddress)

	return ctx.Set(ClickTrackerKey(userAddr), req.clicktracker)
}

var Contract plugin.Contract = contract.MakePluginContract(&ClickTracker{})
