package deployer_whitelist

import (
	"github.com/gogo/protobuf/proto"
	loom "github.com/loomnetwork/go-loom"
	impressiontypes "github.com/loomnetwork/go-loom/builtin/types/impressiontracker"
	"github.com/loomnetwork/go-loom/util"
	"github.com/pkg/errors"
)

type (	
	Impressiontracker = impressiontypes.Impressiontracker
)

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


//ALl the properties  corresponding to impression tracker gets stored in prefix kv store corresponding to user addresses


const (
	ownerRole      = "owner"
	ImpressionPrefix = "impression"
)

var (
	modifyPerm = []byte("modp")
)

func impressiontrackerKey(addr loom.Address) []byte {
	return util.PrefixKey([]byte(ImpressionPrefix), addr.Bytes())
}

type ImpressionTracker struct {
}

func (cl *ImpressionTracker) Meta() (plugin.Meta, error) {
	return plugin.Meta{
		Name:    "impressiontracker",
		Version: "1.0.0",
	}, nil
}

func (cl *ImpressionTracker) AddImpressionTracker(ctx contract.Context, req *AddImpressionTrackerRequest) error {
	trackerOperations, err := ctx.Resolve("trackeroperations")
	if err != nil {
		return errors.Wrap(err, "unable to resolve trackeroperations contract")
	}

	if ctx.Message().Sender.Compare(trackerOperations) != 0 {
		return ErrNotAuthorized
	}

	if req.AddImpressionTrackerRequest == nil {
		return ErrInvalidRequest
	}

	userAddr := loom.UnmarshalAddressPB(req.userAddress)

	return ctx.Set(ImpressionTrackerKey(userAddr), req.impressiontracker)
}

var Contract plugin.Contract = contract.MakePluginContract(&ImpressionTracker{})