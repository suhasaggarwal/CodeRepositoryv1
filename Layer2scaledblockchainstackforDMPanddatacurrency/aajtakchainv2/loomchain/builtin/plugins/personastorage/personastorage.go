package personastorage

import (
	"github.com/gogo/protobuf/proto"
	loom "github.com/loomnetwork/go-loom"
	personatypes "github.com/loomnetwork/go-loom/builtin/types/personastorage"
	"github.com/loomnetwork/go-loom/util"
	"github.com/pkg/errors"
)


//ALl the properties  corresponding to persona storage gets stored in prefix kv store corresponding to user addresses

type (	
	personastorage = personatypes.PersonaStorage
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

const (
	ownerRole      = "owner"
	personaPrefix = "personastorage"
)

var (
	modifyPerm = []byte("modp")
)

func personastorageKey(addr loom.Address) []byte {
	return util.PrefixKey([]byte(personaPrefix), addr.Bytes())
}

type PersonaStorage struct {
}

func (ps *PersonaStorage) Meta() (plugin.Meta, error) {
	return plugin.Meta{
		Name:    "personastorage",
		Version: "1.0.0",
	}, nil
}

func (ps *PersonaStorage) AddPersonaStorage(ctx contract.Context, req *AddPersonaStorageRequest) error {
	trackerOperations, err := ctx.Resolve("trackeroperations")
	if err != nil {
		return errors.Wrap(err, "unable to resolve trackeroperations contract")
	}

	if ctx.Message().Sender.Compare(trackerOperations) != 0 {
		return ErrNotAuthorized
	}

	if req.AddPersonaStorageRequest == nil {
		return ErrInvalidRequest
	}

	userAddr := loom.UnmarshalAddressPB(req.useraddress)

	return ctx.Set(personastorageKey(userAddr), req.personastorage)
}

var Contract plugin.Contract = contract.MakePluginContract(&PersonaStorage{})