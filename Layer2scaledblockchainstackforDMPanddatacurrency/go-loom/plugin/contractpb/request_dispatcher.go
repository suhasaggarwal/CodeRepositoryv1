package contractpb

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/loomnetwork/go-loom/plugin"
	"github.com/loomnetwork/go-loom/plugin/types"
)

// RequestDispatcher dispatches Request(s) to contract methods.
// The dispatcher takes care of unmarshalling requests and marshalling responses from/to protobufs
// or JSON - based on the content type specified in the Request.ContentType/Accept fields.
type RequestDispatcher struct {
	Contract
	callbacks *serviceMap
}

func NewRequestDispatcher(contract Contract) (*RequestDispatcher, error) {
	s := &RequestDispatcher{
		Contract:  contract,
		callbacks: new(serviceMap),
	}
	err := s.callbacks.Register(contract, "contract")
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *RequestDispatcher) Init(ctx plugin.Context, req *plugin.Request) error {
	wrappedCtx := WrapPluginContext(ctx)
	body := bytes.NewBuffer(req.Body)
	unmarshaler, err := UnmarshalerFactory(req.ContentType)
	if err != nil {
		return err
	}

	serviceSpec, methodSpec, err := s.callbacks.Get("contract.Init")
	if err != nil {
		return err
	}

	if methodSpec.methodSig != methodSigInit {
		return errors.New("method call does not match method signature type")
	}

	queryParams := reflect.New(methodSpec.argsType)
	err = unmarshaler.Unmarshal(body, queryParams.Interface().(proto.Message))
	if err != nil {
		return err
	}

	resultTypes := methodSpec.method.Func.Call([]reflect.Value{
		serviceSpec.rcvr,
		reflect.ValueOf(wrappedCtx),
		queryParams,
	})

	err, _ = resultTypes[0].Interface().(error)
	return err
}

func (s *RequestDispatcher) StaticCall(ctx plugin.StaticContext, req *plugin.Request) (*plugin.Response, error) {
	return s.doCall(methodSigStaticCall, WrapPluginStaticContext(ctx), req)
}

func (s *RequestDispatcher) Call(ctx plugin.Context, req *plugin.Request) (*plugin.Response, error) {
	return s.doCall(methodSigCall, WrapPluginContext(ctx), req)
}

func (s *RequestDispatcher) doCall(sig methodSig, ctx interface{}, req *plugin.Request) (*plugin.Response, error) {
	body := bytes.NewBuffer(req.Body)
	unmarshaler, err := UnmarshalerFactory(req.ContentType)
	if err != nil {
		return nil, err
	}
	marshaler, err := MarshalerFactory(req.Accept)
	if err != nil {
		return nil, err
	}

	var query types.ContractMethodCall
	err = unmarshaler.Unmarshal(body, &query)
	if err != nil {
		return nil, err
	}

	serviceSpec, methodSpec, err := s.callbacks.Get("contract." + query.Method)
	if err != nil {
		return nil, err
	}

	if methodSpec.methodSig != sig {
		return nil, errors.New("method call does not match method signature type")
	}

	queryParams := reflect.New(methodSpec.argsType)
	err = unmarshaler.Unmarshal(bytes.NewBuffer(query.Args), queryParams.Interface().(proto.Message))
	if err != nil {
		return nil, err
	}

	resultTypes := methodSpec.method.Func.Call([]reflect.Value{
		serviceSpec.rcvr,
		reflect.ValueOf(ctx),
		queryParams,
	})

	var resp bytes.Buffer
	if len(resultTypes) > 0 {
		err, _ = resultTypes[len(resultTypes)-1].Interface().(error)
		if err != nil {
			return nil, err
		}

		pb, _ := resultTypes[0].Interface().(proto.Message)
		if pb != nil {
			err = marshaler.Marshal(&resp, pb)
			if err != nil {
				return nil, err
			}
		}
	}

	return &plugin.Response{
		ContentType: req.Accept,
		Body:        resp.Bytes(),
	}, nil
}
