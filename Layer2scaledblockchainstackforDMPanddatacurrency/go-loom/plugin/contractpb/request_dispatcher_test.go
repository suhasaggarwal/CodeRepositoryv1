package contractpb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	loom "github.com/loomnetwork/go-loom"
	"github.com/loomnetwork/go-loom/plugin"
	"github.com/loomnetwork/go-loom/testdata"
)

var (
	callArgs = testdata.CallArgs{
		Key:   "test",
		Value: 12345,
	}

	staticCallArgs = testdata.StaticCallArgs{
		Index: 5,
		Name:  "john",
	}

	addr1 = loom.MustParseAddress("chain:0xb16a379ec18d4093666f8f38b11a3071c920207d")
)

type MockContract struct {
	t *testing.T
}

func (c *MockContract) Meta() (plugin.Meta, error) {
	return plugin.Meta{
		Name:    "fakecontract",
		Version: "0.0.1",
	}, nil
}

func (c *MockContract) Init(ctx Context, req *plugin.Request) error {
	return nil
}

func (c *MockContract) HandleTx(ctx Context, args *testdata.CallArgs) error {
	require.Equal(c.t, callArgs.String(), args.String())
	return nil
}

func (c *MockContract) HandleQuery(ctx StaticContext, args *testdata.StaticCallArgs) (*testdata.StaticCallResult, error) {
	require.Equal(c.t, staticCallArgs.String(), args.String())
	return &testdata.StaticCallResult{
		Code:   123,
		Result: "QueryResult",
	}, nil
}

func newFakeRequestDispatcherContract(t *testing.T) *RequestDispatcher {
	r, err := NewRequestDispatcher(&MockContract{t: t})
	if err != nil {
		panic(err)
	}
	return r
}

func TestEmbeddedRequestDispatcherDoesNotRegisterOwnMethods(t *testing.T) {
	var err error
	c := newFakeRequestDispatcherContract(t)
	_, _, err = c.callbacks.Get("contract.Call")
	require.NotNil(t, err)
	_, _, err = c.callbacks.Get("contract.StaticCall")
	require.NotNil(t, err)
}

func TestRequestDispatcherCallMethod(t *testing.T) {
	c := newFakeRequestDispatcherContract(t)
	encodings := []plugin.EncodingType{plugin.EncodingType_JSON, plugin.EncodingType_PROTOBUF3}

	for _, encoding := range encodings {
		marshaler, err := MarshalerFactory(encoding)
		require.Nil(t, err)

		var argsBuffer bytes.Buffer
		require.Nil(t, marshaler.Marshal(&argsBuffer, &callArgs))

		msg := &plugin.ContractMethodCall{
			Method: "HandleTx",
			Args:   argsBuffer.Bytes(),
		}

		var msgBuffer bytes.Buffer
		require.Nil(t, marshaler.Marshal(&msgBuffer, msg))

		req := &plugin.Request{
			ContentType: encoding,
			Accept:      encoding,
			Body:        msgBuffer.Bytes(),
		}

		resp, err := c.Call(plugin.CreateFakeContext(addr1, addr1), req)
		require.Nil(t, err)
		require.Equal(t, req.Accept, resp.ContentType)
	}
}

func TestRequestDispatcherStaticCallMethod(t *testing.T) {
	c := newFakeRequestDispatcherContract(t)
	encodings := []plugin.EncodingType{plugin.EncodingType_JSON, plugin.EncodingType_PROTOBUF3}

	for _, encoding := range encodings {
		marshaler, err := MarshalerFactory(encoding)
		require.Nil(t, err)

		var argsBuffer bytes.Buffer
		require.Nil(t, marshaler.Marshal(&argsBuffer, &staticCallArgs))

		msg := &plugin.ContractMethodCall{
			Method: "HandleQuery",
			Args:   argsBuffer.Bytes(),
		}

		var msgBuffer bytes.Buffer
		require.Nil(t, marshaler.Marshal(&msgBuffer, msg))

		req := &plugin.Request{
			ContentType: encoding,
			Accept:      encoding,
			Body:        msgBuffer.Bytes(),
		}

		resp, err := c.StaticCall(plugin.CreateFakeContext(addr1, addr1), req)
		require.Nil(t, err)
		require.Equal(t, req.Accept, resp.ContentType)

		unmarshaler, err := UnmarshalerFactory(resp.ContentType)
		require.Nil(t, err)

		var callResult testdata.StaticCallResult
		err = unmarshaler.Unmarshal(bytes.NewBuffer(resp.Body), &callResult)
		require.Nil(t, err)
		assert.Equal(t, int32(123), callResult.Code)
		assert.Equal(t, "QueryResult", callResult.Result)
	}
}
