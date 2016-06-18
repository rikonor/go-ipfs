package coreapi

import (
	"bytes"

	core "github.com/ipfs/go-ipfs/core"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
	path "github.com/ipfs/go-ipfs/path"
	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
)

type CoreAPI struct {
	ctx  context.Context
	node *core.IpfsNode
}

func NewCoreAPI(ctx context.Context, node *core.IpfsNode) (*CoreAPI, error) {
	api := &CoreAPI{ctx: ctx, node: node}
	return api, nil
}

func (api *CoreAPI) Context() context.Context {
	return api.ctx
}

func (api *CoreAPI) IpfsNode() *core.IpfsNode {
	return api.node
}

func (api *CoreAPI) ObjectGet(p string) (*coreiface.Object, error) {
	dagnode, err := core.Resolve(api.ctx, api.node, path.Path(p))
	if err != nil {
		return nil, err
	}
	obj := &coreiface.Object{
		Links: make([]*coreiface.Link, len(dagnode.Links)),
		Data:  bytes.NewReader(dagnode.Data),
	}
	for i, l := range dagnode.Links {
		obj.Links[i] = &coreiface.Link{Name: l.Name, Size: l.Size, Hash: l.Hash}
	}
	return obj, nil
}
