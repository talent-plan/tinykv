package raftstore

import "github.com/ngaut/unistore/pd"

type resolverRunner struct {
	pdClient pd.Client
}

func newResolverRunner(pdClient pd.Client) *resolverRunner {
	return &resolverRunner{
		pdClient: pdClient,
	}
}

func (r *resolverRunner) run(t task) {
	// TODO
}
