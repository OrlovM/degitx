// MIT License. Copyright (c) 2020 CQFN
// https://github.com/cqfn/degitx/blob/master/LICENSE

package twopc

import (
	"context"
	"sync"

	"cqfn.org/degitx/pkg/tcommit"
)

// RmNode implements resource manager node instance
type RmNode struct {
	mux sync.Mutex
	id  tcommit.NodeID
	tm  tcommit.Manager
	log map[tcommit.TxID]tcommit.Vote
}

func (rm *RmNode) Prepare(ctx context.Context, tid tcommit.TxID) error {
	var vote tcommit.Vote
	if v, voted := rm.log[tid]; voted {
		vote = v
	} else {

		if rm.canCommit(ctx, tid) {
			vote = tcommit.VotePrepared
		} else {
			vote = tcommit.VoteAborted
		}
		rm.log[tid] = vote
	}
	votes := make(map[tcommit.NodeID]tcommit.Vote)
	votes[rm.id] = vote
	return rm.tm.Begin(ctx, tid, votes)
}

func (rm *RmNode) Commit(ctx context.Context, tid tcommit.TxID) error {
	return rm.tm.Finish(ctx, rm.id)
}

func (rm *RmNode) Abort(ctx context.Context, tid tcommit.TxID) error {
	return rm.tm.Finish(ctx, rm.id)
}

func (rm *RmNode) canCommit(ctx context.Context, tid tcommit.TxID) bool {
	return true
}
