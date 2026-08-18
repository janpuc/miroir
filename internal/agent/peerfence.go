/*
Copyright 2026.

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.gnu.org/licenses/agpl-3.0.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"context"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// DefaultPeerWedgeInterval is how often an agent re-reads which other
// nodes have wedged.
const DefaultPeerWedgeInterval = 60 * time.Second

// PeerWedge reports whether another node's storage stack has been wedged
// long enough to act on. *WedgedPeers implements it; a nil PeerWedge
// disables peer fencing entirely.
type PeerWedge interface {
	Wedged(node string) bool
}

// WedgedPeers tracks which other storage nodes have published a
// StorageWedged condition that has since settled.
//
// It polls instead of watching, and that is deliberate: the agent's
// MiroirNode informer is pinned to this node's own object precisely so an
// unscoped watch does not deliver every node's per-minute heartbeat to
// every agent (N² events across the cluster). One direct list per agent
// per minute is the same order of API traffic the pool-stats publisher
// already makes, and unlike a watch it does not grow with heartbeat rate.
type WedgedPeers struct {
	// Reader must be uncached (mgr.GetAPIReader): the manager's cache
	// holds only this node's own MiroirNode.
	Reader   client.Reader
	NodeName string
	// Interval between polls; DefaultPeerWedgeInterval when zero.
	Interval time.Duration
	// SettleFor is how long StorageWedged must have held before a peer
	// counts as fenceable; constants.WedgeSettleAfter when zero.
	SettleFor time.Duration

	mu     sync.RWMutex
	wedged map[string]bool
}

// Wedged reports the last poll's verdict for node. Never true for this
// node: an agent does not fence itself, and its own breaker already
// refuses everything it could do about it.
func (w *WedgedPeers) Wedged(node string) bool {
	if w == nil {
		return false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.wedged[node]
}

// Start polls until ctx is cancelled. A failed poll keeps the previous
// verdict rather than clearing it: a fence that flickered off on one
// unreachable API call would reconnect a dead peer and re-take the
// promotion veto it exists to lift.
func (w *WedgedPeers) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("wedged-peers")
	interval := w.Interval
	if interval <= 0 {
		interval = DefaultPeerWedgeInterval
	}
	if err := w.refresh(ctx); err != nil {
		log.Error(err, "initial wedged-peer poll failed")
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			if err := w.refresh(ctx); err != nil {
				log.Error(err, "wedged-peer poll failed; keeping the previous verdict")
			}
		}
	}
}

func (w *WedgedPeers) refresh(ctx context.Context) error {
	nodes := &miroirv1alpha1.MiroirNodeList{}
	if err := w.Reader.List(ctx, nodes); err != nil {
		return err
	}
	settle := w.SettleFor
	if settle <= 0 {
		settle = constants.WedgeSettleAfter
	}
	wedged := map[string]bool{}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Name == w.NodeName {
			continue
		}
		if ok, since := n.Status.StorageWedgedSince(); ok && time.Since(since.Time) >= settle {
			wedged[n.Name] = true
		}
	}
	w.mu.Lock()
	w.wedged = wedged
	w.mu.Unlock()
	return nil
}

// fencedPeers names the peers this leg must leave out of its rendered
// resource config, because their node's own agent has reported its
// storage stack wedged.
//
// A wedged node's kernel keeps every resource it held open read-only, and
// DRBD then refuses promotion on the *healthy* peers ("Peer may not
// become primary while device is opened read-only"), so two clean
// survivors cannot serve a volume the third, dead node is sitting on.
// Dropping the peer from the config is what makes the disconnect stick: a
// bare drbdsetup disconnect is undone within the second by the next
// adjust, which reconnects everything the .res still lists.
//
// Deliberately narrow, because fencing a peer that is not actually dead
// risks split-brain:
//   - only a diskful local leg fences; a diskless client or tie-breaker
//     would be cutting away the data path it reads through,
//   - only from a leg the previous pass saw UpToDate and not split-brain
//     (read from status like SkipDiskAttach, so it lags by one pass) — a
//     leg that is itself unhealthy is in no position to judge a peer,
//   - and only while the surviving legs keep a majority, so the fence can
//     never be the thing that costs the volume its quorum. That rules out
//     fencing the only peer of a 2-leg volume, where dropping the link
//     would trade a promotion veto for outright I/O errors.
//
// The verdict rides the rendered config, so it lands on the next full
// pass: within one deep-check interval of the poll that saw the wedge,
// not immediately.
func fencedPeers(vol *miroirv1alpha1.MiroirVolume, localNode string, localDiskless bool, peers PeerWedge) map[string]bool {
	if peers == nil || localDiskless || vol.Spec.DRBD == nil {
		return nil
	}
	local := vol.Status.PerNode[localNode]
	if local.DiskState != drbd.DiskUpToDate || local.SplitBrain {
		return nil
	}
	legs := len(vol.Spec.Replicas) + len(vol.Spec.Clients)
	fenced := map[string]bool{}
	for _, rep := range vol.Spec.Replicas {
		if rep.Node != localNode && peers.Wedged(rep.Node) {
			fenced[rep.Node] = true
		}
	}
	for _, cl := range vol.Spec.Clients {
		if cl.Node != localNode && peers.Wedged(cl.Node) {
			fenced[cl.Node] = true
		}
	}
	if len(fenced) == 0 || (legs-len(fenced))*2 <= legs {
		return nil
	}
	return fenced
}
