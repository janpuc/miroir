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
	"errors"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// fakePeers is a fixed verdict, so the fence gates can be exercised
// without a poll.
type fakePeers map[string]bool

func (p fakePeers) Wedged(node string) bool { return p[node] }

// fenceVol is a 3-leg volume (two diskful + a diskless tie-breaker) whose
// local leg on node-a is clean — the shape where fencing is allowed.
func fenceVol() *miroirv1alpha1.MiroirVolume {
	v := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			DRBD:      &miroirv1alpha1.DRBDSpec{Port: 7000},
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeA, NodeID: 0, Address: "192.168.1.41"},
				{Node: nodeB, NodeID: 1, Address: "192.168.1.42"},
				{Node: nodeC, NodeID: 2, Address: "192.168.1.43", Diskless: true},
			},
		},
	}
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DiskState: drbd.DiskUpToDate},
	}
	return v
}

func TestFencedPeers(t *testing.T) {
	twoLeg := func() *miroirv1alpha1.MiroirVolume {
		v := fenceVol()
		v.Spec.Replicas = v.Spec.Replicas[:2]
		return v
	}
	tests := []struct {
		name        string
		vol         *miroirv1alpha1.MiroirVolume
		diskless    bool
		peers       PeerWedge
		want        []string
		wantSkipWhy string
	}{
		{
			name: "wedged diskful peer of a 3-leg volume", vol: fenceVol(),
			peers: fakePeers{nodeB: true}, want: []string{nodeB},
		},
		{
			name: "no peers oracle wired", vol: fenceVol(), peers: nil,
			wantSkipWhy: "peer fencing is opt-in; without the watcher the fence is inert",
		},
		{
			name: "nothing wedged", vol: fenceVol(), peers: fakePeers{},
			wantSkipWhy: "a healthy cluster must render every configured peer",
		},
		{
			name: "local leg is diskless", vol: fenceVol(), diskless: true,
			peers:       fakePeers{nodeB: true},
			wantSkipWhy: "a diskless leg reads through its diskful peers; fencing one cuts its data path",
		},
		{
			name: "local leg not UpToDate", vol: func() *miroirv1alpha1.MiroirVolume {
				v := fenceVol()
				v.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{DiskState: "Inconsistent"}
				return v
			}(), peers: fakePeers{nodeB: true},
			wantSkipWhy: "a leg that is itself unhealthy is in no position to judge a peer",
		},
		{
			name: "local leg split-brain", vol: func() *miroirv1alpha1.MiroirVolume {
				v := fenceVol()
				v.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{
					DiskState: drbd.DiskUpToDate, SplitBrain: true,
				}
				return v
			}(), peers: fakePeers{nodeB: true},
			wantSkipWhy: "a split leg must resolve its own divergence, not redraw membership",
		},
		{
			name: "fencing would cost the majority", vol: twoLeg(),
			peers:       fakePeers{nodeB: true},
			wantSkipWhy: "losing quorum turns a promotion veto into outright I/O errors",
		},
		{
			name: "unreplicated volume", vol: func() *miroirv1alpha1.MiroirVolume {
				v := fenceVol()
				v.Spec.DRBD = nil
				return v
			}(), peers: fakePeers{nodeB: true},
			wantSkipWhy: "an unreplicated volume has no peers to fence",
		},
		{
			name: "the local node is never fenced", vol: fenceVol(),
			peers:       fakePeers{nodeA: true},
			wantSkipWhy: "an agent does not fence itself out of its own resource",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := fencedPeers(tc.vol, nodeA, tc.diskless, tc.peers)
			if tc.wantSkipWhy != "" {
				if len(got) != 0 {
					t.Fatalf("expected no fencing (%s), got %v", tc.wantSkipWhy, got)
				}
				return
			}
			names := make([]string, 0, len(got))
			for n := range got {
				names = append(names, n)
			}
			slices.Sort(names)
			if !slices.Equal(names, tc.want) {
				t.Fatalf("fenced = %v, want %v", names, tc.want)
			}
		})
	}
}

// The fence lands by omission: a peer left out of the .res is one adjust
// turns into a del-peer, which is what makes the disconnect outlive the
// next reconcile.
func TestDRBDResourceOmitsFencedPeer(t *testing.T) {
	res := drbdResource(fenceVol(), nodeA, "/dev/vg/pvc-1", 1000, false, 0,
		map[string]bool{nodeB: true})

	if slices.ContainsFunc(res.Peers, func(p drbd.Peer) bool { return p.Node == nodeB }) {
		t.Fatalf("a fenced peer must not be rendered: %+v", res.Peers)
	}
	if !slices.ContainsFunc(res.Peers, func(p drbd.Peer) bool { return p.Node == nodeA }) ||
		!slices.ContainsFunc(res.Peers, func(p drbd.Peer) bool { return p.Node == nodeC }) {
		t.Fatalf("only the fenced peer may be dropped: %+v", res.Peers)
	}
}

func TestWedgedPeersPoll(t *testing.T) {
	settled := &miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: nodeB}}
	settled.Status.Conditions = []metav1.Condition{{
		Type:               miroirv1alpha1.ConditionStorageWedged,
		Status:             metav1.ConditionTrue,
		Reason:             miroirv1alpha1.ReasonKernelAssertion,
		Message:            "wedged",
		LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
	}}
	fresh := &miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: nodeC}}
	fresh.Status.Conditions = []metav1.Condition{{
		Type:               miroirv1alpha1.ConditionStorageWedged,
		Status:             metav1.ConditionTrue,
		Reason:             miroirv1alpha1.ReasonKernelAssertion,
		Message:            "wedged",
		LastTransitionTime: metav1.Now(),
	}}
	// This node is wedged too, and must still never fence itself.
	self := &miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: nodeA}}
	self.Status.Conditions = settled.Status.Conditions

	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(settled, fresh, self).Build()
	w := &WedgedPeers{Reader: c, NodeName: nodeA}

	if err := w.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !w.Wedged(nodeB) {
		t.Fatal("a peer wedged past the settle period must be fenceable")
	}
	if w.Wedged(nodeC) {
		t.Fatal("a freshly wedged peer must wait out the settle period — a reboot may fix it")
	}
	if w.Wedged(nodeA) {
		t.Fatal("an agent must never fence itself")
	}
}

// errReader stands in for an API server the agent briefly cannot reach.
type errReader struct{ client.Reader }

func (errReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("api unavailable")
}

// A failed poll keeps the previous verdict: reconnecting a dead peer on
// one unreachable API call re-takes the promotion veto the fence lifted.
func TestWedgedPeersKeepsVerdictOnPollFailure(t *testing.T) {
	w := &WedgedPeers{Reader: errReader{}, NodeName: nodeA}
	w.wedged = map[string]bool{nodeB: true}

	if err := w.refresh(t.Context()); err == nil {
		t.Fatal("an unreachable API must surface as a poll error")
	}
	if !w.Wedged(nodeB) {
		t.Fatal("the previous verdict must survive a failed poll")
	}
}
