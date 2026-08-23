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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

// Strings for the stale-probe tests, hoisted per file so assertions and
// fixtures below stay declarative.
const (
	failCreate            = "create"
	resFileNamePvc1       = "pvc-1.res"
	resFileContent        = "resource"
	jsonNameRoleDevices   = "\"name\":\"pvc-1\",\"role\":\"Secondary\",\"devices\":"
	jsonDiskStateUpToDate = "\"disk-state\":\"UpToDate\""
	needleNotUpToDate     = "is not UpToDate and connected"
	needleLastCopy        = "only live UpToDate copy"
	fmsgRealizeMustFail   = "realize must fail in this fixture"
	fmsgDegradedTooEarly  = "pass %d: disk state degraded before the limit: %q"
	fmsgNotDegraded       = "disk state not degraded to Unknown at the limit: %q"
	fmsgStillConnected    = "connected must degrade to false with the disk state"
	fmsgProbedAtMoved     = "a failed pass must not move lastProbedAt: %v"
	fmsgNoMessage         = "the failure cause must land in the status message"
	fmsgWantUnblocked     = "expected removal to proceed, got %q"
	fmsgWantLastCopy      = "expected the last-copy block, got %q"
	fmsgWantDegradedPeer  = "expected the degraded-peer block, got %q"
	fmsgBackingNotDeleted = "backing device not deleted"
	fmsgFinalizerHeld     = "finalizer not released after removal teardown"
)

// localUpToDateStatusJSON assembles the one-resource drbdsetup status
// fixture the last-copy check reads, from spec-level pieces.
func localUpToDateStatusJSON() string {
	head := string(rune(91)) + string(rune(123))
	tail := string(rune(125)) + string(rune(93))
	return head + jsonNameRoleDevices + head + jsonDiskStateUpToDate + tail + tail
}

// staleSlot shapes one peer entry for the removal-gate fixtures.
func staleSlot(state string, connected bool, probed *metav1.Time) miroirv1alpha1.ReplicaStatus {
	return miroirv1alpha1.ReplicaStatus{DeviceCreated: true, DiskState: state, Connected: connected, LastProbedAt: probed}
}

// degradeFixtureVol is a two-replica DRBD volume whose local leg exists
// and whose realize pipeline the tests then fail on purpose.
func degradeFixtureVol() *miroirv1alpha1.MiroirVolume {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	return v
}

// A hot-looping realize pipeline must stop advertising the last healthy
// probe: after probeFailLimit consecutive errored passes the slot reads
// DiskState Unknown and disconnected, while lastProbedAt honestly stays
// at the last successful probe (the frozen-UpToDate removal deadlock).
func TestReportErrorDegradesFrozenStatusAfterRepeatedFailures(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.failOn = failCreate
	fb.existing[volPvc1] = true
	fb.created[volPvc1] = 1 << 30
	v := degradeFixtureVol()
	seeded := metav1.NewTime(time.Now().Add(-8 * time.Hour).Truncate(time.Second))
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, DiskState: diskStateUpToDate, Connected: true, LastProbedAt: &seeded},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb)}

	for pass := 1; pass <= probeFailLimit; pass++ {
		_, err := r.Reconcile(t.Context(),
			ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
		if err == nil {
			t.Fatal(fmsgRealizeMustFail)
		}
		got := &miroirv1alpha1.MiroirVolume{}
		if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
			t.Fatal(err)
		}
		st := got.Status.PerNode[nodeA]
		if pass < probeFailLimit && st.DiskState != diskStateUpToDate {
			t.Fatalf(fmsgDegradedTooEarly, pass, st.DiskState)
		}
		if pass == probeFailLimit {
			if st.DiskState != drbd.DiskUnknown {
				t.Fatalf(fmsgNotDegraded, st.DiskState)
			}
			if st.Connected {
				t.Fatal(fmsgStillConnected)
			}
		}
		if st.LastProbedAt == nil || !st.LastProbedAt.Time.Equal(seeded.Time) {
			t.Fatalf(fmsgProbedAtMoved, st.LastProbedAt)
		}
		if len(st.Message) == 0 {
			t.Fatal(fmsgNoMessage)
		}
	}
}

// A peer that stopped probing past removalStaleTolerance no longer
// blocks removal when another remaining replica is verifiably healthy —
// the frozen disconnected slot k8s-2 style deadlock. No DRBD driver is
// wired: a fresh healthy peer must make the local kernel check
// unnecessary.
func TestRemovalRoutesAroundSilentStalePeer(t *testing.T) {
	s := newScheme(t)
	staleProbe := metav1.NewTime(time.Now().Add(-2 * removalStaleTolerance))
	freshProbe := metav1.NewTime(time.Now())
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: staleSlot(diskStateUpToDate, false, &staleProbe),
		nodeC: staleSlot(diskStateUpToDate, true, &freshProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}

	if reason := r.removalBlocked(t.Context(), v); len(reason) > 0 {
		t.Fatalf(fmsgWantUnblocked, reason)
	}
}

// With every remaining replica silent, a live UpToDate local leg is the
// last known-good copy: removal must stay blocked, with a reason naming
// the silent peers.
func TestRemovalBlockedWhenLocalLegHoldsLastLiveCopy(t *testing.T) {
	s := newScheme(t)
	staleProbe := metav1.NewTime(time.Now().Add(-2 * removalStaleTolerance))
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: staleSlot(diskStateUpToDate, false, &staleProbe),
		nodeC: staleSlot(diskStateUpToDate, false, &staleProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).Build()
	fe := &fakeDRBDExec{statusJSON: localUpToDateStatusJSON()}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	reason := r.removalBlocked(t.Context(), v)
	if !strings.Contains(reason, needleLastCopy) {
		t.Fatalf(fmsgWantLastCopy, reason)
	}
}

// With every remaining replica silent and no readable local kernel
// state (resource never brought back up), nothing verifiably healthy is
// lost by the teardown: removal proceeds, mirroring what full-volume
// deletion already permits on the same wreck.
func TestRemovalProceedsWhenLocalLegUnreadable(t *testing.T) {
	s := newScheme(t)
	staleProbe := metav1.NewTime(time.Now().Add(-2 * removalStaleTolerance))
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: staleSlot(diskStateUpToDate, false, &staleProbe),
		nodeC: staleSlot(diskStateUpToDate, false, &staleProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).Build()
	fe := &fakeDRBDExec{}
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run}}

	if reason := r.removalBlocked(t.Context(), v); len(reason) > 0 {
		t.Fatalf(fmsgWantUnblocked, reason)
	}
}

// A peer that still probes but is degraded keeps blocking removal: the
// tolerance is for silent nodes only, never for a live resync.
func TestRemovalStillBlockedByFreshDegradedPeer(t *testing.T) {
	s := newScheme(t)
	staleProbe := metav1.NewTime(time.Now().Add(-2 * removalStaleTolerance))
	freshProbe := metav1.NewTime(time.Now())
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: staleSlot(diskStateUpToDate, false, &staleProbe),
		nodeC: staleSlot(diskStateInconsistent, true, &freshProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}

	reason := r.removalBlocked(t.Context(), v)
	if !strings.Contains(reason, needleNotUpToDate) {
		t.Fatalf(fmsgWantDegradedPeer, reason)
	}
}

// A slot with no LastProbedAt (written before the field existed) is
// never tolerated as stale — the legacy conservative behavior stands.
func TestRemovalNilProbeTimestampNeverTolerated(t *testing.T) {
	s := newScheme(t)
	freshProbe := metav1.NewTime(time.Now())
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: staleSlot(diskStateUpToDate, false, nil),
		nodeC: staleSlot(diskStateUpToDate, true, &freshProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}

	reason := r.removalBlocked(t.Context(), v)
	if !strings.Contains(reason, needleNotUpToDate) {
		t.Fatalf(fmsgWantDegradedPeer, reason)
	}
}

// A diskless tie-breaker holds no data, so silent data legs never pin
// its removal on the local kernel check.
func TestRemovalStaleToleranceSkipsLocalCheckForDiskless(t *testing.T) {
	s := newScheme(t)
	staleProbe := metav1.NewTime(time.Now().Add(-2 * removalStaleTolerance))
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {Diskless: true},
		nodeB: staleSlot(diskStateUpToDate, false, &staleProbe),
		nodeC: staleSlot(diskStateUpToDate, false, &staleProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(v).Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(newFakeBackend())}

	if reason := r.removalBlocked(t.Context(), v); len(reason) > 0 {
		t.Fatalf(fmsgWantUnblocked, reason)
	}
}

// End to end: a removed replica tears down, releases its finalizer and
// drops its status slot even while one remaining peer is silent, as
// long as another remaining replica is verifiably healthy.
func TestReconcileRemovalToleratesSilentPeer(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	fb.created[volPvc1] = 1 << 30
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, resFileNamePvc1), []byte(resFileContent), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{}
	staleProbe := metav1.NewTime(time.Now().Add(-2 * removalStaleTolerance))
	freshProbe := metav1.NewTime(time.Now())
	v := removedReplicaVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: staleSlot(diskStateUpToDate, false, &staleProbe),
		nodeC: staleSlot(diskStateUpToDate, true, &freshProbe),
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run}}

	reconcile(t, r, volPvc1)

	if _, ok := fb.created[volPvc1]; ok {
		t.Fatal(fmsgBackingNotDeleted)
	}
	fe.calledWith(t, cmdDownPvc1)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	for _, f := range got.Finalizers {
		if f == constants.FinalizerPrefix+nodeA {
			t.Fatal(fmsgFinalizerHeld)
		}
	}
}
