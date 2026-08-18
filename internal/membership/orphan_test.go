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

package membership

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// snapOrphan is the snapshot a clone volume was cut from.
const snapOrphan = "snap-orphan"

// cloneVol is the shape the reporting cluster kept accumulating: a
// snapshot-clone source, old enough to be past any provisioning race,
// with nothing referencing it.
func cloneVol(age time.Duration) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              volPvc1,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
		},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			Source:    &miroirv1alpha1.VolumeSource{SnapshotName: snapOrphan},
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA}},
		},
	}
}

func newOrphan(t *testing.T, objs ...client.Object) *OrphanReconciler {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(objs...).Build()
	return &OrphanReconciler{Client: c, PVs: c, After: time.Hour}
}

func reconcileOrphan(t *testing.T, r *OrphanReconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func orphanCond(t *testing.T, r *OrphanReconciler) *metav1.Condition {
	t.Helper()
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, miroirv1alpha1.ConditionOrphaned)
}

// The leak the sweep exists for: a volume no PV names still holds pool
// space, a minor and a port, and nothing used to say so.
func TestOrphanConditionsVolumeWithNoPV(t *testing.T) {
	r := newOrphan(t, cloneVol(21*time.Hour))

	reconcileOrphan(t, r)

	cond := orphanCond(t, r)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("a volume with no PV must be conditioned Orphaned, got %+v", cond)
	}
}

// The CSI create window is racy by design: CreateVolume makes the
// MiroirVolume and the provisioner makes the PV afterwards.
func TestOrphanWaitsOutTheProvisioningRace(t *testing.T) {
	r := newOrphan(t, cloneVol(time.Minute))

	res := reconcileOrphan(t, r)

	if cond := orphanCond(t, r); cond != nil {
		t.Fatalf("a volume mid-provision must not be conditioned, got %+v", cond)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > time.Hour {
		t.Fatalf("must requeue for the rest of the grace period, got %v", res.RequeueAfter)
	}
}

func TestOrphanClearsWhenPVAppears(t *testing.T) {
	v := cloneVol(21 * time.Hour)
	r := newOrphan(t, v)
	reconcileOrphan(t, r)
	if cond := orphanCond(t, r); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("setup: expected the volume conditioned Orphaned, got %+v", cond)
	}

	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: volPvc1}}
	if err := r.Create(t.Context(), pv); err != nil {
		t.Fatal(err)
	}
	reconcileOrphan(t, r)

	if cond := orphanCond(t, r); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("a volume that gained a PV must be cleared, got %+v", cond)
	}
}

// A volume already being deleted has legitimately lost its PV: the
// provisioner deletes the PV object only after DeleteVolume returns.
func TestOrphanIgnoresDeletingVolume(t *testing.T) {
	v := cloneVol(21 * time.Hour)
	v.Finalizers = []string{"miroir.home-operations.com/teardown-" + nodeA}
	r := newOrphan(t, v)
	if err := r.Delete(t.Context(), v); err != nil {
		t.Fatal(err)
	}

	reconcileOrphan(t, r)

	if cond := orphanCond(t, r); cond != nil {
		t.Fatalf("a volume under teardown must be left alone, got %+v", cond)
	}
}

// Conditioning is the default; deletion is opt-in.
func TestOrphanDoesNotReapByDefault(t *testing.T) {
	r := newOrphan(t, cloneVol(21*time.Hour))

	reconcileOrphan(t, r)

	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatalf("with no reap window the volume must survive: %v", err)
	}
}

func TestOrphanReapsAfterTheWindow(t *testing.T) {
	v := cloneVol(21 * time.Hour)
	v.Status.Conditions = []metav1.Condition{{
		Type:               miroirv1alpha1.ConditionOrphaned,
		Status:             metav1.ConditionTrue,
		Reason:             reasonNoBackingPV,
		Message:            "no PersistentVolume",
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
	}}
	r := newOrphan(t, v)
	r.ReapAfter = time.Hour

	reconcileOrphan(t, r)

	err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, &miroirv1alpha1.MiroirVolume{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("a settled orphan must be reaped, got %v", err)
	}
}

// Issue #195: never destroy a backing under a consumer, however orphaned
// the paperwork looks.
func TestOrphanReapBlocked(t *testing.T) {
	settled := func() *miroirv1alpha1.MiroirVolume {
		v := cloneVol(21 * time.Hour)
		v.Status.Conditions = []metav1.Condition{{
			Type:               miroirv1alpha1.ConditionOrphaned,
			Status:             metav1.ConditionTrue,
			Reason:             reasonNoBackingPV,
			Message:            "no PersistentVolume",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
		}}
		return v
	}
	now := metav1.Now()
	tests := []struct {
		name string
		vol  *miroirv1alpha1.MiroirVolume
		snap *miroirv1alpha1.MiroirSnapshot
	}{
		{
			name: "a client leg is attached", vol: func() *miroirv1alpha1.MiroirVolume {
				v := settled()
				v.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeB}}
				return v
			}(),
		},
		{
			name: "a leg is DRBD Primary", vol: func() *miroirv1alpha1.MiroirVolume {
				v := settled()
				v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
					nodeA: {PrimarySince: &now},
				}
				return v
			}(),
		},
		{
			name: "a snapshot still references it", vol: settled(),
			snap: &miroirv1alpha1.MiroirSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: snapOrphan, Namespace: "default"},
				Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volPvc1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{tc.vol}
			if tc.snap != nil {
				objs = append(objs, tc.snap)
			}
			r := newOrphan(t, objs...)
			r.ReapAfter = time.Hour

			res := reconcileOrphan(t, r)

			if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1},
				&miroirv1alpha1.MiroirVolume{}); err != nil {
				t.Fatalf("the volume must survive (%s): %v", tc.name, err)
			}
			if res.RequeueAfter != orphanRecheckInterval {
				t.Fatalf("a blocked reap must come round again, got %v", res.RequeueAfter)
			}
		})
	}
}
