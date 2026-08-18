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
	"context"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// orphanRecheckInterval paces re-examination of every volume. Nothing
// event-driven fires when a PersistentVolume is deleted — the controller
// deliberately does not cache PVs (one uncached read per volume per pass
// beats an informer over every PV in the cluster) — so the sweep has to
// come round on its own.
const orphanRecheckInterval = 15 * time.Minute

// reasonNoBackingPV is the Orphaned condition reason.
const reasonNoBackingPV = "NoBackingPV"

// OrphanReconciler flags MiroirVolumes whose PersistentVolume is gone.
//
// A snapshot-clone source whose PV never arrived, or whose PV an operator
// deleted by hand, keeps everything a live volume keeps: thin-pool space,
// a DRBD minor, a replication port, and a full set of miroir_volume_*
// series — including the per-volume wedge gauge, which then pages on a
// volume nobody owns. Nothing in miroir ever collected them; the reporting
// cluster had 28 cleaned by hand and two more within the week.
//
// Conditioning is the default and deleting is opt-in, deliberately. The
// failure mode of a wrong condition is a line in kubectl describe; the
// failure mode of a wrong delete is a destroyed backing device. Set
// ReapAfter only once the Orphaned condition has been seen to name the
// volumes an operator would have deleted anyway.
type OrphanReconciler struct {
	client.Client
	// PVs reads PersistentVolumes uncached (mgr.GetAPIReader): a stale
	// cache here reads as "no PV" and would condition a live volume.
	PVs client.Reader
	// After is how long a volume may exist with no PV before it is
	// conditioned Orphaned. It exists for the provisioning race —
	// CreateVolume creates the MiroirVolume and the external provisioner
	// creates the PV afterwards — so it wants to be comfortably longer
	// than a slow provision. The setup path guards > 0.
	After time.Duration
	// ReapAfter deletes a volume once Orphaned has held this long; zero
	// (the default) never deletes and leaves the condition standing.
	ReapAfter time.Duration
	// Recorder emits the Orphaned and OrphanReaped events; optional.
	Recorder events.EventRecorder
}

// Reconcile checks one volume for its backing PV.
func (r *OrphanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !vol.DeletionTimestamp.IsZero() {
		// Teardown already owns it, and its PV is legitimately gone by
		// now: the provisioner deletes the PV object only after
		// DeleteVolume returns, which is what set this timestamp.
		return ctrl.Result{}, nil
	}

	hasPV, err := r.hasPV(ctx, vol.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if hasPV {
		return ctrl.Result{RequeueAfter: orphanRecheckInterval}, r.clear(ctx, vol)
	}
	if grace := r.After - time.Since(vol.CreationTimestamp.Time); grace > 0 {
		// Too young to tell a stuck provision from a normal one.
		return ctrl.Result{RequeueAfter: grace}, nil
	}
	if err := r.flag(ctx, vol); err != nil {
		return ctrl.Result{}, err
	}
	return r.reap(ctx, vol)
}

// hasPV reports whether a PersistentVolume shares the volume's name.
// Deliberately name-only: a PV that exists but belongs to another driver
// still means something out there references this name, and refusing to
// act is the safe reading.
func (r *OrphanReconciler) hasPV(ctx context.Context, name string) (bool, error) {
	pv := &corev1.PersistentVolume{}
	err := r.PVs.Get(ctx, types.NamespacedName{Name: name}, pv)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// clear retires the condition on a volume whose PV is present — a
// statically re-created PV, or one that lost the provisioning race the
// grace period exists for.
func (r *OrphanReconciler) clear(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	if meta.FindStatusCondition(vol.Status.Conditions, miroirv1alpha1.ConditionOrphaned) == nil {
		return nil
	}
	return r.setCondition(ctx, vol, metav1.Condition{
		Type:    miroirv1alpha1.ConditionOrphaned,
		Status:  metav1.ConditionFalse,
		Reason:  "BoundToPV",
		Message: "a PersistentVolume named " + vol.Name + " exists",
	})
}

// flag raises Orphaned, announcing it once on the transition.
func (r *OrphanReconciler) flag(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) error {
	prev := meta.FindStatusCondition(vol.Status.Conditions, miroirv1alpha1.ConditionOrphaned)
	if prev != nil && prev.Status == metav1.ConditionTrue {
		return nil
	}
	msg := "no PersistentVolume named " + vol.Name + " exists; the volume still holds pool space, " +
		"a DRBD minor and a replication port"
	if err := r.setCondition(ctx, vol, metav1.Condition{
		Type:    miroirv1alpha1.ConditionOrphaned,
		Status:  metav1.ConditionTrue,
		Reason:  reasonNoBackingPV,
		Message: msg,
	}); err != nil {
		return err
	}
	if r.Recorder != nil {
		// The message is data, not a format string: it embeds the volume name.
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning,
			miroirv1alpha1.ConditionOrphaned, "Sweep", "%s", msg)
	}
	return nil
}

func (r *OrphanReconciler) setCondition(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, cond metav1.Condition) error {
	if !meta.SetStatusCondition(&vol.Status.Conditions, cond) {
		return nil
	}
	return r.Status().Update(ctx, vol)
}

// reap deletes an orphan once ReapAfter has elapsed on the condition and
// nothing is using the volume. Its teardown finalizers then release the
// backing device, the minor and the port through the ordinary flow.
func (r *OrphanReconciler) reap(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	if r.ReapAfter <= 0 {
		return ctrl.Result{RequeueAfter: orphanRecheckInterval}, nil
	}
	cond := meta.FindStatusCondition(vol.Status.Conditions, miroirv1alpha1.ConditionOrphaned)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: orphanRecheckInterval}, nil
	}
	if remaining := r.ReapAfter - time.Since(cond.LastTransitionTime.Time); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if reason, err := r.reapBlocked(ctx, vol); err != nil {
		return ctrl.Result{}, err
	} else if reason != "" {
		log.Info("orphan reap blocked", "volume", vol.Name, "reason", reason)
		return ctrl.Result{RequeueAfter: orphanRecheckInterval}, nil
	}
	if err := r.Delete(ctx, vol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, "OrphanReaped", "Sweep",
			"no PersistentVolume has named this volume for %s; deleted it to release its pool space, DRBD minor and port",
			r.ReapAfter)
	}
	log.Info("reaped an orphaned volume", "volume", vol.Name)
	return ctrl.Result{}, nil
}

// reapBlocked reports why the orphan must not be deleted ("" when it may
// go). Issue #195 is the governing constraint: never destroy a backing
// under a consumer, however orphaned the paperwork looks.
func (r *OrphanReconciler) reapBlocked(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (string, error) {
	if len(vol.Spec.Clients) > 0 {
		return "a client leg is attached", nil
	}
	for node, st := range vol.Status.PerNode {
		if st.PrimarySince != nil {
			// DRBD Primary means the device is open: something is using
			// it right now, PV or no PV.
			return "the leg on " + node + " is DRBD Primary", nil
		}
	}
	snaps := &miroirv1alpha1.MiroirSnapshotList{}
	if err := r.List(ctx, snaps); err != nil {
		return "", err
	}
	if slices.ContainsFunc(snaps.Items, func(s miroirv1alpha1.MiroirSnapshot) bool {
		return s.Spec.VolumeName == vol.Name
	}) {
		return "snapshots still reference the volume", nil
	}
	return "", nil
}

// SetupWithManager registers the reconciler on MiroirVolume events. The
// PV side is polled (see orphanRecheckInterval), not watched.
func (r *OrphanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{}).
		Named("orphanvolume").
		Complete(r)
}
