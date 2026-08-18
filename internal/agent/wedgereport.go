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
	"slices"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
)

// DefaultWedgeReportInterval paces the breaker's outward reporting: short
// enough to show the jam building rather than only its aftermath, and cheap
// at one /proc/<pid>/stat read per outstanding child plus two cached reads.
const DefaultWedgeReportInterval = 30 * time.Second

// WedgeReporter publishes the node-scoped breaker's state everywhere
// outside this process that has to act on it: the miroir_node_wedged gauge,
// the StorageWedged condition on the node's MiroirNode (which is how
// auto-evict learns the node is dead without scraping Prometheus), a
// Warning Event on first latch, and a NoSchedule taint that stops the
// scheduler placing new consumers here.
//
// Sampled rather than event-driven because the stranded-child count
// self-heals: state written only on the way up would page, taint, and
// condition forever after the node recovered.
type WedgeReporter struct {
	Client   client.Client
	NodeName string
	// Wedge is the node's single breaker, shared with the DRBD driver,
	// the pool backends and the node service.
	Wedge *backend.Wedge
	// Interval between samples; DefaultWedgeReportInterval when zero.
	Interval time.Duration
	// Recorder emits the first-latch Warning Events; optional.
	Recorder events.EventRecorder
	// Taint applies miroir's storage-wedged taint to this node's Node
	// object while the breaker is open. Opt-out (chart value): an
	// operator running their own node remediation may not want miroir
	// writing to Node objects at all.
	Taint bool

	// open is the last reported breaker state, so the log records edges.
	open bool
}

// Start reports on the interval until ctx is cancelled.
//
// Deliberately no report before the first tick: AssertionWatcher drains the
// kmsg ring on entry, and publishing ahead of it would write StorageWedged=False
// on a node that is still wedged from before this agent started — resetting
// the condition's transition time, and with it auto-evict's settle clock,
// on every agent restart.
func (r *WedgeReporter) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("wedge")
	interval := r.Interval
	if interval <= 0 {
		interval = DefaultWedgeReportInterval
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			r.report(ctx, log)
		}
	}
}

// report publishes one sample. Each destination is attempted
// independently: an API failure on one must not cost the others.
func (r *WedgeReporter) report(ctx context.Context, log logr.Logger) {
	// Read Latched before Err: a latch never clears within the process, so
	// latched implies a non-nil Err, while the reverse ordering could read
	// a stranded-only breaker that drained in between.
	latched := r.Wedge.Latched()
	wedgeErr := r.Wedge.Err()
	RecordNodeWedge(r.Wedge)

	// Edges only: a jammed node stays jammed until it reboots, and
	// repeating the line every interval buries it.
	if open := wedgeErr != nil; open != r.open {
		r.open = open
		if open {
			log.Error(wedgeErr, "node storage stack wedged; refusing further storage commands until this node reboots",
				"stranded", r.Wedge.Stranded())
		} else {
			log.Info("node storage stack recovered; resuming storage commands")
		}
	}

	first, err := r.syncCondition(ctx, latched, wedgeErr)
	if err != nil {
		log.Error(err, "cannot publish the StorageWedged condition")
	}
	if r.Taint {
		if err := r.syncTaint(ctx, wedgeErr != nil); err != nil {
			log.Error(err, "cannot sync the storage-wedged taint")
		}
	}
	if first {
		r.announce(ctx, log, wedgeErr)
	}
}

// syncCondition writes the breaker's state as the StorageWedged condition
// and reports whether this call is the one that raised it — the False→True
// transition of the published condition, so an agent restart on a node that
// is still wedged does not re-announce.
func (r *WedgeReporter) syncCondition(ctx context.Context, latched bool, wedgeErr error) (bool, error) {
	first := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		first = false
		cur := &miroirv1alpha1.MiroirNode{}
		if err := r.Client.Get(ctx, types.NamespacedName{Name: r.NodeName}, cur); err != nil {
			return err
		}
		prev := meta.FindStatusCondition(cur.Status.Conditions, miroirv1alpha1.ConditionStorageWedged)
		wasWedged := prev != nil && prev.Status == metav1.ConditionTrue
		if !meta.SetStatusCondition(&cur.Status.Conditions, wedgeCondition(latched, wedgeErr)) {
			return nil
		}
		if err := r.Client.Status().Update(ctx, cur); err != nil {
			return err
		}
		first = wedgeErr != nil && !wasWedged
		return nil
	})
	return first && err == nil, err
}

// wedgeCondition renders the breaker as the StorageWedged condition.
// wedgeErr is Wedge.Err() — nil exactly when the breaker is closed — and
// latched separates the reboot-only kernel fault from a stranded-child
// count that retires itself as those tasks drain.
func wedgeCondition(latched bool, wedgeErr error) metav1.Condition {
	if wedgeErr == nil {
		return metav1.Condition{
			Type:    miroirv1alpha1.ConditionStorageWedged,
			Status:  metav1.ConditionFalse,
			Reason:  miroirv1alpha1.ReasonStorageHealthy,
			Message: "the node's storage command breaker is closed",
		}
	}
	reason := miroirv1alpha1.ReasonStrandedChildren
	if latched {
		reason = miroirv1alpha1.ReasonKernelAssertion
	}
	return metav1.Condition{
		Type:    miroirv1alpha1.ConditionStorageWedged,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: wedgeErr.Error(),
	}
}

// announce records the latch on both objects an operator reaches for: the
// MiroirNode carries miroir's own diagnosis, and the Node is where anyone
// triaging a host that stopped mounting volumes looks first.
func (r *WedgeReporter) announce(ctx context.Context, log logr.Logger, wedgeErr error) {
	if r.Recorder == nil {
		return
	}
	const note = "the node's storage stack has jammed; miroir refuses further storage commands here " +
		"and the volumes with a leg on this node cannot be served from it until it reboots: %s"
	mn := &miroirv1alpha1.MiroirNode{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: r.NodeName}, mn); err != nil {
		log.Error(err, "cannot record the StorageWedged event on the MiroirNode")
	} else {
		r.Recorder.Eventf(mn, nil, corev1.EventTypeWarning,
			miroirv1alpha1.ConditionStorageWedged, "Latch", note, wedgeErr)
	}
	node := &corev1.Node{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: r.NodeName}, node); err != nil {
		log.Error(err, "cannot record the StorageWedged event on the Node")
		return
	}
	r.Recorder.Eventf(node, nil, corev1.EventTypeWarning,
		miroirv1alpha1.ConditionStorageWedged, "Latch", note, wedgeErr)
}

// wedgeTaint is the taint this node wears while its breaker is open.
func wedgeTaint() corev1.Taint {
	return corev1.Taint{
		Key:    constants.TaintStorageWedged,
		Value:  "true",
		Effect: corev1.TaintEffectNoSchedule,
	}
}

// syncTaint adds or removes the storage-wedged taint to match the breaker.
//
// Removal is effectively reboot-gated rather than reconcile-gated, and that
// is the point: a latch is permanent for the process, and a fresh agent on
// an unrebooted node re-latches from the kmsg replay well inside the first
// report interval. So the taint clears only on a node whose kernel log no
// longer carries the assertion — a booted one — with no state of its own to
// persist.
func (r *WedgeReporter) syncTaint(ctx context.Context, wedged bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node := &corev1.Node{}
		if err := r.Client.Get(ctx, types.NamespacedName{Name: r.NodeName}, node); err != nil {
			return err
		}
		want := wedgeTaint()
		i := slices.IndexFunc(node.Spec.Taints, func(t corev1.Taint) bool {
			return t.Key == want.Key
		})
		patch := client.MergeFromWithOptions(node.DeepCopy(), client.MergeFromWithOptimisticLock{})
		switch {
		case wedged && i < 0:
			node.Spec.Taints = append(node.Spec.Taints, want)
		case wedged:
			// Rewrite a drifted taint: NoExecute here would evict the pods
			// whose volumes live on this node, and they cannot start
			// anywhere else until auto-evict re-places those legs.
			if node.Spec.Taints[i].Value == want.Value && node.Spec.Taints[i].Effect == want.Effect {
				return nil
			}
			node.Spec.Taints[i] = want
		case i >= 0:
			node.Spec.Taints = slices.Delete(node.Spec.Taints, i, i+1)
		default:
			return nil
		}
		return r.Client.Patch(ctx, node, patch)
	})
}
