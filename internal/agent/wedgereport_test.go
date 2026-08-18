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
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/constants"
)

const testAssertion = "kernel log assertion: drbd pvc-1/0 drbd1022: ASSERTION i >= 0 FAILED in put_ldev"

func newReporter(t *testing.T, rec events.EventRecorder) (*WedgeReporter, client.Client) {
	t.Helper()
	s := runtime.NewScheme()
	if err := miroirv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&miroirv1alpha1.MiroirNode{}).
		WithObjects(
			&miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: nodeA}},
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeA}},
		).
		Build()
	return &WedgeReporter{
		Client:   c,
		NodeName: nodeA,
		Wedge:    backend.NewWedge(backend.DefaultWedgeLimit),
		Recorder: rec,
		Taint:    true,
	}, c
}

func wedgedCondition(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	mn := &miroirv1alpha1.MiroirNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, mn); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(mn.Status.Conditions, miroirv1alpha1.ConditionStorageWedged)
}

func taint(t *testing.T, c client.Client) *corev1.Taint {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, node); err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(node.Spec.Taints, func(tt corev1.Taint) bool {
		return tt.Key == constants.TaintStorageWedged
	})
	if i < 0 {
		return nil
	}
	return &node.Spec.Taints[i]
}

// The whole point of the condition: a latched breaker becomes something a
// controller can key off, carrying the kernel's own words.
func TestWedgeReporterPublishesLatch(t *testing.T) {
	rec := events.NewFakeRecorder(8)
	r, c := newReporter(t, rec)
	log := ctrl.LoggerFrom(t.Context())

	r.report(t.Context(), log)
	if cond := wedgedCondition(t, c); cond == nil || cond.Status != metav1.ConditionFalse ||
		cond.Reason != miroirv1alpha1.ReasonStorageHealthy {
		t.Fatalf("a closed breaker must publish StorageWedged=False, got %+v", cond)
	}
	if tt := taint(t, c); tt != nil {
		t.Fatalf("a closed breaker must not taint the node, got %+v", tt)
	}

	r.Wedge.Latch(testAssertion)
	r.report(t.Context(), log)

	cond := wedgedCondition(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("a latched breaker must publish StorageWedged=True, got %+v", cond)
	}
	if cond.Reason != miroirv1alpha1.ReasonKernelAssertion {
		t.Fatalf("reason = %q, want %q", cond.Reason, miroirv1alpha1.ReasonKernelAssertion)
	}
	if !strings.Contains(cond.Message, "put_ldev") {
		t.Fatalf("the message must carry the latched reason, got %q", cond.Message)
	}
	tt := taint(t, c)
	if tt == nil {
		t.Fatal("a latched breaker must taint the node")
	}
	if tt.Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("taint effect = %q, want NoSchedule: NoExecute would evict pods that "+
			"cannot start elsewhere until their legs move", tt.Effect)
	}
	// One Warning apiece on the MiroirNode and the Node.
	for range 2 {
		select {
		case ev := <-rec.Events:
			if !strings.Contains(ev, miroirv1alpha1.ConditionStorageWedged) {
				t.Fatalf("unexpected event %q", ev)
			}
		default:
			t.Fatal("expected a StorageWedged Warning on both the MiroirNode and the Node")
		}
	}
}

// The latch is permanent for the process, so every later tick would re-fire
// the Warning if the announcement were not gated on the published
// transition.
func TestWedgeReporterAnnouncesOnce(t *testing.T) {
	rec := events.NewFakeRecorder(8)
	r, _ := newReporter(t, rec)
	log := ctrl.LoggerFrom(t.Context())

	r.Wedge.Latch(testAssertion)
	r.report(t.Context(), log)
	<-rec.Events
	<-rec.Events

	r.report(t.Context(), log)
	select {
	case ev := <-rec.Events:
		t.Fatalf("a still-latched breaker must not re-announce, got %q", ev)
	default:
	}
}

// An agent restarting on a node that is still wedged finds the condition
// already True: it must neither re-announce nor disturb the transition
// time auto-evict's settle period is measured from.
func TestWedgeReporterRestartKeepsTransition(t *testing.T) {
	rec := events.NewFakeRecorder(8)
	r, c := newReporter(t, rec)
	log := ctrl.LoggerFrom(t.Context())
	r.Wedge.Latch(testAssertion)
	r.report(t.Context(), log)
	<-rec.Events
	<-rec.Events
	first := wedgedCondition(t, c).LastTransitionTime

	// A fresh process against the same API state, re-latched by the kmsg
	// replay before its first report.
	restarted := &WedgeReporter{
		Client:   c,
		NodeName: nodeA,
		Wedge:    backend.NewWedge(backend.DefaultWedgeLimit),
		Recorder: rec,
		Taint:    true,
	}
	restarted.Wedge.Latch(testAssertion)
	restarted.report(t.Context(), log)

	if got := wedgedCondition(t, c).LastTransitionTime; !got.Equal(&first) {
		t.Fatalf("transition time moved across an agent restart: %v → %v", first, got)
	}
	select {
	case ev := <-rec.Events:
		t.Fatalf("an agent restart on a still-wedged node must not re-announce, got %q", ev)
	default:
	}
}

// A breaker that closes again (its stranded children drained) must take the
// condition and the taint with it — state written only on the way up would
// keep the node unschedulable after it recovered.
func TestWedgeReporterClearsOnRecovery(t *testing.T) {
	r, c := newReporter(t, nil)
	log := ctrl.LoggerFrom(t.Context())
	node := &corev1.Node{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, node); err != nil {
		t.Fatal(err)
	}
	node.Spec.Taints = []corev1.Taint{
		{Key: "other", Value: "keep", Effect: corev1.TaintEffectNoSchedule},
		wedgeTaint(),
	}
	if err := c.Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}

	r.report(t.Context(), log)

	if cond := wedgedCondition(t, c); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("a closed breaker must clear the condition, got %+v", cond)
	}
	if tt := taint(t, c); tt != nil {
		t.Fatalf("a closed breaker must remove the taint, got %+v", tt)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, node); err != nil {
		t.Fatal(err)
	}
	if len(node.Spec.Taints) != 1 || node.Spec.Taints[0].Key != "other" {
		t.Fatalf("foreign taints must survive, got %+v", node.Spec.Taints)
	}
}

// Some operators run their own node remediation and will not have miroir
// writing to Node objects; the condition is published either way.
func TestWedgeReporterTaintOptOut(t *testing.T) {
	r, c := newReporter(t, nil)
	r.Taint = false
	r.Wedge.Latch(testAssertion)

	r.report(t.Context(), ctrl.LoggerFrom(t.Context()))

	if tt := taint(t, c); tt != nil {
		t.Fatalf("wedgeTaint disabled must leave the node untainted, got %+v", tt)
	}
	if cond := wedgedCondition(t, c); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("the condition is published regardless of the taint, got %+v", cond)
	}
}

// A taint whose effect drifted is rewritten: NoExecute would evict the pods
// whose volumes live here, and they cannot start anywhere else yet.
func TestWedgeReporterRewritesDriftedTaint(t *testing.T) {
	r, c := newReporter(t, nil)
	node := &corev1.Node{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, node); err != nil {
		t.Fatal(err)
	}
	node.Spec.Taints = []corev1.Taint{{
		Key: constants.TaintStorageWedged, Value: "true", Effect: corev1.TaintEffectNoExecute,
	}}
	if err := c.Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	r.Wedge.Latch(testAssertion)

	r.report(t.Context(), ctrl.LoggerFrom(t.Context()))

	tt := taint(t, c)
	if tt == nil || tt.Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("a drifted taint must be rewritten to NoSchedule, got %+v", tt)
	}
}

func TestWedgeCondition(t *testing.T) {
	tests := []struct {
		name    string
		latched bool
		err     error
		status  metav1.ConditionStatus
		reason  string
	}{
		{"closed", false, nil, metav1.ConditionFalse, miroirv1alpha1.ReasonStorageHealthy},
		{"latched", true, errors.New("boom"), metav1.ConditionTrue, miroirv1alpha1.ReasonKernelAssertion},
		{"stranded", false, errors.New("boom"), metav1.ConditionTrue, miroirv1alpha1.ReasonStrandedChildren},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cond := wedgeCondition(tc.latched, tc.err)
			if cond.Status != tc.status || cond.Reason != tc.reason {
				t.Fatalf("got %s/%s, want %s/%s", cond.Status, cond.Reason, tc.status, tc.reason)
			}
			if cond.Message == "" {
				t.Fatal("the condition must always carry a message")
			}
		})
	}
}
