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
	"fmt"
	"strings"
	"time"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// probeFailLimit is how many consecutive errored reconcile passes a
// volume tolerates before reportError stops advertising the slot's
// persisted DRBD fields: past it, DiskState degrades to drbd.DiskUnknown
// and Connected to false. An errored pass probed nothing, so beyond a
// couple of retries the persisted healthy claim is one nobody is
// verifying — peers gate replica removal and auto-evict on it, and a
// frozen UpToDate from a hot-looping agent held both closed for hours
// while LastProbedAt quietly aged.
const probeFailLimit = 3

// removalStaleTolerance is how stale a remaining replica's LastProbedAt
// may grow before removalBlocked treats the slot's claims as
// unverifiable instead of authoritative. Deliberately well past
// StaleProbeThreshold (which only degrades the reported phase): removal
// destroys a local backing, so the gate waits out agent restarts and
// node reboots before it starts routing around a silent peer.
const removalStaleTolerance = 3 * StaleProbeThreshold

// staleRemovalReasonFmt is the block reason surfaced on the removed
// replica's status Message when silent peers are tallied and this node
// still holds the only live UpToDate copy.
const staleRemovalReasonFmt = "peer replicas (%s) have not probed within %v and this node holds the only live UpToDate copy; refusing to tear down the last healthy replica"

// nodeListSeparator joins node names in operator-facing messages.
const nodeListSeparator = ", "

// staleRemovalBlockedReason names the tallied silent peers in the
// removal block reason.
func staleRemovalBlockedReason(nodes []string) string {
	return fmt.Sprintf(staleRemovalReasonFmt, strings.Join(nodes, nodeListSeparator), removalStaleTolerance)
}

// bumpProbeFails counts one errored reconcile pass and returns the
// consecutive-failure streak for the volume.
func (r *VolumeReconciler) bumpProbeFails(name string) int {
	r.probeMu.Lock()
	defer r.probeMu.Unlock()
	if r.probeFails == nil {
		r.probeFails = map[string]int{}
	}
	r.probeFails[name]++
	return r.probeFails[name]
}

// clearProbeFails resets the errored-pass streak for a volume — every
// full pass that lands its status patch calls it, and the teardown
// paths clear it for map hygiene.
func (r *VolumeReconciler) clearProbeFails(name string) {
	r.probeMu.Lock()
	delete(r.probeFails, name)
	r.probeMu.Unlock()
}

// staleBeyondRemovalTolerance reports whether the slot last probed
// successfully long enough ago that the removal gate must stop trusting
// its claims. A nil LastProbedAt (a slot written before the field
// existed) is never stale — trust the persisted fields, mirroring
// computePhaseAt.
func staleBeyondRemovalTolerance(st miroirv1alpha1.ReplicaStatus) bool {
	return st.LastProbedAt != nil && time.Since(st.LastProbedAt.Time) > removalStaleTolerance
}

// localCopyHealthy reports whether the leg pending removal is itself a
// live UpToDate copy — the one state where routing around silent peers
// would destroy the last known-good data. Read from the kernel, not the
// local status slot: the realize pipeline stops running once the
// replica leaves the spec, so the slot freezes at whatever it last
// held. A diskless tie-breaker never holds data, and an unreadable
// local state (resource already down, or never brought back up after a
// reboot) is not a healthy copy either.
func (r *VolumeReconciler) localCopyHealthy(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) bool {
	if vol.Status.PerNode[r.NodeName].Diskless {
		return false
	}
	st, err := r.DRBD.Status(ctx, vol.Name)
	return err == nil && st.DiskState == drbd.DiskUpToDate
}
