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

package stage

import (
	"context"
	"errors"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

const snapName = "snap-1"

type statusOnlyDRBD struct{}

func (statusOnlyDRBD) Status(context.Context, string) (drbd.Status, error) {
	return drbd.Status{}, nil
}

// restartingDRBD is the resourceRestarter upgrade the recovery needs.
type restartingDRBD struct {
	statusOnlyDRBD
	restarted []string
	err       error
}

func (r *restartingDRBD) Restart(_ context.Context, name string) error {
	r.restarted = append(r.restarted, name)
	return r.err
}

func replicatedVolume() *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			DRBD: &miroirv1alpha1.DRBDSpec{Port: 7000},
		},
	}
}

// errFrozenMount mirrors what FormatAndMount surfaces when the kernel
// refuses the mount over a pinned bdev freeze count (fs/super.c via
// mount(8)'s output).
var errFrozenMount = errors.New(`mount failed: exit status 32
Output: mount: /stage/globalmount: /dev/drbd1378 already mounted or mount point busy.
mount warning:
      * drbd1378: Can't mount, blockdev is frozen`)

func TestRecoverFrozenBdevRestartsAndRetries(t *testing.T) {
	r := &restartingDRBD{}
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, replicatedVolume(), "/dev/drbd1378", errFrozenMount)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("recovery must hand kubelet an Unavailable retry, got %v", err)
	}
	if len(r.restarted) != 1 || r.restarted[0] != "pvc-1" {
		t.Fatalf("the volume's resource must be restarted: %v", r.restarted)
	}
}

func TestRecoverFrozenBdevSurfacesRestartFailure(t *testing.T) {
	r := &restartingDRBD{err: errors.New("resource wedged")}
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, replicatedVolume(), "/dev/drbd1378", errFrozenMount)
	if status.Code(err) != codes.Internal {
		t.Fatalf("a failed restart must surface, got %v", err)
	}
}

func TestRecoverFrozenBdevIgnoresOtherMountErrors(t *testing.T) {
	r := &restartingDRBD{}
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, replicatedVolume(), "/dev/drbd1378",
		errors.New("mount failed: wrong fs type"))
	if err != nil {
		t.Fatalf("an ordinary mount failure is not the recovery's, got %v", err)
	}
	if len(r.restarted) != 0 {
		t.Fatalf("no restart may run for an ordinary mount failure: %v", r.restarted)
	}
}

func TestRecoverFrozenBdevSkipsLocalVolumes(t *testing.T) {
	r := &restartingDRBD{}
	vol := replicatedVolume()
	vol.Spec.DRBD = nil
	if err := recoverFrozenBdev(t.Context(), Deps{DRBD: r}, vol, "/dev/vg-miroir/pvc-1", errFrozenMount); err != nil {
		t.Fatalf("a local volume's device is no DRBD resource to restart, got %v", err)
	}
	if len(r.restarted) != 0 {
		t.Fatalf("no restart may run for a local volume: %v", r.restarted)
	}
}

func TestRecoverFrozenBdevNeedsRestarter(t *testing.T) {
	err := recoverFrozenBdev(t.Context(), Deps{DRBD: statusOnlyDRBD{}}, replicatedVolume(), "/dev/drbd1378", errFrozenMount)
	if err != nil {
		t.Fatalf("a status-only DRBD dep must fall through to the generic wrap, got %v", err)
	}
}

// recordingExec scripts each shell-out and records the binary invoked, so a
// test can assert not just what a path did but that it shelled out at all.
// An empty script panics on the first unexpected command.
func recordingExec(ran *[]string, outs ...struct {
	out string
	err error
},
) *testingexec.FakeExec {
	fe := &testingexec.FakeExec{}
	for _, o := range outs {
		fe.CommandScript = append(fe.CommandScript,
			func(cmd string, args ...string) utilexec.Cmd {
				*ran = append(*ran, cmd)
				c := &testingexec.FakeCmd{CombinedOutputScript: []testingexec.FakeAction{
					func() ([]byte, []byte, error) { return []byte(o.out), nil, o.err },
				}}
				return testingexec.InitFakeCmd(c, cmd, args...)
			})
	}
	return fe
}

// A device the caller already probed as formatted must be mounted without a
// second opinion. FormatAndMount re-probes with blkid and mkfs's whenever that
// probe reads empty — and blkid reports empty both for a blank device and for
// one it could not read, which is what a frozen DRBD device looks like. Going
// through it here is how a populated volume gets reformatted between the
// caller's probe and the mount.
func TestMountStagedFormattedDeviceNeverReprobes(t *testing.T) {
	var ran []string
	fm := mount.NewFakeMounter(nil)
	fe := recordingExec(&ran)
	d := Deps{Mounter: mount.NewSafeFormatAndMount(fm, fe)}

	if err := mountStaged(d, "/dev/drbd1378", "/stage/globalmount", "ext4",
		[]string{"noatime"}, false); err != nil {
		t.Fatalf("mounting an already-formatted device must succeed: %v", err)
	}
	if len(ran) != 0 {
		t.Fatalf("the formatted path must not shell out at all, ran %v", ran)
	}
	if len(fm.MountPoints) != 1 || fm.MountPoints[0].Device != "/dev/drbd1378" {
		t.Fatalf("the device must be mounted: %+v", fm.MountPoints)
	}
	if !slices.Contains(fm.MountPoints[0].Opts, "defaults") ||
		!slices.Contains(fm.MountPoints[0].Opts, "noatime") {
		t.Fatalf("mount options must match what FormatAndMount would have used: %v",
			fm.MountPoints[0].Opts)
	}
}

// The blank path is the only one allowed to reach mkfs.
func TestMountStagedBlankDeviceFormats(t *testing.T) {
	var ran []string
	fm := mount.NewFakeMounter(nil)
	fe := recordingExec(&ran,
		struct {
			out string
			err error
		}{"", utilexec.CodeExitError{Err: errors.New("blkid"), Code: 2}},
		struct {
			out string
			err error
		}{"", nil},
	)
	d := Deps{Mounter: mount.NewSafeFormatAndMount(fm, fe)}

	if err := mountStaged(d, "/dev/drbd1378", "/stage/globalmount", "ext4", nil, true); err != nil {
		t.Fatalf("a blank device must be formatted and mounted: %v", err)
	}
	if !slices.Equal(ran, []string{"blkid", "mkfs.ext4"}) {
		t.Fatalf("the blank path must probe then mkfs, ran %v", ran)
	}
}

// apiReader answers the formatted-flag confirmation from one volume and
// counts the reads, standing in for the uncached API reader.
type apiReader struct {
	vol  *miroirv1alpha1.MiroirVolume
	err  error
	gets int
}

func (r *apiReader) Get(_ context.Context, key client.ObjectKey, obj client.Object,
	_ ...client.GetOption) error {
	r.gets++
	if r.err != nil {
		return r.err
	}
	v, ok := obj.(*miroirv1alpha1.MiroirVolume)
	if !ok || r.vol == nil || key.Name != r.vol.Name {
		return errors.New("no such volume")
	}
	r.vol.DeepCopyInto(v)
	return nil
}

func (r *apiReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

func restoredVolume() *miroirv1alpha1.MiroirVolume {
	vol := replicatedVolume()
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapName}
	return vol
}

// The cache carries a restore as never-formatted for as long as the watch
// takes to deliver the controller's inherited flag; a blank clone staged
// inside that window must be refused, not reformatted.
func TestFormattedBeforeConfirmsRestoreAgainstTheAPI(t *testing.T) {
	live := restoredVolume()
	live.Status.Formatted = true
	r := &apiReader{vol: live}
	formatted, err := formattedBefore(t.Context(), Deps{Reader: r}, restoredVolume())
	if err != nil {
		t.Fatal(err)
	}
	if !formatted {
		t.Fatal("a stale cached flag must lose to the API server's")
	}
	if r.gets != 1 {
		t.Fatalf("the confirmation must read once, read %d times", r.gets)
	}
}

func TestFormattedBeforeFormatsARestoreOfAnUnformattedSource(t *testing.T) {
	r := &apiReader{vol: restoredVolume()}
	formatted, err := formattedBefore(t.Context(), Deps{Reader: r}, restoredVolume())
	if err != nil {
		t.Fatal(err)
	}
	if formatted {
		t.Fatal("a source that never carried a filesystem must still mkfs")
	}
}

func TestFormattedBeforeSkipsTheReadForFreshVolumes(t *testing.T) {
	r := &apiReader{err: errors.New("must not be read")}
	formatted, err := formattedBefore(t.Context(), Deps{Reader: r}, replicatedVolume())
	if err != nil || formatted {
		t.Fatalf("a volume with no content source answers from the cache, got %v %v", formatted, err)
	}
	if r.gets != 0 {
		t.Fatalf("no confirmation read may run for a fresh volume, ran %d", r.gets)
	}
}

func TestFormattedBeforeRefusesToGuessOnAReadFailure(t *testing.T) {
	r := &apiReader{err: errors.New("apiserver unreachable")}
	_, err := formattedBefore(t.Context(), Deps{Reader: r}, restoredVolume())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("an unreadable flag must hand kubelet a retry, got %v", err)
	}
}

func TestXFSCloneMountFlags(t *testing.T) {
	const noatime = "noatime"
	vol := replicatedVolume()
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapName}
	original := []string{noatime}
	got := xfsCloneMountFlags(vol, "xfs", original)
	if !slices.Equal(got, []string{noatime, "nouuid"}) {
		t.Fatalf("flags = %v, want noatime,nouuid", got)
	}
	if !slices.Equal(original, []string{noatime}) {
		t.Fatalf("input flags were mutated: %v", original)
	}
}

func TestXFSCloneMountFlagsSkipsOtherFilesystemsAndSources(t *testing.T) {
	vol := replicatedVolume()
	flags := []string{"relatime"}
	if got := xfsCloneMountFlags(vol, "xfs", flags); !slices.Equal(got, flags) {
		t.Fatalf("non-clone flags = %v, want %v", got, flags)
	}
	vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapName}
	if got := xfsCloneMountFlags(vol, "ext4", flags); !slices.Equal(got, flags) {
		t.Fatalf("ext4 clone flags = %v, want %v", got, flags)
	}
	withNoUUID := []string{"nouuid"}
	if got := xfsCloneMountFlags(vol, "xfs", withNoUUID); !slices.Equal(got, withNoUUID) {
		t.Fatalf("existing nouuid flags = %v, want %v", got, withNoUUID)
	}
}
