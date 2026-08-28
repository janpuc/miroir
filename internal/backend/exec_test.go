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

package backend

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The deadline alone cannot bound the wait: Cmd.Wait blocks in wait4 until
// the child is reaped, and a task in uninterruptible sleep never answers the
// SIGKILL. awaitChild is what turns that into a returning call, so its three
// outcomes are pinned here rather than left to a D-state process no test can
// portably create.
func TestAwaitChild(t *testing.T) {
	errChild := errors.New("exit status 1")

	for _, tc := range []struct {
		name          string
		deliver       error
		deliverAfter  time.Duration // relative to the call; <0 means never
		ctxTimeout    time.Duration
		grace         time.Duration
		wantErr       error
		wantAbandoned bool
	}{
		{
			name:         "returns before the deadline",
			deliver:      nil,
			deliverAfter: 0,
			ctxTimeout:   time.Minute,
			grace:        time.Minute,
		},
		{
			name:         "propagates the child's own error",
			deliver:      errChild,
			deliverAfter: 0,
			ctxTimeout:   time.Minute,
			grace:        time.Minute,
			wantErr:      errChild,
		},
		{
			name:         "a killed child that dies inside the grace is not abandoned",
			deliver:      errChild,
			deliverAfter: 20 * time.Millisecond,
			ctxTimeout:   10 * time.Millisecond,
			grace:        time.Minute,
			wantErr:      errChild,
		},
		{
			name:          "a child that never dies is abandoned once the grace expires",
			deliverAfter:  -1,
			ctxTimeout:    10 * time.Millisecond,
			grace:         20 * time.Millisecond,
			wantAbandoned: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			if tc.deliverAfter >= 0 {
				go func() {
					time.Sleep(tc.deliverAfter)
					done <- tc.deliver
				}()
			}
			ctx, cancel := context.WithTimeout(context.Background(), tc.ctxTimeout)
			defer cancel()

			err, abandoned := awaitChild(ctx, done, tc.grace)

			if abandoned != tc.wantAbandoned {
				t.Fatalf("awaitChild() abandoned = %v, want %v", abandoned, tc.wantAbandoned)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("awaitChild() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// A caller freed by the abandon path must never be handed back to the fast
// busy retry: the device is pinned in the kernel and each retry strands
// another task.
func TestBusyDoesNotReclassifyAbandoned(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   error
		want error
	}{
		{
			name: "an abandoned child stays abandoned",
			in:   ErrAbandoned,
			want: ErrAbandoned,
		},
		{
			// The captured output of a held-open failure survives into the
			// abandoned error, so the substring match would otherwise win.
			in:   errors.Join(ErrAbandoned, errors.New("Device is held open by someone")),
			name: "held-open text does not make an abandoned child busy",
			want: ErrAbandoned,
		},
		{
			name: "an ordinary held-open failure is still busy",
			in:   errors.New("Device is held open by someone"),
			want: ErrBusy,
		},
		{
			name: "nil stays nil",
			in:   nil,
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Busy(tc.in)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("Busy() = %v, want nil", got)
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("Busy() = %v, want it to wrap %v", got, tc.want)
			}
			if tc.want == ErrAbandoned && errors.Is(got, ErrBusy) {
				t.Fatalf("Busy() = %v, must not classify an abandoned child as retryable", got)
			}
		})
	}
}
