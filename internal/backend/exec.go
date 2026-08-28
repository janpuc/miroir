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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Exec runs a CLI command and returns its combined output. Injectable so
// backends are unit-testable without lvm/zfs installed.
type Exec func(ctx context.Context, name string, args ...string) (string, error)

// execTimeout bounds a single host command. Every miroir CLI call (lvm,
// zfs, drbdadm, drbdsetup, losetup, blockdev) is a metadata operation
// that completes in well under a second on healthy hardware; the only way
// to exceed this is a genuinely wedged device or pool. Reconcile contexts
// have no deadline of their own, so without this bound a child stuck in
// D-state pins the single reconcile worker forever and head-of-line-blocks
// every other volume on the node.
const execTimeout = 2 * time.Minute

// abandonGrace is how long Run keeps waiting for a killed child after its
// deadline fired before giving up on it entirely.
//
// The deadline alone does not bound Run. exec.CommandContext sends SIGKILL
// when ctx is done, but a task in uninterruptible sleep never receives it,
// and Cmd.Wait calls Process.Wait (wait4) before it consults WaitDelay — so
// Wait blocks until the child is reaped no matter what WaitDelay says.
// WaitDelay only ever bounds the I/O copier goroutines, which Wait reaches
// afterwards. Abandoning the child is the only thing that frees the caller:
// otherwise one unkillable drbdsetup pins a reconcile worker for the life of
// the process, and because the volume never reaches a reporting path it does
// so with no log line, no Event and no status write.
const abandonGrace = 10 * time.Second

// ErrAbandoned marks a command Run stopped waiting for: the deadline fired,
// the SIGKILL went unanswered, and the child is still in the kernel holding
// whatever locks it took. The caller's goroutine is released; the child is
// not. Callers must park rather than retry — a fast retry only spawns
// another task that strands the same way.
var ErrAbandoned = errors.New("command abandoned in uninterruptible sleep: node reboot required")

// syncBuffer serialises the writes exec's copier goroutines make against
// the reads Run does. CombinedOutput can share one plain bytes.Buffer
// because Wait has joined those goroutines before it returns; an abandoned
// child keeps its pipes open, so here they outlive Run and the buffer needs
// its own lock.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// RealExec executes commands on the host without wedge tracking. The agent
// container runs with the host namespaces, so lvm/zfs act on the node's
// devices directly. Callers with a breaker to share use Runner instead.
func RealExec(ctx context.Context, name string, args ...string) (string, error) {
	return (&Runner{}).Run(ctx, name, args...)
}

// Runner executes host commands, reporting every child the kernel refuses to
// let die to a node-scoped Wedge. Its Run method has the Exec signature, so
// it drops in wherever RealExec is injected.
type Runner struct {
	// Wedge both gates and records: commands are refused once it has
	// tripped, and stranded children are reported to it. Nil disables both.
	Wedge *Wedge
}

// Run executes name with args and returns its combined output. Once the
// breaker is open it refuses to spawn: the new child would strand too, and
// each one holds more locks and pushes the node further from a graceful
// reboot.
func (r *Runner) Run(ctx context.Context, name string, args ...string) (string, error) {
	line := name + " " + strings.Join(args, " ")
	if err := r.Wedge.Err(); err != nil {
		return "", fmt.Errorf("%s: %w", strings.TrimSpace(line), err)
	}
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	// Bounds the copier goroutines once ctx is done, for the children that
	// do die: without it they hold the pipes open and Wait never returns
	// even after wait4 has. It cannot bound wait4 itself — see abandonGrace.
	cmd.WaitDelay = abandonGrace
	// Force the C locale: the delete/exists classifiers match lvm/zfs error
	// text ("in use", "Failed to find", …), which the tools localise.
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	var out syncBuffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("%s: %w", line, err)
	}
	// Read the pid now: after the abandon path returns, cmd is owned by the
	// goroutine below and touching cmd.Process races it.
	pid := cmd.Process.Pid
	// Buffered so the goroutine can finish and exit whenever the kernel
	// finally releases the child, long after Run has abandoned it.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	waitErr, abandoned := awaitChild(ctx, done, abandonGrace)
	if abandoned {
		// Recorded here rather than after the wait: an abandoned child is
		// precisely what the breaker counts, and no code downstream of the
		// wait can observe it — the wait is what never returns.
		r.Wedge.note(pid, strings.TrimSpace(line))
		return out.String(), fmt.Errorf("%s: %w: %s",
			line, ErrAbandoned, strings.TrimSpace(out.String()))
	}
	return r.result(ctx, pid, line, out.String(), waitErr)
}

// awaitChild waits for done to carry the child's Wait result, allowing grace
// past ctx's own expiry before reporting the child abandoned. Splitting the
// two selects is what bounds Run: the outer one returns as soon as the child
// does, and the inner one caps how long a child that ignored its SIGKILL can
// hold the caller.
func awaitChild(ctx context.Context, done <-chan error, grace time.Duration) (err error, abandoned bool) {
	select {
	case err := <-done:
		return err, false
	case <-ctx.Done():
		// The kill is queued. A killable child dies in microseconds, so
		// anything still here past the grace is not coming back.
		select {
		case err := <-done:
			return err, false
		case <-time.After(grace):
			return nil, true
		}
	}
}

// result shapes one completed child's outcome, shared by the two paths that
// can observe a Wait return so their error text and strand bookkeeping
// cannot drift apart.
func (r *Runner) result(ctx context.Context, pid int, line, out string, err error) (string, error) {
	// Only a command we killed can have stranded a child, and a killable one
	// is already gone by here. A child that did die frees its pid, so this
	// read can in principle land on an unrelated task now holding it in D;
	// that misread cannot latch, since Stranded re-checks every pid and
	// tripping needs Limit outstanding at once.
	if err != nil && ctx.Err() != nil {
		r.Wedge.note(pid, strings.TrimSpace(line))
	}
	if err != nil {
		return out, fmt.Errorf("%s: %w: %s", line, err, strings.TrimSpace(out))
	}
	return out, nil
}

// Busy classifies a delete/destroy/down failure: it wraps err as ErrBusy
// when the cause clears on its own — the device is still open, or (zfs)
// snapshots or restore clones must go first — so the caller retries. Other
// errors pass through unchanged and are treated as permanent. Returns nil
// for nil. Exported so the agent can classify drbdsetup down failures the
// same way (a still-staged device answers "held open").
func Busy(err error) error {
	if err == nil {
		return nil
	}
	// An abandoned child is never busy-retryable: the device it held is
	// pinned in the kernel and the retry would strand another task.
	if errors.Is(err, ErrAbandoned) {
		return err
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "held open"),
		strings.Contains(s, "busy"),
		strings.Contains(s, "in use"),
		strings.Contains(s, "has children"),     // zfs: snapshots exist
		strings.Contains(s, "dependent clones"): // zfs: restore clones exist
		return fmt.Errorf("%w: %v", ErrBusy, err)
	}
	return err
}
