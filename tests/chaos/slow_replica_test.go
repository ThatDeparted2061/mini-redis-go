package chaos

// Chaos test: a 500ms-delayed replica must NOT throttle the primary.
//
// mini-redis ships writes to replicas with a non-blocking enqueue and drops-and-
// logs on a full queue (see replication.Propagate) — precisely so a slow replica
// can never back-pressure the write path. This test proves it under a REAL
// network delay rather than a mock: the replica runs in its own network
// namespace whose veth carries a `tc netem delay 500ms`, so every replicated
// frame is delayed but the client<->primary loopback path stays fast. We then
// blast 100k writes at the primary and assert its throughput is unchanged.
//
// Requires Linux + root (network namespaces and tc need CAP_NET_ADMIN) + the
// iproute2 tools + the sch_netem kernel module. Anywhere else it SKIPS — a
// missing capability is an environment gap, not a failure; only genuine
// throttling fails the test.

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestSlowReplicaDoesNotThrottlePrimary(t *testing.T) {
	requireNetnsCapable(t)

	const n = 100000

	// --- Baseline: primary with NO replica, measure raw write throughput. ---
	primary := startServer(t, "--appendonly=false")
	primary.waitReady(t)
	pc := primary.client()
	defer pc.Close()

	base := time.Now()
	writeBurst(t, pc, "base", n)
	baseDur := time.Since(base)
	t.Logf("baseline (no replica): %d writes in %s (%.0f ops/s)", n, baseDur.Round(time.Millisecond), float64(n)/baseDur.Seconds())

	// --- Attach a replica behind a 500ms-delayed link. ---
	nsr := setupDelayedNetns(t, 500)
	replica := startServerNetns(t, nsr.ns, nsr.peerIP,
		"--appendonly=false", "--replicaof", nsr.hostIP+" "+primary.port)
	replica.waitReady(t)
	// Confirm the replica really attached to the stream (one write round-trips
	// through the delay), so we know the slow feed is live during the blast.
	waitReplicaLive(t, primary, replica, "_live_slow")

	// --- Same blast, now with the slow replica draining behind us. ---
	slow := time.Now()
	writeBurst(t, pc, "slow", n)
	slowDur := time.Since(slow)
	t.Logf("with 500ms replica: %d writes in %s (%.0f ops/s)", n, slowDur.Round(time.Millisecond), float64(n)/slowDur.Seconds())

	// If the primary blocked on the replica it would take ~n*500ms ≈ 14 hours;
	// this absolute ceiling makes "not throttled" unambiguous.
	if slowDur > 30*time.Second {
		t.Fatalf("primary throttled by slow replica: %d writes took %s", n, slowDur)
	}
	// And it should stay in the same ballpark as the baseline (5x slack absorbs CI
	// noise while still catching any real back-pressure, which would be ~1000x).
	if slowDur > 5*baseDur {
		t.Fatalf("primary throughput collapsed with a slow replica: baseline %s vs %s", baseDur, slowDur)
	}
}

// requireNetnsCapable skips the test unless we can actually build a delayed
// namespace: Linux, root, and the ip/tc tools present.
func requireNetnsCapable(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("slow-replica test needs Linux network namespaces + tc")
	}
	if euid := os.Geteuid(); euid != 0 {
		t.Skipf("slow-replica test needs root for netns/tc (euid=%d); run the chaos job as root", euid)
	}
	for _, tool := range []string{"ip", "tc"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("slow-replica test needs %q in PATH: %v", tool, err)
		}
	}
}

// netnsRig is a network namespace wired to the host by a veth pair, with a
// netem delay on the host-side interface.
type netnsRig struct {
	ns     string // namespace name
	hostIP string // host end of the veth (the replica dials this as its primary)
	peerIP string // namespace end (a host client dials this to reach the replica)
}

// setupDelayedNetns builds the rig and registers teardown. On any setup error it
// tears down what it built and SKIPS — an environment that can't do netns/tc
// shouldn't fail the suite.
func setupDelayedNetns(t *testing.T, delayMs int) netnsRig {
	t.Helper()
	suffix := randSuffix()
	rig := netnsRig{
		ns:     "chaos" + suffix,
		hostIP: "10.211.55.1",
		peerIP: "10.211.55.2",
	}
	hveth := "cv" + suffix + "h" // host side  (<=15 chars: 2+6+1)
	pveth := "cv" + suffix + "p" // peer side (moved into the namespace)

	// Best-effort teardown: deleting the host veth removes its peer too, and
	// deleting the namespace cleans up anything left inside it.
	cleanup := func() {
		_ = exec.Command("ip", "link", "del", hveth).Run()
		_ = exec.Command("ip", "netns", "del", rig.ns).Run()
	}
	t.Cleanup(cleanup)

	steps := [][]string{
		{"ip", "netns", "add", rig.ns},
		{"ip", "link", "add", hveth, "type", "veth", "peer", "name", pveth},
		{"ip", "link", "set", pveth, "netns", rig.ns},
		{"ip", "addr", "add", rig.hostIP + "/24", "dev", hveth},
		{"ip", "link", "set", hveth, "up"},
		{"ip", "netns", "exec", rig.ns, "ip", "addr", "add", rig.peerIP + "/24", "dev", pveth},
		{"ip", "netns", "exec", rig.ns, "ip", "link", "set", pveth, "up"},
		{"ip", "netns", "exec", rig.ns, "ip", "link", "set", "lo", "up"},
		// Delay only the host->namespace direction (the replication feed); the
		// replica's ACKs come back on the peer side, undelayed.
		{"tc", "qdisc", "add", "dev", hveth, "root", "netem", "delay", fmt.Sprintf("%dms", delayMs)},
	}
	for _, s := range steps {
		if out, err := exec.Command(s[0], s[1:]...).CombinedOutput(); err != nil {
			cleanup()
			t.Skipf("could not set up delayed netns (%v): %s\n%s", s, err, out)
		}
	}
	return rig
}
