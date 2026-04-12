from __future__ import annotations

import argparse
from collections import Counter
import ctypes
from dataclasses import dataclass
from itertools import cycle
import os
import sys
import time


def load_store_class():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    local_asio = os.path.join(root, "build", "mooncake-asio", "libasio.so")
    local_integration = os.path.join(root, "build", "mooncake-integration")

    if os.path.exists(local_asio) and os.path.isdir(local_integration):
        ctypes.CDLL(local_asio, mode=ctypes.RTLD_GLOBAL)
        sys.path.insert(0, local_integration)
        import store

        return store.MooncakeDistributedStore

    from mooncake.store import MooncakeDistributedStore

    return MooncakeDistributedStore


MooncakeDistributedStore = load_store_class()


@dataclass(frozen=True)
class PolicyProfile:
    prefix_count: int
    prefix_chunks: int
    session_count: int
    session_kv_chunks: int
    decode_steps: int
    decode_growth_stride: int
    prefix_bytes: int
    session_bytes: int
    global_segment_size: int
    local_buffer_size: int
    session_pause_ms: int
    pressure_rounds: int
    burst_session_count: int
    pressure_round_pause_ms: int
    lease_age_wait_ms: int
    verify_rounds: int
    phase_shift_rounds: int
    allow_put_failures: bool
    base_pattern: tuple[int, ...]
    shift_pattern: tuple[int, ...]


PROFILES = {
    "steady_skew": PolicyProfile(
        prefix_count=6,
        prefix_chunks=2,
        session_count=8,
        session_kv_chunks=2,
        decode_steps=4,
        decode_growth_stride=2,
        prefix_bytes=512 * 1024,
        session_bytes=704 * 1024,
        global_segment_size=34 * 1024 * 1024,
        local_buffer_size=34 * 1024 * 1024,
        session_pause_ms=40,
        pressure_rounds=2,
        burst_session_count=3,
        pressure_round_pause_ms=120,
        lease_age_wait_ms=6500,
        verify_rounds=3,
        phase_shift_rounds=0,
        allow_put_failures=False,
        base_pattern=(0, 0, 0, 1, 1, 2, 3, 4, 5),
        shift_pattern=(),
    ),
    "burst_pressure": PolicyProfile(
        prefix_count=6,
        prefix_chunks=3,
        session_count=6,
        session_kv_chunks=3,
        decode_steps=6,
        decode_growth_stride=2,
        prefix_bytes=512 * 1024,
        session_bytes=1024 * 1024,
        global_segment_size=36 * 1024 * 1024,
        local_buffer_size=36 * 1024 * 1024,
        session_pause_ms=30,
        pressure_rounds=4,
        burst_session_count=5,
        pressure_round_pause_ms=80,
        lease_age_wait_ms=6500,
        verify_rounds=4,
        phase_shift_rounds=0,
        allow_put_failures=True,
        base_pattern=(0, 0, 0, 0, 1, 1, 2, 2, 3, 4, 5),
        shift_pattern=(),
    ),
    "phase_shift": PolicyProfile(
        prefix_count=8,
        prefix_chunks=2,
        session_count=6,
        session_kv_chunks=2,
        decode_steps=4,
        decode_growth_stride=2,
        prefix_bytes=512 * 1024,
        session_bytes=768 * 1024,
        global_segment_size=32 * 1024 * 1024,
        local_buffer_size=32 * 1024 * 1024,
        session_pause_ms=40,
        pressure_rounds=2,
        burst_session_count=2,
        pressure_round_pause_ms=120,
        lease_age_wait_ms=6500,
        verify_rounds=3,
        phase_shift_rounds=2,
        allow_put_failures=False,
        base_pattern=(0, 0, 0, 1, 1, 2, 3),
        shift_pattern=(4, 4, 4, 5, 5, 6, 7),
    ),
}


@dataclass
class SuiteStats:
    puts: int = 0
    put_failures: int = 0
    gets: int = 0
    hits: int = 0
    misses: int = 0
    prefix_hits: int = 0
    prefix_gets: int = 0
    session_hits: int = 0
    session_gets: int = 0
    cold_hits: int = 0
    cold_gets: int = 0

    def record_get(self, hit: bool, category: str) -> None:
        self.gets += 1
        if hit:
            self.hits += 1
        else:
            self.misses += 1

        if category == "prefix":
            self.prefix_gets += 1
            if hit:
                self.prefix_hits += 1
        elif category == "session":
            self.session_gets += 1
            if hit:
                self.session_hits += 1
        elif category == "cold":
            self.cold_gets += 1
            if hit:
                self.cold_hits += 1

    @property
    def overall_hit_rate(self) -> float:
        return self.hits / self.gets if self.gets else 0.0

    @property
    def prefix_hit_rate(self) -> float:
        return self.prefix_hits / self.prefix_gets if self.prefix_gets else 0.0

    @property
    def session_hit_rate(self) -> float:
        return self.session_hits / self.session_gets if self.session_gets else 0.0

    @property
    def cold_miss_rate(self) -> float:
        misses = self.cold_gets - self.cold_hits
        return misses / self.cold_gets if self.cold_gets else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Policy-oriented Mooncake cache benchmark with skew, bursts, and phase shifts."
    )
    parser.add_argument("--profile", choices=sorted(PROFILES), default="steady_skew")
    parser.add_argument("--metadata-url", default="http://localhost:8080/metadata")
    parser.add_argument("--master-address", default="localhost:50051")
    parser.add_argument("--node-address", default="localhost")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--run-id", default=f"policy-{int(time.time())}")
    parser.add_argument("--global-segment-size", type=int)
    parser.add_argument("--local-buffer-size", type=int)
    parser.add_argument("--put-retries", type=int, default=3)
    parser.add_argument("--put-retry-sleep-ms", type=int, default=200)
    parser.add_argument("--put-failure-cooldown-ms", type=int, default=400)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def profile_value(args: argparse.Namespace, field: str):
    override = getattr(args, field, None)
    if override is not None:
        return override
    return getattr(PROFILES[args.profile], field)


def namespaced_key(args: argparse.Namespace, suffix: str) -> str:
    return f"policy/{args.run_id}/{suffix}"


def phase(name: str, detail: str) -> None:
    print(f"{name}: {detail}")


def prefix_keys_for(args: argparse.Namespace, prefix_id: int, cfg: dict[str, int]) -> list[str]:
    return [
        namespaced_key(args, f"prefix-{prefix_id}/chunk-{chunk_idx}")
        for chunk_idx in range(cfg["prefix_chunks"])
    ]


def select_prefix_sequence(pattern: tuple[int, ...], prefix_count: int) -> cycle:
    normalized = tuple(prefix_id % prefix_count for prefix_id in pattern)
    return cycle(normalized if normalized else tuple(range(prefix_count)))


def get_hit(
    store: MooncakeDistributedStore, key: str, stats: SuiteStats, category: str
) -> bool:
    hit = store.get(key) != b""
    stats.record_get(hit, category)
    return hit


def put_with_retries(
    store: MooncakeDistributedStore,
    key: str,
    value: bytes,
    stats: SuiteStats,
    args: argparse.Namespace,
) -> bool:
    for _ in range(args.put_retries):
        rc = store.put(key, value)
        if rc == 0:
            stats.puts += 1
            return True
        stats.put_failures += 1
        time.sleep(args.put_retry_sleep_ms / 1000.0)
    time.sleep(args.put_failure_cooldown_ms / 1000.0)
    return False


def prefill_prefixes(
    store: MooncakeDistributedStore,
    cfg: dict[str, int],
    stats: SuiteStats,
    args: argparse.Namespace,
) -> list[str]:
    payload = b"P" * cfg["prefix_bytes"]
    prefix_keys: list[str] = []
    for prefix_id in range(cfg["prefix_count"]):
        for key in prefix_keys_for(args, prefix_id, cfg):
            if put_with_retries(store, key, payload, stats, args):
                prefix_keys.append(key)
    return prefix_keys


def run_session(
    store: MooncakeDistributedStore,
    session_id: int,
    prefix_id: int,
    cfg: dict[str, int],
    stats: SuiteStats,
    args: argparse.Namespace,
) -> list[str]:
    payload = b"S" * cfg["session_bytes"]
    active_keys: list[str] = []

    for chunk_idx in range(cfg["session_kv_chunks"]):
        key = namespaced_key(args, f"session-{session_id}/kv-{chunk_idx}")
        if put_with_retries(store, key, payload, stats, args):
            active_keys.append(key)

    prefix_keys = prefix_keys_for(args, prefix_id, cfg)
    for key in prefix_keys:
        get_hit(store, key, stats, "prefix")

    for step in range(cfg["decode_steps"]):
        if prefix_keys:
            get_hit(store, prefix_keys[step % len(prefix_keys)], stats, "prefix")
        if active_keys:
            get_hit(store, active_keys[-1], stats, "session")
        if len(active_keys) > 1:
            warm_key = active_keys[(session_id + step) % len(active_keys)]
            get_hit(store, warm_key, stats, "session")

        if (step + 1) % cfg["decode_growth_stride"] == 0:
            key = namespaced_key(args, f"session-{session_id}/kv-{len(active_keys)}")
            if put_with_retries(store, key, payload, stats, args):
                active_keys.append(key)

    return active_keys


def probe_prefix_group(
    store: MooncakeDistributedStore,
    prefix_ids: list[int],
    cfg: dict[str, int],
    stats: SuiteStats,
    args: argparse.Namespace,
) -> tuple[int, int]:
    hits = 0
    total = 0
    for _ in range(cfg["verify_rounds"]):
        for prefix_id in prefix_ids:
            for key in prefix_keys_for(args, prefix_id, cfg):
                total += 1
                if get_hit(store, key, stats, "prefix"):
                    hits += 1
    return hits, total


def ranked_prefix_ids(counter: Counter[int], prefix_count: int) -> list[int]:
    scored = [(counter.get(prefix_id, 0), prefix_id) for prefix_id in range(prefix_count)]
    return [prefix_id for _, prefix_id in sorted(scored, key=lambda item: (-item[0], item[1]))]


def probe_cold_keys(
    store: MooncakeDistributedStore, cold_keys: list[str], stats: SuiteStats
) -> tuple[int, int]:
    hits = 0
    total = 0
    for key in cold_keys:
        total += 1
        if get_hit(store, key, stats, "cold"):
            hits += 1
    return hits, total


def estimate_bytes(cfg: dict[str, int]) -> tuple[int, int]:
    prefix_bytes = cfg["prefix_count"] * cfg["prefix_chunks"] * cfg["prefix_bytes"]
    per_session_chunks = cfg["session_kv_chunks"] + (
        cfg["decode_steps"] // cfg["decode_growth_stride"]
    )
    base = prefix_bytes + cfg["session_count"] * per_session_chunks * cfg["session_bytes"]
    burst = (
        cfg["pressure_rounds"]
        * cfg["burst_session_count"]
        * per_session_chunks
        * cfg["session_bytes"]
    )
    return base, base + burst


def print_summary(
    args: argparse.Namespace,
    cfg: dict[str, int],
    profile: PolicyProfile,
    stats: SuiteStats,
    base_counter: Counter[int],
    shift_counter: Counter[int],
    session_key_count: int,
    top_hits: int,
    top_total: int,
    tail_hits: int,
    tail_total: int,
    old_hot_hits: int,
    old_hot_total: int,
    new_hot_hits: int,
    new_hot_total: int,
    cold_hits: int,
    cold_total: int,
) -> None:
    print()
    print(f"Profile: {args.profile}")
    print(f"Run ID: {args.run_id}")
    print(f"Endpoints: metadata={args.metadata_url}, master={args.master_address}")
    print(
        "Reuse: base="
        + ", ".join(f"prefix-{pid}={count}" for pid, count in sorted(base_counter.items()))
    )
    if shift_counter:
        print(
            "Shift reuse: "
            + ", ".join(f"prefix-{pid}={count}" for pid, count in sorted(shift_counter.items()))
        )
    print(
        "Ops: "
        f"puts={stats.puts}, put_failures={stats.put_failures}, "
        f"gets={stats.gets}, hits={stats.hits}, misses={stats.misses}"
    )
    print(
        "Hit rates: "
        f"overall={stats.overall_hit_rate:.1%}, "
        f"prefix={stats.prefix_hit_rate:.1%}, "
        f"session={stats.session_hit_rate:.1%}, "
        f"cold_miss={stats.cold_miss_rate:.1%}"
    )
    if args.profile == "phase_shift":
        print(
            "Phase shift: "
            f"old_hot={old_hot_hits}/{old_hot_total}, "
            f"new_hot={new_hot_hits}/{new_hot_total}"
        )
    else:
        print(
            "Prefix survival: "
            f"top={top_hits}/{top_total}, tail={tail_hits}/{tail_total}"
        )
    print(
        "Objects: "
        f"session_keys={session_key_count}, cold_probe_hits={cold_hits}/{cold_total}"
    )

    write_state = "expected-pressure" if profile.allow_put_failures else (
        "ok" if stats.put_failures == 0 else "failed"
    )
    if args.profile == "phase_shift":
        signal_ok = new_hot_total > 0 and new_hot_hits >= old_hot_hits
    else:
        signal_ok = top_total > 0 and tail_total > 0 and top_hits > tail_hits
    overall_ok = signal_ok and (profile.allow_put_failures or stats.put_failures == 0)

    print(
        "Result: "
        f"{'PASS' if overall_ok else 'FAIL'} "
        f"(writes={write_state}, policy_signal={'ok' if signal_ok else 'weak'})"
    )
    if args.verbose:
        print(f"Config: {cfg}")


def main() -> None:
    args = parse_args()
    profile = PROFILES[args.profile]
    cfg = {
        "prefix_count": profile_value(args, "prefix_count"),
        "prefix_chunks": profile_value(args, "prefix_chunks"),
        "session_count": profile_value(args, "session_count"),
        "session_kv_chunks": profile_value(args, "session_kv_chunks"),
        "decode_steps": profile_value(args, "decode_steps"),
        "decode_growth_stride": profile_value(args, "decode_growth_stride"),
        "prefix_bytes": profile_value(args, "prefix_bytes"),
        "session_bytes": profile_value(args, "session_bytes"),
        "global_segment_size": profile_value(args, "global_segment_size"),
        "local_buffer_size": profile_value(args, "local_buffer_size"),
        "session_pause_ms": profile_value(args, "session_pause_ms"),
        "pressure_rounds": profile_value(args, "pressure_rounds"),
        "burst_session_count": profile_value(args, "burst_session_count"),
        "pressure_round_pause_ms": profile_value(args, "pressure_round_pause_ms"),
        "lease_age_wait_ms": profile_value(args, "lease_age_wait_ms"),
        "verify_rounds": profile_value(args, "verify_rounds"),
        "phase_shift_rounds": profile_value(args, "phase_shift_rounds"),
    }

    steady_estimate, peak_estimate = estimate_bytes(cfg)
    phase(
        "Phase 0",
        f"profile={args.profile}, segment={cfg['global_segment_size'] // (1024 * 1024)}MB, "
        f"steady_estimate={steady_estimate // (1024 * 1024)}MB, "
        f"peak_estimate={peak_estimate // (1024 * 1024)}MB, "
        f"lease_age_wait={cfg['lease_age_wait_ms']}ms",
    )

    store = MooncakeDistributedStore()
    rc = store.setup(
        args.node_address,
        args.metadata_url,
        cfg["global_segment_size"],
        cfg["local_buffer_size"],
        args.protocol,
        args.rdma_devices,
        args.master_address,
    )
    if rc != 0:
        raise RuntimeError(f"store.setup failed with rc={rc}")

    stats = SuiteStats()
    base_counter: Counter[int] = Counter()
    shift_counter: Counter[int] = Counter()
    session_state: dict[int, list[str]] = {}

    top_hits = top_total = tail_hits = tail_total = 0
    old_hot_hits = old_hot_total = new_hot_hits = new_hot_total = 0
    cold_hits = cold_total = 0

    try:
        prefix_keys = prefill_prefixes(store, cfg, stats, args)
        phase(
            "Phase 1",
            f"prefix prefill wrote {len(prefix_keys)}/{cfg['prefix_count'] * cfg['prefix_chunks']} objects",
        )

        base_seq = select_prefix_sequence(profile.base_pattern, cfg["prefix_count"])
        next_session_id = 0
        for _ in range(cfg["session_count"]):
            prefix_id = next(base_seq)
            base_counter[prefix_id] += 1
            session_state[next_session_id] = run_session(
                store, next_session_id, prefix_id, cfg, stats, args
            )
            next_session_id += 1
            time.sleep(cfg["session_pause_ms"] / 1000.0)
        phase(
            "Phase 2",
            f"base_sessions={cfg['session_count']}, prefix_hit_rate={stats.prefix_hit_rate:.1%}, session_keys={sum(len(v) for v in session_state.values())}",
        )

        cold_keys: list[str] = []
        for session_id in range(min(2, len(session_state))):
            cold_keys.extend(session_state.get(session_id, []))
        cold_keys = cold_keys[:12]

        if cfg["lease_age_wait_ms"] > 0:
            time.sleep(cfg["lease_age_wait_ms"] / 1000.0)
        phase("Phase 3", f"aged candidates for {cfg['lease_age_wait_ms']}ms")

        if args.profile == "phase_shift":
            shift_seq = select_prefix_sequence(profile.shift_pattern, cfg["prefix_count"])
            shift_sessions = cfg["phase_shift_rounds"] * cfg["burst_session_count"]
            for _ in range(shift_sessions):
                prefix_id = next(shift_seq)
                shift_counter[prefix_id] += 1
                session_state[next_session_id] = run_session(
                    store, next_session_id, prefix_id, cfg, stats, args
                )
                next_session_id += 1
                time.sleep(cfg["session_pause_ms"] / 1000.0)
            phase(
                "Phase 4",
                f"phase_shift_sessions={shift_sessions}, put_failures={stats.put_failures}",
            )
            old_hot = [pid for pid, _ in base_counter.most_common(2)]
            new_hot = [pid for pid, _ in shift_counter.most_common(2)]
            old_hot_hits, old_hot_total = probe_prefix_group(
                store, old_hot, cfg, stats, args
            )
            new_hot_hits, new_hot_total = probe_prefix_group(
                store, new_hot, cfg, stats, args
            )
        else:
            pressure_seq = select_prefix_sequence(profile.base_pattern, cfg["prefix_count"])
            pressure_sessions = cfg["pressure_rounds"] * cfg["burst_session_count"]
            for _ in range(cfg["pressure_rounds"]):
                for _ in range(cfg["burst_session_count"]):
                    prefix_id = next(pressure_seq)
                    base_counter[prefix_id] += 1
                    session_state[next_session_id] = run_session(
                        store, next_session_id, prefix_id, cfg, stats, args
                    )
                    next_session_id += 1
                    time.sleep(cfg["session_pause_ms"] / 1000.0)
                time.sleep(cfg["pressure_round_pause_ms"] / 1000.0)
            phase(
                "Phase 4",
                f"pressure_sessions={pressure_sessions}, put_failures={stats.put_failures}",
            )

            ranked = ranked_prefix_ids(base_counter, cfg["prefix_count"])
            hottest = ranked[:2]
            coldest = list(reversed(ranked[-2:]))
            top_hits, top_total = probe_prefix_group(store, hottest, cfg, stats, args)
            tail_hits, tail_total = probe_prefix_group(store, coldest, cfg, stats, args)

        cold_hits, cold_total = probe_cold_keys(store, cold_keys, stats)
        if args.profile == "phase_shift":
            phase(
                "Phase 5",
                f"old_hot={old_hot_hits}/{old_hot_total}, new_hot={new_hot_hits}/{new_hot_total}, cold_hits={cold_hits}/{cold_total}",
            )
        else:
            phase(
                "Phase 5",
                f"top_prefix={top_hits}/{top_total}, tail_prefix={tail_hits}/{tail_total}, cold_hits={cold_hits}/{cold_total}",
            )

        print_summary(
            args,
            cfg,
            profile,
            stats,
            base_counter,
            shift_counter,
            sum(len(v) for v in session_state.values()),
            top_hits,
            top_total,
            tail_hits,
            tail_total,
            old_hot_hits,
            old_hot_total,
            new_hot_hits,
            new_hot_total,
            cold_hits,
            cold_total,
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()


#   steady_skew

#   Phase 0: profile=steady_skew, segment=34MB, steady_estimate=28MB, peak_estimate=44MB
#   ...
#   Ops: puts=68, put_failures=0, gets=228, hits=218, misses=10
#   Hit rates: overall=95.6%, prefix=94.4%, session=100.0%, cold_miss=50.0%
#   Prefix survival: top=12/12, tail=9/12
#   Result: PASS

#   This is now a good non-saturating baseline. It produces some misses, no write failures, and a visible hot-vs-tail gap.

#   burst_pressure

#   Phase 0: profile=burst_pressure, segment=36MB, steady_estimate=45MB, peak_estimate=165MB
#   ...
#   Ops: puts=169, put_failures=29, gets=606, hits=406, misses=200
#   Hit rates: overall=67.0%, prefix=48.9%, session=85.9%, cold_miss=100.0%
#   Prefix survival: top=12/24, tail=0/24
#   Result: PASS (writes=expected-pressure)

#   This remains the harsh stress benchmark. It clearly separates hot and cold prefixes under overload.

#   phase_shift

#   Phase 0: profile=phase_shift, segment=32MB, steady_estimate=26MB, peak_estimate=38MB
#   ...
#   Ops: puts=56, put_failures=0, gets=172, hits=160, misses=12
#   Hit rates: overall=93.0%, prefix=92.9%, session=93.8%, cold_miss=12.5%
#   Phase shift: old_hot=6/12, new_hot=12/12
#   Result: PASS

#   This is the clean adaptation benchmark. It shows the cache shifting toward the new hot set without saturation.