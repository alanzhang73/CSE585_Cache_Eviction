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
class WorkloadProfile:
    prefix_count: int
    prefix_chunks: int
    session_count: int
    session_kv_chunks: int
    decode_steps: int
    burst_session_count: int
    pressure_rounds: int
    prefix_bytes: int
    session_bytes: int
    decode_growth_stride: int
    verify_rounds: int
    global_segment_size: int
    local_buffer_size: int
    session_pause_ms: int
    pressure_round_pause_ms: int
    lease_age_wait_ms: int
    hot_refresh_interval_ms: int
    cold_probe_limit: int
    allow_put_failures: bool
    keep_hot_prefixes: bool
    hot_prefix_count: int
    probe_all_prefixes: bool
    prefix_reuse_pattern: tuple[int, ...]


PROFILES = {
    "smoke": WorkloadProfile(
        prefix_count=2,
        prefix_chunks=2,
        session_count=2,
        session_kv_chunks=2,
        decode_steps=4,
        burst_session_count=1,
        pressure_rounds=1,
        prefix_bytes=256 * 1024,
        session_bytes=256 * 1024,
        decode_growth_stride=2,
        verify_rounds=2,
        global_segment_size=64 * 1024 * 1024,
        local_buffer_size=16 * 1024 * 1024,
        session_pause_ms=50,
        pressure_round_pause_ms=300,
        lease_age_wait_ms=0,
        hot_refresh_interval_ms=0,
        cold_probe_limit=0,
        allow_put_failures=False,
        keep_hot_prefixes=False,
        hot_prefix_count=0,
        probe_all_prefixes=False,
        prefix_reuse_pattern=(0, 1),
    ),
    "eviction": WorkloadProfile(
        prefix_count=2,
        prefix_chunks=2,
        session_count=4,
        session_kv_chunks=2,
        decode_steps=4,
        burst_session_count=4,
        pressure_rounds=2,
        prefix_bytes=512 * 1024,
        session_bytes=768 * 1024,
        decode_growth_stride=2,
        verify_rounds=3,
        global_segment_size=24 * 1024 * 1024,
        local_buffer_size=24 * 1024 * 1024,
        session_pause_ms=50,
        pressure_round_pause_ms=200,
        lease_age_wait_ms=6500,
        hot_refresh_interval_ms=1000,
        cold_probe_limit=8,
        allow_put_failures=False,
        keep_hot_prefixes=True,
        hot_prefix_count=2,
        probe_all_prefixes=False,
        prefix_reuse_pattern=(0, 0, 1, 1),
    ),
    "pressure": WorkloadProfile(
        prefix_count=3,
        prefix_chunks=3,
        session_count=4,
        session_kv_chunks=3,
        decode_steps=6,
        burst_session_count=6,
        pressure_rounds=3,
        prefix_bytes=512 * 1024,
        session_bytes=1024 * 1024,
        decode_growth_stride=2,
        verify_rounds=4,
        global_segment_size=32 * 1024 * 1024,
        local_buffer_size=32 * 1024 * 1024,
        session_pause_ms=40,
        pressure_round_pause_ms=120,
        lease_age_wait_ms=6500,
        hot_refresh_interval_ms=1000,
        cold_probe_limit=12,
        allow_put_failures=True,
        keep_hot_prefixes=True,
        hot_prefix_count=2,
        probe_all_prefixes=False,
        prefix_reuse_pattern=(0, 0, 0, 1, 1, 2),
    ),
    "policy_eviction": WorkloadProfile(
        prefix_count=5,
        prefix_chunks=2,
        session_count=5,
        session_kv_chunks=2,
        decode_steps=4,
        burst_session_count=4,
        pressure_rounds=2,
        prefix_bytes=512 * 1024,
        session_bytes=768 * 1024,
        decode_growth_stride=2,
        verify_rounds=3,
        global_segment_size=24 * 1024 * 1024,
        local_buffer_size=24 * 1024 * 1024,
        session_pause_ms=50,
        pressure_round_pause_ms=200,
        lease_age_wait_ms=6500,
        hot_refresh_interval_ms=0,
        cold_probe_limit=8,
        allow_put_failures=False,
        keep_hot_prefixes=False,
        hot_prefix_count=0,
        probe_all_prefixes=True,
        prefix_reuse_pattern=(0, 0, 0, 1, 1, 2, 3, 4),
    ),
    "policy_pressure": WorkloadProfile(
        prefix_count=6,
        prefix_chunks=3,
        session_count=5,
        session_kv_chunks=3,
        decode_steps=6,
        burst_session_count=6,
        pressure_rounds=3,
        prefix_bytes=512 * 1024,
        session_bytes=1024 * 1024,
        decode_growth_stride=2,
        verify_rounds=4,
        global_segment_size=32 * 1024 * 1024,
        local_buffer_size=32 * 1024 * 1024,
        session_pause_ms=40,
        pressure_round_pause_ms=120,
        lease_age_wait_ms=6500,
        hot_refresh_interval_ms=0,
        cold_probe_limit=12,
        allow_put_failures=True,
        keep_hot_prefixes=False,
        hot_prefix_count=0,
        probe_all_prefixes=True,
        prefix_reuse_pattern=(0, 0, 0, 0, 1, 1, 1, 2, 2, 3, 4, 5),
    ),
}


@dataclass
class WorkloadStats:
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
    def hit_rate(self) -> float:
        return self.hits / self.gets if self.gets else 0.0

    @property
    def prefix_hit_rate(self) -> float:
        return self.prefix_hits / self.prefix_gets if self.prefix_gets else 0.0

    @property
    def session_hit_rate(self) -> float:
        return self.session_hits / self.session_gets if self.session_gets else 0.0

    @property
    def cold_misses(self) -> int:
        return self.cold_gets - self.cold_hits

    @property
    def cold_miss_rate(self) -> float:
        return self.cold_misses / self.cold_gets if self.cold_gets else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Short Mooncake synthetic testcase with deterministic profiles, "
            "hot-prefix probes, and cold-key miss probes."
        )
    )
    parser.add_argument("--profile", choices=sorted(PROFILES), default="smoke")
    parser.add_argument("--metadata-url", default="http://localhost:8080/metadata")
    parser.add_argument("--master-address", default="localhost:50051")
    parser.add_argument("--node-address", default="localhost")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--run-id", default=f"run-{int(time.time())}")
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
    return f"llm/{args.run_id}/{suffix}"


def put_with_retries(
    store: MooncakeDistributedStore,
    key: str,
    value: bytes,
    stats: WorkloadStats,
    retries: int,
    retry_sleep_ms: int,
    failure_cooldown_ms: int,
) -> bool:
    for _ in range(retries):
        rc = store.put(key, value)
        if rc == 0:
            stats.puts += 1
            return True
        stats.put_failures += 1
        time.sleep(retry_sleep_ms / 1000.0)
    time.sleep(failure_cooldown_ms / 1000.0)
    return False


def get_hit(
    store: MooncakeDistributedStore,
    key: str,
    stats: WorkloadStats,
    category: str,
) -> bool:
    hit = store.get(key) != b""
    stats.record_get(hit, category)
    return hit


def phase(label: str, detail: str) -> None:
    print(f"{label}: {detail}")


def estimate_bytes(cfg: dict[str, int]) -> tuple[int, int]:
    prefix_footprint = cfg["prefix_count"] * cfg["prefix_chunks"] * cfg["prefix_bytes"]
    per_session_chunks = cfg["session_kv_chunks"] + (
        cfg["decode_steps"] // cfg["decode_growth_stride"]
    )
    session_footprint = cfg["session_count"] * per_session_chunks * cfg["session_bytes"]
    pressure_footprint = (
        cfg["burst_session_count"]
        * cfg["pressure_rounds"]
        * per_session_chunks
        * cfg["session_bytes"]
    )
    return (
        prefix_footprint + session_footprint,
        prefix_footprint + session_footprint + pressure_footprint,
    )


def simulate_prefix_prefill(
    store: MooncakeDistributedStore,
    cfg: dict[str, int],
    stats: WorkloadStats,
    args: argparse.Namespace,
) -> list[str]:
    prefix_keys: list[str] = []
    payload = b"P" * cfg["prefix_bytes"]

    for prefix_id in range(cfg["prefix_count"]):
        for chunk_idx in range(cfg["prefix_chunks"]):
            key = namespaced_key(args, f"prefix-{prefix_id}/chunk-{chunk_idx}")
            if put_with_retries(
                store,
                key,
                payload,
                stats,
                args.put_retries,
                args.put_retry_sleep_ms,
                args.put_failure_cooldown_ms,
            ):
                prefix_keys.append(key)
    return prefix_keys


def prefix_keys_for(args: argparse.Namespace, prefix_id: int, cfg: dict[str, int]) -> list[str]:
    return [
        namespaced_key(args, f"prefix-{prefix_id}/chunk-{chunk_idx}")
        for chunk_idx in range(cfg["prefix_chunks"])
    ]


def select_prefix_sequence(cfg: dict[str, int]) -> cycle:
    pattern = cfg["prefix_reuse_pattern"]
    if not pattern:
        pattern = tuple(range(cfg["prefix_count"]))
    normalized = tuple(prefix_id % cfg["prefix_count"] for prefix_id in pattern)
    return cycle(normalized)


def simulate_session(
    store: MooncakeDistributedStore,
    session_id: int,
    prefix_id: int,
    cfg: dict[str, int],
    stats: WorkloadStats,
    args: argparse.Namespace,
) -> list[str]:
    payload = b"S" * cfg["session_bytes"]
    session_keys: list[str] = []

    for chunk_idx in range(cfg["session_kv_chunks"]):
        key = namespaced_key(args, f"session-{session_id}/kv-{chunk_idx}")
        if put_with_retries(
            store,
            key,
            payload,
            stats,
            args.put_retries,
            args.put_retry_sleep_ms,
            args.put_failure_cooldown_ms,
        ):
            session_keys.append(key)

    prefix_keys = prefix_keys_for(args, prefix_id, cfg)

    for key in prefix_keys:
        get_hit(store, key, stats, "prefix")

    active_keys = list(session_keys)
    for step in range(cfg["decode_steps"]):
        if prefix_keys:
            get_hit(store, prefix_keys[step % len(prefix_keys)], stats, "prefix")
        if active_keys:
            get_hit(store, active_keys[-1], stats, "session")
        if len(active_keys) > 1:
            warm_key = active_keys[(step + session_id) % len(active_keys)]
            get_hit(store, warm_key, stats, "session")

        if (step + 1) % cfg["decode_growth_stride"] == 0:
            new_idx = len(active_keys)
            key = namespaced_key(args, f"session-{session_id}/kv-{new_idx}")
            if put_with_retries(
                store,
                key,
                payload,
                stats,
                args.put_retries,
                args.put_retry_sleep_ms,
                args.put_failure_cooldown_ms,
            ):
                active_keys.append(key)

    return active_keys


def keep_prefixes_hot(
    store: MooncakeDistributedStore,
    hot_prefix_ids: list[int],
    cfg: dict[str, int],
    stats: WorkloadStats,
    args: argparse.Namespace,
) -> None:
    if cfg["lease_age_wait_ms"] <= 0 or not cfg["keep_hot_prefixes"] or not hot_prefix_ids:
        return

    deadline = time.time() + cfg["lease_age_wait_ms"] / 1000.0
    refresh_sleep = max(cfg["hot_refresh_interval_ms"], 100) / 1000.0
    while time.time() < deadline:
        for prefix_id in hot_prefix_ids:
            for key in prefix_keys_for(args, prefix_id, cfg):
                get_hit(store, key, stats, "prefix")
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(refresh_sleep, remaining))


def verify_hot_prefixes(
    store: MooncakeDistributedStore,
    hot_prefix_ids: list[int],
    cfg: dict[str, int],
    stats: WorkloadStats,
    args: argparse.Namespace,
) -> tuple[int, int]:
    hits = 0
    total = 0
    for _ in range(cfg["verify_rounds"]):
        for prefix_id in hot_prefix_ids:
            for key in prefix_keys_for(args, prefix_id, cfg):
                total += 1
                if get_hit(store, key, stats, "prefix"):
                    hits += 1
    return hits, total


def verify_all_prefixes(
    store: MooncakeDistributedStore,
    cfg: dict[str, int],
    stats: WorkloadStats,
    args: argparse.Namespace,
) -> tuple[int, int]:
    hits = 0
    total = 0
    for _ in range(cfg["verify_rounds"]):
        for prefix_id in range(cfg["prefix_count"]):
            for key in prefix_keys_for(args, prefix_id, cfg):
                total += 1
                if get_hit(store, key, stats, "prefix"):
                    hits += 1
    return hits, total


def probe_cold_keys(
    store: MooncakeDistributedStore,
    cold_keys: list[str],
    stats: WorkloadStats,
) -> tuple[int, int]:
    hits = 0
    total = 0
    for key in cold_keys:
        total += 1
        if get_hit(store, key, stats, "cold"):
            hits += 1
    return hits, total


def print_summary(
    args: argparse.Namespace,
    cfg: dict[str, int],
    profile: WorkloadProfile,
    stats: WorkloadStats,
    prefix_use_counter: Counter[int],
    prefix_key_count: int,
    session_key_count: int,
    hot_hits: int,
    hot_total: int,
    all_prefix_hits: int,
    all_prefix_total: int,
    cold_hits: int,
    cold_total: int,
) -> None:
    hot_prefix_pass = (
        (not profile.keep_hot_prefixes)
        or (hot_hits == hot_total and hot_total > 0)
    )
    prefix_policy_signal = (
        (not profile.probe_all_prefixes)
        or (all_prefix_total > 0 and all_prefix_hits < all_prefix_total)
    )
    miss_pass = cold_total == 0 or cold_hits < cold_total
    write_pass = profile.allow_put_failures or stats.put_failures == 0
    overall_pass = write_pass and hot_prefix_pass and miss_pass and prefix_policy_signal

    print()
    print(f"Profile: {args.profile}")
    print(f"Run ID: {args.run_id}")
    print(f"Endpoints: metadata={args.metadata_url}, master={args.master_address}")
    print(
        "Reuse: "
        + ", ".join(
            f"prefix-{prefix_id}={count}"
            for prefix_id, count in sorted(prefix_use_counter.items())
        )
    )
    print(
        "Ops: "
        f"puts={stats.puts}, put_failures={stats.put_failures}, "
        f"gets={stats.gets}, hits={stats.hits}, misses={stats.misses}"
    )
    print(
        "Hit rates: "
        f"overall={stats.hit_rate:.1%}, "
        f"prefix={stats.prefix_hit_rate:.1%}, "
        f"session={stats.session_hit_rate:.1%}, "
        f"cold_miss={stats.cold_miss_rate:.1%}"
    )
    print(
        "Objects: "
        f"prefix_keys={prefix_key_count}, session_keys={session_key_count}, "
        f"hot_prefix_probe={hot_hits}/{hot_total}, "
        f"all_prefix_probe={all_prefix_hits}/{all_prefix_total}, "
        f"cold_probe_hits={cold_hits}/{cold_total}"
    )
    print(
        "Result: "
        f"{'PASS' if overall_pass else 'FAIL'} "
        f"(writes={'expected-pressure' if profile.allow_put_failures else ('ok' if write_pass else 'failed')}, "
        f"hot_prefixes={'ok' if hot_prefix_pass else 'degraded'}, "
        f"all_prefixes={'mixed' if profile.probe_all_prefixes else 'n/a'}, "
        f"cold_keys={'missed' if miss_pass else 'all_hit'})"
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
        "burst_session_count": profile_value(args, "burst_session_count"),
        "pressure_rounds": profile_value(args, "pressure_rounds"),
        "prefix_bytes": profile_value(args, "prefix_bytes"),
        "session_bytes": profile_value(args, "session_bytes"),
        "decode_growth_stride": profile_value(args, "decode_growth_stride"),
        "verify_rounds": profile_value(args, "verify_rounds"),
        "global_segment_size": profile_value(args, "global_segment_size"),
        "local_buffer_size": profile_value(args, "local_buffer_size"),
        "session_pause_ms": profile_value(args, "session_pause_ms"),
        "pressure_round_pause_ms": profile_value(args, "pressure_round_pause_ms"),
        "lease_age_wait_ms": profile_value(args, "lease_age_wait_ms"),
        "hot_refresh_interval_ms": profile_value(args, "hot_refresh_interval_ms"),
        "cold_probe_limit": profile_value(args, "cold_probe_limit"),
        "keep_hot_prefixes": profile_value(args, "keep_hot_prefixes"),
        "hot_prefix_count": profile_value(args, "hot_prefix_count"),
        "probe_all_prefixes": profile_value(args, "probe_all_prefixes"),
        "prefix_reuse_pattern": profile_value(args, "prefix_reuse_pattern"),
    }

    steady_estimate, peak_estimate = estimate_bytes(cfg)
    phase(
        "Phase 0",
        "profile="
        f"{args.profile}, segment={cfg['global_segment_size'] // (1024 * 1024)}MB, "
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

    stats = WorkloadStats()
    prefix_use_counter: Counter[int] = Counter()
    session_state: dict[int, list[str]] = {}

    try:
        prefix_keys = simulate_prefix_prefill(store, cfg, stats, args)
        phase(
            "Phase 1",
            f"prefix prefill wrote {len(prefix_keys)}/{cfg['prefix_count'] * cfg['prefix_chunks']} objects",
        )

        popular_prefixes = select_prefix_sequence(cfg)
        for session_id in range(cfg["session_count"]):
            prefix_id = next(popular_prefixes)
            prefix_use_counter[prefix_id] += 1
            session_state[session_id] = simulate_session(
                store, session_id, prefix_id, cfg, stats, args
            )
            time.sleep(cfg["session_pause_ms"] / 1000.0)
        phase(
            "Phase 2",
            f"base sessions={cfg['session_count']}, session_keys={sum(len(keys) for keys in session_state.values())}, prefix_hit_rate={stats.prefix_hit_rate:.1%}",
        )

        hot_prefix_ids = [
            prefix_id
            for prefix_id, _ in prefix_use_counter.most_common(
                min(cfg["hot_prefix_count"], cfg["prefix_count"])
            )
        ]
        cold_keys = []
        for session_id in range(min(cfg["session_count"], 2)):
            cold_keys.extend(session_state.get(session_id, []))
        cold_keys = cold_keys[: cfg["cold_probe_limit"]]

        if cfg["lease_age_wait_ms"] > 0:
            keep_prefixes_hot(store, hot_prefix_ids, cfg, stats, args)
            phase(
                "Phase 3",
                f"aged cold keys for {cfg['lease_age_wait_ms']}ms while refreshing hot prefixes",
            )
        else:
            phase("Phase 3", "no lease aging for this profile")

        next_session_id = cfg["session_count"]
        pressure_prefixes = select_prefix_sequence(cfg)
        for _ in range(cfg["pressure_rounds"]):
            for _ in range(cfg["burst_session_count"]):
                session_id = next_session_id
                next_session_id += 1
                prefix_id = next(pressure_prefixes)
                prefix_use_counter[prefix_id] += 1
                session_state[session_id] = simulate_session(
                    store, session_id, prefix_id, cfg, stats, args
                )
                time.sleep(cfg["session_pause_ms"] / 1000.0)
            time.sleep(cfg["pressure_round_pause_ms"] / 1000.0)
        phase(
            "Phase 4",
            f"pressure_sessions={cfg['burst_session_count'] * cfg['pressure_rounds']}, put_failures={stats.put_failures}",
        )

        hot_hits, hot_total = verify_hot_prefixes(
            store, hot_prefix_ids, cfg, stats, args
        )
        all_prefix_hits, all_prefix_total = verify_all_prefixes(
            store, cfg, stats, args
        )
        cold_hits, cold_total = probe_cold_keys(store, cold_keys, stats)
        phase(
            "Phase 5",
            f"hot-prefix probe {hot_hits}/{hot_total}, "
            f"all-prefix probe {all_prefix_hits}/{all_prefix_total}, "
            f"cold-key hits {cold_hits}/{cold_total}",
        )

        print_summary(
            args,
            cfg,
            profile,
            stats,
            prefix_use_counter,
            len(prefix_keys),
            sum(len(keys) for keys in session_state.values()),
            hot_hits,
            hot_total,
            all_prefix_hits,
            all_prefix_total,
            cold_hits,
            cold_total,
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()


#   Smoke

#   Phase 0: profile=smoke, segment=64MB, steady_estimate=3MB, peak_estimate=4MB, lease_age_wait=0ms
#   ...
#   Ops: puts=16, put_failures=0, gets=50, hits=50, misses=0
#   Hit rates: overall=100.0%, prefix=100.0%, session=100.0%
#   Result: PASS

#   This is the clean sanity check. Everything fits easily, there is no eviction, and it mostly verifies client/server wiring plus basic put/get behavior.

#   Eviction

#   Phase 0: profile=eviction, segment=24MB, steady_estimate=14MB, peak_estimate=38MB, lease_age_wait=6500ms
#   ...
#   Ops: puts=52, put_failures=0, gets=228, hits=221, misses=7
#   Hit rates: overall=96.9%, prefix=100.0%, session=100.0%, cold_miss=87.5%
#   Objects: hot_prefix_probe=12/12, cold_probe_hits=1/8
#   Result: PASS

#   This is the controlled eviction case. It generated some misses without write failures. The hot prefixes were preserved, and most cold probe keys were
#   gone. This is a good correctness-style eviction test, but it still protects prefixes too aggressively for policy comparison.

#   Pressure

#   Phase 0: profile=pressure, segment=32MB, steady_estimate=28MB, peak_estimate=136MB, lease_age_wait=6500ms
#   ...
#   Ops: puts=135, put_failures=31, gets=572, hits=473, misses=99
#   Hit rates: overall=82.7%, prefix=87.0%, session=81.5%, cold_miss=100.0%
#   Objects: hot_prefix_probe=24/24, all_prefix_probe=24/36, cold_probe_hits=0/12
#   Result: PASS (writes=expected-pressure)

#   This is the overload test. It intentionally saturates the segment, so the put_failures are expected. It creates real pressure and real misses while
#   still keeping the designated hot prefixes alive.

#   Policy Eviction

#   Phase 0: profile=policy_eviction, segment=24MB, steady_estimate=20MB, peak_estimate=44MB, lease_age_wait=6500ms
#   ...
#   Ops: puts=59, put_failures=14, gets=218, hits=166, misses=52
#   Hit rates: overall=76.1%, prefix=66.7%, session=92.2%, cold_miss=100.0%
#   Objects: all_prefix_probe=12/30, cold_probe_hits=0/8
#   Result: FAIL (writes=failed, all_prefixes=mixed)

#   This is the first profile that looks useful for comparing eviction policies because prefix survival is no longer forced to 100%. Only 12/30 probed
#   prefixes survived. The issue is that it currently overshoots and starts failing writes, so it is a bit too aggressive for a clean “policy baseline”
#   run.

#   Policy Pressure

#   Phase 0: profile=policy_pressure, segment=32MB, steady_estimate=39MB, peak_estimate=147MB, lease_age_wait=6500ms
#   ...
#   Ops: puts=150, put_failures=35, gets=567, hits=342, misses=225
#   Hit rates: overall=60.3%, prefix=40.1%, session=83.3%, cold_miss=100.0%
#   Objects: all_prefix_probe=4/72, cold_probe_hits=0/12
#   Result: PASS (writes=expected-pressure, all_prefixes=mixed)

#   This is the harshest policy-comparison case. It heavily pressures the cache and exposes strong differentiation in prefix survival. Only 4/72 probed
#   prefixes survived, so this profile should be sensitive to different eviction policies.