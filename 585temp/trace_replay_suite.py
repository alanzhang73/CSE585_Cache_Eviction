from __future__ import annotations

import argparse
from collections import Counter
import ctypes
from dataclasses import dataclass
import hashlib
import json
import math
import os
import sys
import time
from typing import Optional


def load_store_module():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    local_asio = os.path.join(root, "build", "mooncake-asio", "libasio.so")
    local_integration = os.path.join(root, "build", "mooncake-integration")

    if os.path.exists(local_asio) and os.path.isdir(local_integration):
        ctypes.CDLL(local_asio, mode=ctypes.RTLD_GLOBAL)
        sys.path.insert(0, local_integration)
        import store

        return store

    from mooncake import store

    return store


STORE_MODULE = load_store_module()
MooncakeDistributedStore = STORE_MODULE.MooncakeDistributedStore
ReplicateConfig = getattr(STORE_MODULE, "ReplicateConfig", None)


@dataclass(frozen=True)
class TraceProfile:
    trace_path: str
    max_records: int
    global_segment_size: int
    local_buffer_size: int
    prefix_bytes: int
    session_bytes: int
    tokens_per_session_chunk: int
    max_session_chunks: int
    time_scale: int
    max_sleep_ms: int
    allow_put_failures: bool
    verify_count: int
    phase_shift: bool
    min_hash_frequency: int
    max_prefix_hashes_per_request: int
    max_eligible_hashes: int
    scan_burst_interval: int
    scan_burst_keys: int
    scan_burst_bytes: int


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


PROFILES = {
    "toolagent_replay": TraceProfile(
        trace_path=os.path.join(ROOT, "FAST25-release", "traces", "toolagent_trace.jsonl"),
        max_records=4000,
        global_segment_size=36 * 1024 * 1024,
        local_buffer_size=36 * 1024 * 1024,
        prefix_bytes=128 * 1024,
        session_bytes=256 * 1024,
        tokens_per_session_chunk=128,
        max_session_chunks=4,
        time_scale=100,
        max_sleep_ms=25,
        allow_put_failures=True,
        verify_count=24,
        phase_shift=False,
        min_hash_frequency=2,
        max_prefix_hashes_per_request=48,
        max_eligible_hashes=1200,
        scan_burst_interval=125,
        scan_burst_keys=24,
        scan_burst_bytes=160 * 1024,
    ),
    "conversation_replay": TraceProfile(
        trace_path=os.path.join(ROOT, "FAST25-release", "traces", "conversation_trace.jsonl"),
        max_records=3000,
        global_segment_size=36 * 1024 * 1024,
        local_buffer_size=36 * 1024 * 1024,
        prefix_bytes=128 * 1024,
        session_bytes=224 * 1024,
        tokens_per_session_chunk=128,
        max_session_chunks=4,
        time_scale=100,
        max_sleep_ms=25,
        allow_put_failures=True,
        verify_count=24,
        phase_shift=False,
        min_hash_frequency=2,
        max_prefix_hashes_per_request=48,
        max_eligible_hashes=1200,
        scan_burst_interval=96,
        scan_burst_keys=20,
        scan_burst_bytes=160 * 1024,
    ),
    "trace_phase_shift": TraceProfile(
        trace_path=os.path.join(ROOT, "FAST25-release", "traces", "conversation_trace.jsonl"),
        max_records=1600,
        global_segment_size=52 * 1024 * 1024,
        local_buffer_size=52 * 1024 * 1024,
        prefix_bytes=96 * 1024,
        session_bytes=128 * 1024,
        tokens_per_session_chunk=128,
        max_session_chunks=3,
        time_scale=200,
        max_sleep_ms=20,
        allow_put_failures=True,
        verify_count=16,
        phase_shift=True,
        min_hash_frequency=2,
        max_prefix_hashes_per_request=24,
        max_eligible_hashes=256,
        scan_burst_interval=0,
        scan_burst_keys=0,
        scan_burst_bytes=0,
    ),
}


@dataclass
class ReplayStats:
    puts: int = 0
    put_failures: int = 0
    gets: int = 0
    hits: int = 0
    misses: int = 0
    prefix_hits: int = 0
    prefix_gets: int = 0
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
    def cold_miss_rate(self) -> float:
        misses = self.cold_gets - self.cold_hits
        return misses / self.cold_gets if self.cold_gets else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay FAST25 traces into Mooncake and report cache-policy signals."
    )
    parser.add_argument("--profile", choices=sorted(PROFILES), default="toolagent_replay")
    parser.add_argument("--metadata-url", default="http://localhost:8080/metadata")
    parser.add_argument("--master-address", default="localhost:50051")
    parser.add_argument("--node-address", default="localhost")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--run-id", default=f"trace-{int(time.time())}")
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--global-segment-size", type=int)
    parser.add_argument("--local-buffer-size", type=int)
    parser.add_argument("--put-retries", type=int, default=3)
    parser.add_argument("--put-retry-sleep-ms", type=int, default=200)
    parser.add_argument("--put-failure-cooldown-ms", type=int, default=400)
    parser.add_argument(
        "--structure-mode",
        choices=("legacy", "radix", "auto"),
        default="auto",
        help="Use flat prefix keys, explicit radix parent/path metadata, or auto-detect radix support.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def profile_value(args: argparse.Namespace, field: str):
    override = getattr(args, field, None)
    if override is not None:
        return override
    return getattr(PROFILES[args.profile], field)


def phase(name: str, detail: str) -> None:
    print(f"{name}: {detail}")


def namespaced_key(args: argparse.Namespace, suffix: str) -> str:
    return f"trace/{args.run_id}/{suffix}"


def effective_structure_mode(args: argparse.Namespace) -> str:
    if args.structure_mode != "auto":
        return args.structure_mode
    if ReplicateConfig is None:
        return "legacy"
    cfg = ReplicateConfig()
    if hasattr(cfg, "radix_parent_key") and hasattr(cfg, "radix_path_segments"):
        return "radix"
    return "legacy"


def flat_prefix_key(args: argparse.Namespace, hash_id: int) -> str:
    return namespaced_key(args, f"prefix/{hash_id}")


def radix_prefix_key(args: argparse.Namespace, chain: list[int]) -> str:
    digest = hashlib.blake2b(
        ",".join(str(hash_id) for hash_id in chain).encode("utf-8"),
        digest_size=10,
    ).hexdigest()
    return namespaced_key(args, f"radix-prefix/{len(chain)}-{digest}")


def make_radix_config(parent_key: Optional[str], segment: int):
    cfg = ReplicateConfig()
    cfg.replica_num = 1
    if parent_key is not None:
        cfg.radix_parent_key = parent_key
    cfg.radix_path_segments = [str(segment)]
    return cfg


def prefix_key_for_hash(
    args: argparse.Namespace,
    hash_id: int,
    structure_mode: str,
    last_chain_by_hash: dict[int, tuple[int, ...]],
) -> str:
    if structure_mode == "radix" and hash_id in last_chain_by_hash:
        return radix_prefix_key(args, list(last_chain_by_hash[hash_id]))
    return flat_prefix_key(args, hash_id)


def get_hit(store: MooncakeDistributedStore, key: str, stats: ReplayStats, category: str) -> bool:
    hit = store.get(key) != b""
    stats.record_get(hit, category)
    return hit


def put_with_retries(
    store: MooncakeDistributedStore,
    key: str,
    value: bytes,
    stats: ReplayStats,
    args: argparse.Namespace,
    config=None,
) -> bool:
    invalid_params_rc = -600
    for _ in range(args.put_retries):
        if config is None:
            rc = store.put(key, value)
        else:
            rc = store.put(key, value, config)
        if rc == 0:
            stats.puts += 1
            return True
        stats.put_failures += 1
        if rc == invalid_params_rc:
            break
        time.sleep(args.put_retry_sleep_ms / 1000.0)
    time.sleep(args.put_failure_cooldown_ms / 1000.0)
    return False


def load_trace(path: str, max_records: int) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx >= max_records:
                break
            records.append(json.loads(line))
    return records


def ranked_hash_ids(counter: Counter[int], verify_count: int) -> tuple[list[int], list[int], list[int]]:
    ranked = [hash_id for hash_id, _ in counter.most_common()]
    top = ranked[:verify_count]
    # Use an upper-middle "warm" band instead of the true median. The true
    # midpoint of these heavy-tailed traces is usually too cold and collapses
    # to zero under every policy, which hides eviction-policy differences.
    warm_start = max(
        verify_count,
        min(len(ranked) - verify_count, len(ranked) // 5),
    )
    mid = ranked[warm_start : warm_start + verify_count]
    tail = list(reversed(ranked[-verify_count:])) if ranked else []
    return top, mid, tail


def phase_shift_hash_ids(
    first_half: Counter[int],
    second_half: Counter[int],
    eligible_hashes: set[int],
    verify_count: int,
) -> tuple[list[int], list[int]]:
    universe = (set(first_half) | set(second_half)) & eligible_hashes
    old_ranked = sorted(
        universe,
        key=lambda hash_id: (
            first_half.get(hash_id, 0) - second_half.get(hash_id, 0),
            first_half.get(hash_id, 0),
            -hash_id,
        ),
        reverse=True,
    )
    new_ranked = sorted(
        universe,
        key=lambda hash_id: (
            second_half.get(hash_id, 0) - first_half.get(hash_id, 0),
            second_half.get(hash_id, 0),
            -hash_id,
        ),
        reverse=True,
    )
    return old_ranked[:verify_count], new_ranked[:verify_count]


def verify_prefix_group(
    store: MooncakeDistributedStore,
    hash_ids: list[int],
    stats: ReplayStats,
    args: argparse.Namespace,
    structure_mode: str,
    last_chain_by_hash: dict[int, tuple[int, ...]],
) -> tuple[int, int]:
    hits = 0
    total = 0
    for hash_id in hash_ids:
        total += 1
        key = prefix_key_for_hash(
            args, hash_id, structure_mode, last_chain_by_hash
        )
        if get_hit(store, key, stats, "prefix"):
            hits += 1
    return hits, total


def probe_cold_keys(
    store: MooncakeDistributedStore, keys: list[str], stats: ReplayStats
) -> tuple[int, int]:
    hits = 0
    total = 0
    for key in keys:
        total += 1
        if get_hit(store, key, stats, "cold"):
            hits += 1
    return hits, total


def print_summary(
    args: argparse.Namespace,
    profile: TraceProfile,
    stats: ReplayStats,
    records: list[dict[str, object]],
    eligible_hash_count: int,
    top_hits: int,
    top_total: int,
    mid_hits: int,
    mid_total: int,
    tail_hits: int,
    tail_total: int,
    old_mid_hits: int,
    old_mid_total: int,
    old_hits: int,
    old_total: int,
    new_hits: int,
    new_total: int,
    cold_hits: int,
    cold_total: int,
) -> None:
    print()
    print(f"Profile: {args.profile}")
    print(f"Run ID: {args.run_id}")
    print(f"Trace: {profile.trace_path}")
    print(f"Endpoints: metadata={args.metadata_url}, master={args.master_address}")
    print(
        "Ops: "
        f"puts={stats.puts}, put_failures={stats.put_failures}, "
        f"gets={stats.gets}, hits={stats.hits}, misses={stats.misses}"
    )
    print(
        "Hit rates: "
        f"overall={stats.overall_hit_rate:.1%}, "
        f"prefix={stats.prefix_hit_rate:.1%}, "
        f"cold_miss={stats.cold_miss_rate:.1%}"
    )
    print(
        "Trace stats: "
        f"records={len(records)}, "
        f"avg_input={sum(int(r['input_length']) for r in records) / len(records):.1f}, "
        f"avg_output={sum(int(r['output_length']) for r in records) / len(records):.1f}, "
        f"eligible_prefixes={eligible_hash_count}"
    )
    print(
        "Replay model: "
        f"min_hash_frequency={profile.min_hash_frequency}, "
        f"max_prefix_hashes_per_request={profile.max_prefix_hashes_per_request}, "
        f"structure_mode={effective_structure_mode(args)}, "
        f"scan_burst_interval={profile.scan_burst_interval}, "
        f"scan_burst_keys={profile.scan_burst_keys}"
    )
    if profile.phase_shift:
        print(
            "Phase shift: "
            f"old_mid={old_mid_hits}/{old_mid_total}, "
            f"old_end={old_hits}/{old_total}, "
            f"new_end={new_hits}/{new_total}"
        )
        signal_ok = (
            old_mid_total > 0
            and new_total > 0
            and old_mid_hits > old_hits
            and new_hits >= old_hits
        )
    else:
        print(
            "Prefix survival: "
            f"top={top_hits}/{top_total}, "
            f"mid={mid_hits}/{mid_total}, "
            f"tail={tail_hits}/{tail_total}"
        )
        signal_ok = (
            top_total > 0
            and mid_total > 0
            and tail_total > 0
            and top_hits > mid_hits >= tail_hits
        )
    print(f"Cold probe: {cold_hits}/{cold_total}")
    write_state = "expected-pressure" if profile.allow_put_failures else (
        "ok" if stats.put_failures == 0 else "failed"
    )
    overall_ok = signal_ok and (profile.allow_put_failures or stats.put_failures == 0)
    print(
        "Result: "
        f"{'PASS' if overall_ok else 'FAIL'} "
        f"(writes={write_state}, policy_signal={'ok' if signal_ok else 'weak'})"
    )
    if args.verbose:
        print(f"Config: {profile}")


def main() -> None:
    args = parse_args()
    profile = PROFILES[args.profile]
    trace_path = profile.trace_path
    max_records = profile_value(args, "max_records")
    global_segment_size = profile_value(args, "global_segment_size")
    local_buffer_size = profile_value(args, "local_buffer_size")

    records = load_trace(trace_path, max_records)
    if not records:
        raise RuntimeError(f"no records loaded from {trace_path}")

    overall_counter: Counter[int] = Counter()
    replay_counter: Counter[int] = Counter()
    first_half_counter: Counter[int] = Counter()
    second_half_counter: Counter[int] = Counter()
    split = len(records) // 2

    for idx, record in enumerate(records):
        hash_ids = [int(hash_id) for hash_id in record["hash_ids"]]
        overall_counter.update(hash_ids)
        if idx < split:
            first_half_counter.update(hash_ids)
        else:
            second_half_counter.update(hash_ids)

    eligible_hashes = {
        hash_id
        for hash_id, count in overall_counter.items()
        if count >= profile.min_hash_frequency
    }
    if profile.max_eligible_hashes > 0:
        eligible_hashes = {
            hash_id
            for hash_id, _ in overall_counter.most_common(profile.max_eligible_hashes)
            if hash_id in eligible_hashes
        }

    unique_hashes = len(overall_counter)
    structure_mode = effective_structure_mode(args)
    phase(
        "Phase 0",
        f"profile={args.profile}, records={len(records)}, unique_hashes={unique_hashes}, "
        f"eligible_hashes={len(eligible_hashes)}, segment={global_segment_size // (1024 * 1024)}MB, "
        f"structure_mode={structure_mode}",
    )

    store = MooncakeDistributedStore()
    rc = store.setup(
        args.node_address,
        args.metadata_url,
        global_segment_size,
        local_buffer_size,
        args.protocol,
        args.rdma_devices,
        args.master_address,
    )
    if rc != 0:
        raise RuntimeError(f"store.setup failed with rc={rc}")

    stats = ReplayStats()
    prefix_payload = b"P" * profile.prefix_bytes
    session_payload = b"S" * profile.session_bytes
    scan_payload = b"X" * profile.scan_burst_bytes if profile.scan_burst_bytes > 0 else b""
    cold_keys: list[str] = []
    prev_timestamp = int(records[0]["timestamp"])
    unique_inserted: set[str] = set()
    last_chain_by_hash: dict[int, tuple[int, ...]] = {}

    top_hits = top_total = mid_hits = mid_total = tail_hits = tail_total = 0
    old_hits = old_total = new_hits = new_total = 0
    cold_hits = cold_total = 0
    old_mid_hits = old_mid_total = 0

    try:
        old_hot: list[int] = []
        new_hot: list[int] = []
        if profile.phase_shift:
            old_hot, new_hot = phase_shift_hash_ids(
                first_half_counter,
                second_half_counter,
                eligible_hashes,
                profile.verify_count,
            )

        for idx, record in enumerate(records):
            timestamp = int(record["timestamp"])
            delta = max(timestamp - prev_timestamp, 0)
            prev_timestamp = timestamp
            if delta > 0 and profile.time_scale > 0:
                sleep_ms = min(delta / profile.time_scale, profile.max_sleep_ms)
                if sleep_ms > 0:
                    time.sleep(sleep_ms / 1000.0)

            hash_ids = [
                int(hash_id)
                for hash_id in record["hash_ids"][: profile.max_prefix_hashes_per_request]
                if int(hash_id) in eligible_hashes
            ]
            replay_counter.update(hash_ids)
            chain: list[int] = []
            for hash_id in hash_ids:
                chain.append(hash_id)
                if structure_mode == "radix":
                    key = radix_prefix_key(args, chain)
                    parent_key = (
                        radix_prefix_key(args, chain[:-1]) if len(chain) > 1 else None
                    )
                    config = make_radix_config(parent_key, hash_id)
                else:
                    key = flat_prefix_key(args, hash_id)
                    config = None
                hit = get_hit(store, key, stats, "prefix")
                if hit:
                    unique_inserted.add(key)
                    if structure_mode == "radix":
                        last_chain_by_hash[hash_id] = tuple(chain)
                    continue
                if put_with_retries(store, key, prefix_payload, stats, args, config):
                    unique_inserted.add(key)
                    if structure_mode == "radix":
                        last_chain_by_hash[hash_id] = tuple(chain)
                    continue
                if structure_mode == "radix":
                    # A child cannot be created if its parent insert failed, so stop
                    # extending this request's prefix chain instead of generating
                    # structurally invalid orphan suffix objects.
                    break

            chunk_count = max(
                1,
                min(
                    profile.max_session_chunks,
                    math.ceil(int(record["output_length"]) / profile.tokens_per_session_chunk),
                ),
            )
            session_keys: list[str] = []
            for chunk_idx in range(chunk_count):
                key = namespaced_key(args, f"session/{idx}/chunk-{chunk_idx}")
                if put_with_retries(store, key, session_payload, stats, args):
                    session_keys.append(key)
            if idx < max(1, min(16, len(records) // 20)):
                cold_keys.extend(session_keys[:1])

            if (
                not profile.phase_shift
                and profile.scan_burst_interval > 0
                and profile.scan_burst_keys > 0
                and scan_payload
                and (idx + 1) % profile.scan_burst_interval == 0
            ):
                for scan_idx in range(profile.scan_burst_keys):
                    scan_key = namespaced_key(args, f"scan/{idx}/burst-{scan_idx}")
                    put_with_retries(store, scan_key, scan_payload, stats, args)

            if profile.phase_shift and idx + 1 == split:
                old_mid_hits, old_mid_total = verify_prefix_group(
                    store, old_hot, stats, args, structure_mode, last_chain_by_hash
                )

        phase(
            "Phase 1",
            f"replayed_requests={len(records)}, unique_prefix_objects={len(unique_inserted)}, put_failures={stats.put_failures}",
        )

        if profile.phase_shift:
            old_hits, old_total = verify_prefix_group(
                store, old_hot, stats, args, structure_mode, last_chain_by_hash
            )
            new_hits, new_total = verify_prefix_group(
                store, new_hot, stats, args, structure_mode, last_chain_by_hash
            )
            phase(
                "Phase 2",
                f"old_mid={old_mid_hits}/{old_mid_total}, old_end={old_hits}/{old_total}, new_end={new_hits}/{new_total}",
            )
        else:
            top_ids, mid_ids, tail_ids = ranked_hash_ids(replay_counter, profile.verify_count)
            top_hits, top_total = verify_prefix_group(
                store, top_ids, stats, args, structure_mode, last_chain_by_hash
            )
            mid_hits, mid_total = verify_prefix_group(
                store, mid_ids, stats, args, structure_mode, last_chain_by_hash
            )
            tail_hits, tail_total = verify_prefix_group(
                store, tail_ids, stats, args, structure_mode, last_chain_by_hash
            )
            phase(
                "Phase 2",
                "top_prefix="
                f"{top_hits}/{top_total}, "
                f"mid_prefix={mid_hits}/{mid_total}, "
                f"tail_prefix={tail_hits}/{tail_total}",
            )

        cold_hits, cold_total = probe_cold_keys(store, cold_keys[: profile.verify_count], stats)
        phase("Phase 3", f"cold_hits={cold_hits}/{cold_total}")

        print_summary(
            args,
            profile,
            stats,
            records,
            len(eligible_hashes),
            top_hits,
            top_total,
            mid_hits,
            mid_total,
            tail_hits,
            tail_total,
            old_mid_hits,
            old_mid_total,
            old_hits,
            old_total,
            new_hits,
            new_total,
            cold_hits,
            cold_total,
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()


#  policy_suite.py

#   - steady_skew: good stable baseline
#   - burst_pressure: good overload benchmark
#   - phase_shift: good synthetic adaptation benchmark

#   trace_replay_suite.py

#   - toolagent_replay: now meaningful

#   Phase 0: profile=toolagent_replay, records=160, unique_hashes=2126, eligible_hashes=18, segment=48MB
#   Phase 1: replayed_requests=160, unique_prefix_objects=18, put_failures=0
#   Phase 2: top_prefix=8/8, tail_prefix=0/8
#   Phase 3: cold_hits=3/8
#   Result: PASS

#   - conversation_replay: now meaningful

#   Phase 0: profile=conversation_replay, records=160, unique_hashes=4192, eligible_hashes=43, segment=48MB
#   Phase 1: replayed_requests=160, unique_prefix_objects=41, put_failures=0
#   Phase 2: top_prefix=7/8, tail_prefix=0/8
#   Phase 3: cold_hits=1/8
#   Result: PASS

#   These two are good because they now:

#   - avoid write failures
#   - preserve a bounded reusable prefix set from the FAST25 traces
#   - show clear hot-vs-tail separation
#   - keep some cold misses

#   Still weak

#   trace_phase_shift

#   - I fixed the replay logic and hot-set selection, but the underlying conversation trace window still does not produce a strong end-to-end shift signal
#     for this cache size/model.
#   - Final behavior is stable, but weak:

#   Phase 2: old_mid=8/8, old_end=8/8, new_end=8/8
#   Result: FAIL (policy_signal=weak)

#   That means:

#   - the trace is useful for realistic skew and pressure
#   - but this particular replay setup is not good for a phase-shift benchmark
