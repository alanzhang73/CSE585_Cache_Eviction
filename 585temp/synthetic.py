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

    # Prefer the local build so the test client matches the running master.
    if os.path.exists(local_asio) and os.path.isdir(local_integration):
        ctypes.CDLL(local_asio, mode=ctypes.RTLD_GLOBAL)
        sys.path.insert(0, local_integration)
        import store

        return store.MooncakeDistributedStore

    from mooncake.store import MooncakeDistributedStore

    return MooncakeDistributedStore


MooncakeDistributedStore = load_store_class()


@dataclass
class WorkloadStats:
    puts: int = 0
    put_failures: int = 0
    gets: int = 0
    hits: int = 0
    misses: int = 0

    def record_get(self, hit: bool) -> None:
        self.gets += 1
        if hit:
            self.hits += 1
        else:
            self.misses += 1

    @property
    def hit_rate(self) -> float:
        return self.hits / self.gets if self.gets else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a synthetic LLM-style Mooncake workload with shared "
            "prefix reuse, per-session prefill writes, and repeated decode "
            "reads, without running a real model."
        )
    )
    parser.add_argument("--metadata-url", default="http://localhost:8080/metadata")
    parser.add_argument("--master-address", default="localhost:50051")
    parser.add_argument("--node-address", default="localhost")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--global-segment-size", type=int, default=24 * 1024 * 1024)
    parser.add_argument("--local-buffer-size", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--prefix-count", type=int, default=3)
    parser.add_argument("--prefix-chunks", type=int, default=4)
    parser.add_argument("--session-count", type=int, default=10)
    parser.add_argument("--session-kv-chunks", type=int, default=3)
    parser.add_argument("--decode-steps", type=int, default=16)
    parser.add_argument("--burst-session-count", type=int, default=28)
    parser.add_argument("--pressure-rounds", type=int, default=6)
    parser.add_argument("--prefix-bytes", type=int, default=512 * 1024)
    parser.add_argument("--session-bytes", type=int, default=768 * 1024)
    parser.add_argument("--decode-growth-stride", type=int, default=2)
    parser.add_argument("--verify-rounds", type=int, default=5)
    parser.add_argument("--put-retries", type=int, default=20)
    parser.add_argument("--put-retry-sleep-ms", type=int, default=250)
    parser.add_argument("--put-failure-cooldown-ms", type=int, default=1500)
    parser.add_argument("--session-pause-ms", type=int, default=150)
    parser.add_argument("--pressure-round-pause-ms", type=int, default=2500)
    return parser.parse_args()


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


def get_hit(store: MooncakeDistributedStore, key: str, stats: WorkloadStats) -> bool:
    hit = store.get(key) != b""
    stats.record_get(hit)
    return hit


def simulate_prefix_prefill(
    store: MooncakeDistributedStore,
    args: argparse.Namespace,
    stats: WorkloadStats,
) -> list[str]:
    prefix_keys: list[str] = []
    prefix_payload = b"P" * args.prefix_bytes

    for prefix_id in range(args.prefix_count):
        for chunk_idx in range(args.prefix_chunks):
            key = f"llm/prefix-{prefix_id}/chunk-{chunk_idx}"
            if put_with_retries(
                store,
                key,
                prefix_payload,
                stats,
                args.put_retries,
                args.put_retry_sleep_ms,
                args.put_failure_cooldown_ms,
            ):
                prefix_keys.append(key)

    return prefix_keys


def simulate_session(
    store: MooncakeDistributedStore,
    session_id: int,
    prefix_id: int,
    args: argparse.Namespace,
    stats: WorkloadStats,
) -> list[str]:
    session_payload = b"S" * args.session_bytes
    session_keys: list[str] = []

    for chunk_idx in range(args.session_kv_chunks):
        key = f"llm/session-{session_id}/kv-{chunk_idx}"
        if put_with_retries(
            store,
            key,
            session_payload,
            stats,
            args.put_retries,
            args.put_retry_sleep_ms,
            args.put_failure_cooldown_ms,
        ):
            session_keys.append(key)

    prefix_keys = [
        f"llm/prefix-{prefix_id}/chunk-{chunk_idx}"
        for chunk_idx in range(args.prefix_chunks)
    ]

    for key in prefix_keys:
        get_hit(store, key, stats)

    active_keys = list(session_keys)
    for step in range(args.decode_steps):
        prefix_key = prefix_keys[step % len(prefix_keys)]
        get_hit(store, prefix_key, stats)

        hot_session_key = active_keys[-1]
        get_hit(store, hot_session_key, stats)

        if len(active_keys) > 1:
            warm_session_key = active_keys[(step + session_id) % len(active_keys)]
            get_hit(store, warm_session_key, stats)

        if (step + 1) % args.decode_growth_stride == 0:
            new_idx = len(active_keys)
            key = f"llm/session-{session_id}/kv-{new_idx}"
            if put_with_retries(
                store,
                key,
                session_payload,
                stats,
                args.put_retries,
                args.put_retry_sleep_ms,
                args.put_failure_cooldown_ms,
            ):
                active_keys.append(key)

    return active_keys


def main() -> None:
    args = parse_args()
    store = MooncakeDistributedStore()

    rc = store.setup(
        args.node_address,
        args.metadata_url,
        args.global_segment_size,
        args.local_buffer_size,
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
        prefix_keys = simulate_prefix_prefill(store, args, stats)

        popular_prefixes = cycle(
            [0, 1, 0, 2, 0, 1][: max(1, min(args.prefix_count, 6))]
        )
        for session_id in range(args.session_count):
            prefix_id = next(popular_prefixes) % args.prefix_count
            prefix_use_counter[prefix_id] += 1
            session_state[session_id] = simulate_session(
                store, session_id, prefix_id, args, stats
            )
            time.sleep(args.session_pause_ms / 1000.0)

        next_session_id = args.session_count
        for _ in range(args.pressure_rounds):
            for _ in range(args.burst_session_count):
                session_id = next_session_id
                next_session_id += 1
                prefix_id = (session_id + 1) % args.prefix_count
                prefix_use_counter[prefix_id] += 1
                session_state[session_id] = simulate_session(
                    store, session_id, prefix_id, args, stats
                )
                time.sleep(args.session_pause_ms / 1000.0)
            time.sleep(args.pressure_round_pause_ms / 1000.0)

        verification_hits = 0
        verification_gets = 0
        hot_prefix_ids = [
            prefix_id
            for prefix_id, _ in prefix_use_counter.most_common(min(2, args.prefix_count))
        ]
        for _ in range(args.verify_rounds):
            for prefix_id in hot_prefix_ids:
                for chunk_idx in range(args.prefix_chunks):
                    verification_gets += 1
                    if get_hit(
                        store,
                        f"llm/prefix-{prefix_id}/chunk-{chunk_idx}",
                        stats,
                    ):
                        verification_hits += 1

        print("Synthetic LLM-like Mooncake workload")
        print(f"Metadata server : {args.metadata_url}")
        print(f"Master service  : {args.master_address}")
        print(f"Prefix objects  : {len(prefix_keys)}")
        print(f"Sessions        : {args.session_count}")
        print(
            f"Burst sessions  : {args.burst_session_count} x "
            f"{args.pressure_rounds} rounds"
        )
        print(f"Session states  : {sum(len(keys) for keys in session_state.values())}")
        print(
            "Prefix reuse    : "
            + ", ".join(
                f"prefix-{prefix_id}={count}"
                for prefix_id, count in sorted(prefix_use_counter.items())
            )
        )
        print(
            f"Operations      : puts={stats.puts}, put_failures={stats.put_failures}, "
            f"gets={stats.gets}, "
            f"hits={stats.hits}, misses={stats.misses}, "
            f"hit_rate={stats.hit_rate:.1%}"
        )
        print(
            "Backoff config  : "
            f"retry_sleep={args.put_retry_sleep_ms}ms, "
            f"failure_cooldown={args.put_failure_cooldown_ms}ms, "
            f"session_pause={args.session_pause_ms}ms, "
            f"round_pause={args.pressure_round_pause_ms}ms"
        )
        print(
            f"Hot-prefix probe: {verification_hits}/{verification_gets} hits "
            f"after burst pressure"
        )
        print("Interpretation  :")
        print("  Prefix chunks approximate shared prompt KV cache.")
        print("  Session chunks approximate per-request KV state.")
        print("  Repeated gets approximate decode-time KV reuse.")
        print("  Burst sessions approximate multi-tenant pressure.")
    finally:
        store.close()


if __name__ == "__main__":
    main()
