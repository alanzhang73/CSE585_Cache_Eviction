from __future__ import annotations

import argparse
import ctypes
import os
import sys
import time
from typing import Iterable


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise Mooncake eviction with mixed object sizes and staggered "
            "lease ages so different eviction policies are easier to compare."
        )
    )
    parser.add_argument("--node-address", default="localhost")
    parser.add_argument(
        "--metadata-server",
        default="http://localhost:8080/metadata",
    )
    parser.add_argument("--master-server", default="localhost:50051")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--global-segment-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--local-buffer-size", type=int, default=8 * 1024 * 1024)
    parser.add_argument(
        "--policy-label",
        default="unknown",
        help="Just for labeling output. Set this to the master policy you started.",
    )
    parser.add_argument(
        "--lease-wait-seconds",
        type=float,
        default=6.5,
        help="Must exceed master default_kv_lease_ttl. Default TTL is 5 seconds.",
    )
    parser.add_argument(
        "--age-gap-seconds",
        type=float,
        default=0.35,
        help="Delay between early seed writes to make original policy prefer them.",
    )
    parser.add_argument(
        "--pressure-count",
        type=int,
        default=48,
        help="Number of pressure writes after the seed phase.",
    )
    return parser.parse_args()


def put_blob(
    store: MooncakeDistributedStore, key: str, size_bytes: int, *, must_succeed: bool = True
) -> bool:
    payload = bytes([65 + (len(key) % 26)]) * size_bytes
    rc = store.put(key, payload)
    if rc != 0:
        if must_succeed:
            raise RuntimeError(f"put failed for {key} with rc={rc}")
        return False
    return True


def key_exists(store: MooncakeDistributedStore, key: str) -> bool:
    value = store.get(key)
    return value != b""


def report_group(store: MooncakeDistributedStore, title: str, keys: Iterable[str]) -> None:
    survivors = [key for key in keys if key_exists(store, key)]
    missing = [key for key in keys if key not in survivors]
    print(f"{title}:")
    print(f"  survivors ({len(survivors)}): {survivors}")
    print(f"  missing   ({len(missing)}): {missing}")


def main() -> None:
    args = parse_args()

    store = MooncakeDistributedStore()
    rc = store.setup(
        args.node_address,
        args.metadata_server,
        args.global_segment_size,
        args.local_buffer_size,
        args.protocol,
        args.rdma_devices,
        args.master_server,
    )
    if rc != 0:
        raise RuntimeError(f"store.setup failed with rc={rc}")

    early_small_keys = [f"early_small_{i}" for i in range(8)]
    late_large_keys = [f"late_large_{i}" for i in range(4)]
    mid_medium_keys = [f"mid_medium_{i}" for i in range(6)]

    try:
        print(f"Running workload against policy label: {args.policy_label}")
        print("Phase 1: insert older small objects")
        for key in early_small_keys:
            put_blob(store, key, 256 * 1024)
            time.sleep(args.age_gap_seconds)

        print("Phase 2: insert medium objects")
        for key in mid_medium_keys:
            put_blob(store, key, 768 * 1024)

        print("Phase 3: insert newer large objects")
        for key in late_large_keys:
            put_blob(store, key, 4 * 1024 * 1024)

        print(
            f"Sleeping {args.lease_wait_seconds:.1f}s so the seeded keys become "
            "eligible for eviction"
        )
        time.sleep(args.lease_wait_seconds)

        print("Phase 4: apply write pressure to trigger eviction")
        pressure_successes = 0
        pressure_failures = 0
        for i in range(args.pressure_count):
            if put_blob(store, f"pressure_{i}", 1024 * 1024, must_succeed=False):
                pressure_successes += 1
            else:
                pressure_failures += 1

        print()
        print("Survival report after eviction pressure")
        report_group(store, "Older small keys", early_small_keys)
        report_group(store, "Medium keys", mid_medium_keys)
        report_group(store, "Newer large keys", late_large_keys)
        print(f"Pressure writes: success={pressure_successes}, failures={pressure_failures}")

        print()
        print("How to read this:")
        print("  original tends to evict the oldest expired keys first")
        print("  size_aware tends to evict the largest expired keys first")
        print(
            "  sieve prefers expired objects that have not been recently "
            "referenced, then falls back to lease timeout ordering"
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
