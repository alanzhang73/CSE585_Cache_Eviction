from __future__ import annotations

import argparse
import ctypes
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan-resistance benchmark for Mooncake eviction policies. "
            "Creates a referenced warm set, then a one-hit scan, then a second "
            "pressure wave that forces eviction among expired objects."
        )
    )
    parser.add_argument("--node-address", default="localhost")
    parser.add_argument("--metadata-url", default="http://localhost:8080/metadata")
    parser.add_argument("--master-address", default="localhost:50051")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--policy-label", default="unknown")
    parser.add_argument("--run-id", default=f"scan-{int(time.time())}")
    parser.add_argument("--global-segment-size", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--local-buffer-size", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--object-bytes", type=int, default=768 * 1024)
    parser.add_argument("--warm-count", type=int, default=8)
    parser.add_argument("--scan-a-count", type=int, default=8)
    parser.add_argument("--scan-b-count", type=int, default=10)
    parser.add_argument("--lease-wait-seconds", type=float, default=6.5)
    parser.add_argument("--put-retries", type=int, default=3)
    parser.add_argument("--put-retry-sleep-ms", type=int, default=200)
    return parser.parse_args()


def phase(name: str, detail: str) -> None:
    print(f"{name}: {detail}")


def namespaced_key(args: argparse.Namespace, group: str, idx: int) -> str:
    return f"scan/{args.run_id}/{group}-{idx}"


def put_with_retries(
    store: MooncakeDistributedStore, key: str, payload: bytes, args: argparse.Namespace
) -> bool:
    for _ in range(args.put_retries):
        rc = store.put(key, payload)
        if rc == 0:
            return True
        time.sleep(args.put_retry_sleep_ms / 1000.0)
    return False


def key_exists(store: MooncakeDistributedStore, key: str) -> bool:
    return store.get(key) != b""


def count_hits(store: MooncakeDistributedStore, keys: list[str]) -> tuple[int, list[str]]:
    survivors: list[str] = []
    for key in keys:
        if key_exists(store, key):
            survivors.append(key)
    return len(survivors), survivors


def main() -> None:
    args = parse_args()
    payload = b"X" * args.object_bytes

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

    warm_keys = [namespaced_key(args, "warm", i) for i in range(args.warm_count)]
    scan_a_keys = [namespaced_key(args, "scan-a", i) for i in range(args.scan_a_count)]
    scan_b_keys = [namespaced_key(args, "scan-b", i) for i in range(args.scan_b_count)]

    scan_b_successes = 0
    scan_b_failures = 0

    try:
        phase(
            "Phase 0",
            "policy="
            f"{args.policy_label}, segment={args.global_segment_size // (1024 * 1024)}MB, "
            f"object={args.object_bytes // 1024}KB, warm={args.warm_count}, "
            f"scan_a={args.scan_a_count}, scan_b={args.scan_b_count}",
        )

        phase("Phase 1", "insert warm working set")
        for key in warm_keys:
            if not put_with_retries(store, key, payload, args):
                raise RuntimeError(f"failed to seed warm key: {key}")

        phase("Phase 2", "touch warm set to set recently_referenced")
        warm_hits = 0
        for key in warm_keys:
            if key_exists(store, key):
                warm_hits += 1
        phase("Phase 2", f"warm_refresh={warm_hits}/{len(warm_keys)}")

        phase(
            "Phase 3",
            f"sleep {args.lease_wait_seconds:.1f}s so warm set becomes evictable but stays referenced",
        )
        time.sleep(args.lease_wait_seconds)

        phase("Phase 4", "insert first one-hit scan wave without rereads")
        for key in scan_a_keys:
            if not put_with_retries(store, key, payload, args):
                raise RuntimeError(f"failed to seed scan-a key: {key}")

        phase(
            "Phase 5",
            f"sleep {args.lease_wait_seconds:.1f}s so scan-a also becomes evictable",
        )
        time.sleep(args.lease_wait_seconds)

        phase("Phase 6", "insert second scan wave to force eviction among expired keys")
        for key in scan_b_keys:
            if put_with_retries(store, key, payload, args):
                scan_b_successes += 1
            else:
                scan_b_failures += 1

        warm_survivor_count, warm_survivors = count_hits(store, warm_keys)
        scan_a_survivor_count, scan_a_survivors = count_hits(store, scan_a_keys)
        scan_b_survivor_count, scan_b_survivors = count_hits(store, scan_b_keys)

        phase(
            "Phase 7",
            f"warm={warm_survivor_count}/{len(warm_keys)}, "
            f"scan_a={scan_a_survivor_count}/{len(scan_a_keys)}, "
            f"scan_b={scan_b_survivor_count}/{len(scan_b_keys)}",
        )

        print()
        print(f"Policy: {args.policy_label}")
        print(
            "Writes: "
            f"scan_b_successes={scan_b_successes}, scan_b_failures={scan_b_failures}"
        )
        print(f"Warm survivors ({warm_survivor_count}): {warm_survivors}")
        print(f"Scan-A survivors ({scan_a_survivor_count}): {scan_a_survivors}")
        print(f"Scan-B survivors ({scan_b_survivor_count}): {scan_b_survivors}")
        print()
        print("How to read this:")
        print("  A better scan-resistant policy should preserve more warm keys.")
        print("  A worse policy will evict warm keys in favor of one-hit scan data.")
        print(
            "  For the current SIEVE approximation, the expected advantage comes from "
            "warm keys being read once while scan keys are never reread."
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
