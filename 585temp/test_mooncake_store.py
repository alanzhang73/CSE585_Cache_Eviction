import ctypes
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
store = MooncakeDistributedStore()

store.setup(
    "localhost",                    # your node address
    "http://localhost:8080/metadata",  # metadata server
    64 * 1024 * 1024,             # 512 MB segment size
    8 * 1024 * 1024,             # 128 MB local buffer
    "tcp",                         # use TCP
    "",                            # rdma_devices: empty for local TCP test
    "localhost:50051"              # master service
)

for i in range(2000):
    store.put("hello_key" + str(i), b"A" * 1024 *256)
    # time.sleep(.1)
data = store.get("hello_key5")
# print(data.decode())

store.close()



# baseline
# I20260412 10:46:19.615314 53919 master_service.cpp:3092] client_id=2975395154147425646-11567303459873391269, action=client_expired
# I20260412 10:46:23.616050 53919 master_service.cpp:3092] client_id=17531405076750858226-17308708717917791152, action=client_expired
# I20260412 10:46:26.615259 53923 rpc_service.cpp:41] Master Metrics: Mem Storage: 0 B / 0 B | SSD Storage: 0 B / 0 B | Keys: 0 (soft-pinned: 0) | Clients: 0 | Requests (Success/Total): PutStart=4000/4000, PutEnd=4000/4000, PutRevoke=0/0, Get=0/2, Exist=0/0, Del=0/0, DelAll=0/0, Ping=6/6, CopyStart=0/0, CopyEnd=0/0, CopyRevoke=0/0, MoveStart=0/0, MoveEnd=0/0, MoveRevoke=0/0, EvictDiskReplica=0/0 | Batch Requests (Req=Success/PartialSuccess/Total, Item=Success/Total): PutStart:(Req=0/0/0, Item=0/0), PutEnd:(Req=0/0/0, Item=0/0), PutRevoke:(Req=0/0/0, Item=0/0), Get:(Req=0/0/0, Item=0/0), ExistKey:(Req=0/0/0, Item=0/0), QueryIp:(Req=0/0/0, Item=0/0), Clear:(Req=0/0/0, Item=0/0), CreateMoveTask:(Req=0/0), CreateCopyTask:(Req=0/0), QueryTask=(Req=0/0), FetchTasks=(Req=6/6), MarkTaskToComplete= (Req=0/0),  | Eviction: Success/Attempts=192/192, keys=3532, size=883.00 MB | Discard: Released/Total=0/0, StagingSize=0 B | Snapshots: Success=0, Fail=0