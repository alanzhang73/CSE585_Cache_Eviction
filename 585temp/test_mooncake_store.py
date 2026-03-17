from mooncake.store import MooncakeDistributedStore
import time

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