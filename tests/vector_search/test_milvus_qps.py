from pymilvus import (
    connections,
    Collection,
)
import time
import threading

thread_num = 500

vectors_to_search = [[10.0, 14.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 138.0, 51.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 153.0, 58.0, 0.0, 3.0, 16.0, 3.0, 0.0, 0.0, 7.0, 3.0, 0.0, 5.0, 153.0, 41.0, 0.0, 0.0, 2.0, 5.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 147.0, 50.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0, 153.0, 45.0, 0.0, 1.0, 16.0, 1.0, 0.0, 12.0, 15.0, 2.0, 0.0, 2.0, 144.0, 29.0, 0.0, 1.0, 1.0, 1.0, 4.0, 1.0, 0.0, 0.0, 0.0, 0.0, 153.0, 4.0, 0.0, 0.0, 0.0, 0.0, 0.0, 18.0, 153.0, 8.0, 0.0, 0.0, 9.0, 8.0, 1.0, 48.0, 14.0, 0.0, 0.0, 0.0, 87.0, 60.0, 1.0, 4.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 127.0, 22.0, 0.0, 0.0, 0.0, 0.0, 0.0, 4.0, 153.0, 90.0, 0.0, 0.0, 2.0, 3.0, 0.0, 2.0, 20.0, 13.0, 0.0, 0.0, 44.0, 42.0, 0.0, 0.0]]


def do_search():
    connections.connect("default", host="localhost", port="19530")
    hello_milvus = Collection("test_sift10M")
    hello_milvus.load()
    search_params = {
            "metric_type": "l2",
            "params": {"nprobe": 1},
    }
    result = hello_milvus.search(vectors_to_search, "embeddings", search_params, limit=10, output_fields=["pk"])

threadlist = []

start_time = time.time()
for i in range(thread_num):
    threadlist.append(threading.Thread(target=do_search))
    threadlist[i].start()
for i in range(thread_num):
    threadlist[i].join()

print("thread: {} latency: {}".format(thread_num, time.time() - start_time))