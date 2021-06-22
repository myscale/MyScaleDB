import os
import threading
import time

##############################
####defined variables are here
##############################

thread_num=100
clickhouse_client="/server/build_docker/programs/clickhouse-client"
is_tcp=False

#stress test 

# test_query_vector = [line.strip() for line in open("/server/tests/vector_search/query_vector.txt", 'r')][0]

# search_query = "SELECT docId, headLine, distance('topK = 10')(vector, {}) as d FROM {} ORDER BY d ASC".format(test_query_vector, "test_xhs")

test_query_vector = [line.strip() for line in open("/home/ubuntu/workspace/ClickHouse/tests/vector_search/query_vector_sift.txt", 'r')][0]

search_query = "SELECT id, distance('topK = 10')(data, {}) FROM {}".format(test_query_vector, "test_sift10M")
##############################
print("thread num: ",thread_num)

def do_search():
    if is_tcp:
        os.system("{} -q \"{}\" > /dev/null 2>&1".format(clickhouse_client, search_query))
    else:
        os.system("echo \"{}\"  | curl '{}' --data-binary @- > /dev/null 2>&1".format(search_query, "http://localhost:8123/"))

# stree test time in second
threadlist = []

start_time = time.time()
for i in range(thread_num):
    threadlist.append(threading.Thread(target=do_search))
    threadlist[i].start()
for i in range(thread_num):
    threadlist[i].join()

print("thread: {} latency: {}".format(thread_num, time.time() - start_time))