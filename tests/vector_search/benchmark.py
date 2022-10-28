import sys
import time
import numpy as np
import threading
import os
from myscaledb import Client
import csv
import json
from plot import plot_all

##############################
####defined variables are here
##############################

# benchmark result are written both into the benchamrk_result table in the benchmarked clickhouse instance,
# and also the benchmark_result/{}.json directory.
# for each test suite, please refer to the Vector Index Benchmark Design on quip

########################### benchmark configuration
name = "benchmark_table"  # the table name used for benchmarking
dimension = 128  # dimension of inpu data
metrics = "'metric_type = L2', 'compression_scheme=LZ4','mode = CPU',"  # default metrics for build index
index_type_pack = ["IVFFLAT", "IVFPQ", "HNSWFLAT", "HNSWPQ", "HNSWSQ"]  # collection of index name to test
dataset = "SIFT10M"  # dataset name
insert_file_name = "/mnt/datadisk0/sift10M.csv"  # for convenience, we prepare a csv for insertion instead of fvecs file
build_index_timeout_s = 3600  # timeout for build index
test_id = 0  # test batch id, will be updated from clickhouse later
auto_increment = 0  # datapoint id

########################### stress test configurtion
stress_test_time = 0  # stress test time, set to greater than 0 for stress testing
thread_num = 10  # how many thread to use, only for stress test
insert_data_per_thread = 10000000  # how many lines of data per thread to insert, only for stress test
query_per_thread = 1  #how many queires for each thread, only for stress test
stress_test_k = 50 #k for stress test queries
########################### build index parameters
IVFFLAT_build_param = "'ncentroids=1024'"
IVFPQ_build_param = "'ncentroids=1024','M=32'"
IVFSQ_build_param = "'ncentroids=1024'"
HNSWFLAT_build_param = "'ef_c=100','m=32'"
HNSWPQ_build_param = "'pq_m=32','m=32'"
HNSWSQ_build_param = "'ef_c=100','m=32'"
index_build_param_map = {"IVFFLAT": IVFFLAT_build_param, "IVFPQ": IVFPQ_build_param,
                         "IVFSQ": IVFSQ_build_param, "HNSWFLAT": HNSWFLAT_build_param,
                         "HNSWPQ": HNSWPQ_build_param, "HNSWSQ": HNSWSQ_build_param}
all_index_query_param_map = {}

############################ test suite 1
k_1 = [5]                                           #number of k
nq_1 = [1]                                          # number of queires per request
IVF_query_param_pack_1 = [1, 8, 16, 32, 64]         # nprobe
HNSW_query_param_pack_1 = [50, 100, 200, 400]       # ef_s
connection_pack_1 = [1, 4, 8, 20, 50, 100]          # number of connections per benchmarking session
index_query_param_map_1 = {"IVFFLAT": IVF_query_param_pack_1, "IVFPQ": IVF_query_param_pack_1,
                           "IVFSQ": IVF_query_param_pack_1, "HNSWFLAT": HNSW_query_param_pack_1,
                           "HNSWPQ": HNSW_query_param_pack_1, "HNSWSQ": HNSW_query_param_pack_1}
all_index_query_param_map["test1"] = index_query_param_map_1

############################# test suite 2
k_2 = [5, 50, 500, 5000, 50000]
nq_2 = [1]
IVF_query_param_pack_2 = [8]  # nprobe
HNSW_query_param_pack_2 = [100]  # ef_s
connection_pack_2 = [1]
index_query_param_map_2 = {"IVFFLAT": IVF_query_param_pack_2, "IVFPQ": IVF_query_param_pack_2,
                           "IVFSQ": IVF_query_param_pack_2, "HNSWFLAT": HNSW_query_param_pack_2,
                           "HNSWPQ": HNSW_query_param_pack_2, "HNSWSQ": HNSW_query_param_pack_2}
all_index_query_param_map["test2"] = index_query_param_map_2

############################# test suite 3
k_3 = [50]
nq_3 = [10000]
IVF_query_param_pack_3 = [1, 8, 16, 32, 64]  # nprobe
HNSW_query_param_pack_3 = [50, 100, 200, 400]  # ef_s
connection_pack_3 = [1]
index_query_param_map_3 = {"IVFFLAT": IVF_query_param_pack_3, "IVFPQ": IVF_query_param_pack_3,
                           "IVFSQ": IVF_query_param_pack_3, "HNSWFLAT": HNSW_query_param_pack_3,
                           "HNSWPQ": HNSW_query_param_pack_3, "HNSWSQ": HNSW_query_param_pack_3}
all_index_query_param_map["test3"] = index_query_param_map_3

client = Client(max_query_size=262144000)
client.execute('CREATE TABLE if not exists benchmark_result '
               '(test_batch_id UInt32,'
               ' test_time DateTime,'
               ' latency50 Float32,'
               ' latency90 Float32,'
               ' latency99 Float32,'
               ' qps Float32,'
               ' build_time Float32,'
               ' datapoint_id UInt32,'
               ' index_type Enum8(\'IVFFLAT\'=1,\'HNSWFLAT\'=2,\'HNSWPQ\'=3,\'HNSWSQ\'=4,\'IVFPQ\'=5,\'IVFSQ\'=6),'
               ' dataset Enum8(\'SIFT10M\'=1,\'GIST1M\'=2),'
               ' intersection_accuracy Float32,'
               ' parameter String,'
               ' build_parameter String,'
               ' connections UInt32,'
               ' test_suite UInt32,'
               ' nq UInt32,'
               ' accuracyAt1 Float32,'
               ' accuracyAt10 Float32,'
               ' accuracyAt50 Float32,'
               ' topK UInt32)'
               'ENGINE = MergeTree order by test_time')


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


def mmap_bvecs(fname):
    x = np.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    return x.reshape(-1, d + 4)[:, 4:]


def load_sift10M():
    # print("Loading sift10M...", end='', file=sys.stderr)
    # xt = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs")
    # xt = np.float32(xt[:10*1000*1000].copy(order='C'))
    # xb = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs")
    # xb = np.float32(xb[:10*1000*1000].copy(order='C'))
    xt = []
    xb = []
    xq = mmap_bvecs("/mnt/datadisk0/10M_query.bvecs")
    xq = np.float32(xq.copy(order='C'))
    gt = ivecs_read("/mnt/datadisk0/idx_10M.ivecs")
    gt = gt.copy(order='C')
    print(gt.shape)
    print("done", file=sys.stderr)

    return xb, xq, xt, gt


def load_sift1M():
    print("Loading sift1M...", end='', file=sys.stderr)
    xt = fvecs_read("/mnt/cephfs/milvus/SIFT10M/sift1M.fvecs")
    xt = np.float32(xt[:1 * 1000 * 1000].copy(order='C'))
    xb = fvecs_read("/mnt/cephfs/milvus/SIFT10M/sift1M.fvecs")
    xb = np.float32(xb[:1 * 1000 * 1000].copy(order='C'))
    print(xb.shape)
    xq = fvecs_read("/mnt/cephfs/milvus/SIFT10M/query/1M_query.fvecs")
    xq = np.float32(xq.copy(order='C'))
    gt = ivecs_read("/mnt/cephfs/milvus/SIFT10M/gnd/idx_1M.ivecs")
    gt = gt.copy(order='C')
    print(gt.shape)
    print("done", file=sys.stderr)

    return xb, xq, xt, gt


def get_recall_value(true_ids, result_ids, top_k):
    """
    Use the intersection length
    """
    sum_ratio = 0.0
    # print(result_ids)
    for _, item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        tmp = set(true_ids[item][:top_k]).intersection(set(result_ids[item]))
        sum_ratio = sum_ratio + len(tmp) / len(result_ids[item])
        # logger.debug(sum_radio)
    return round(sum_ratio / len(result_ids), 3)


def get_recall_at_n(true_ids, result_ids, n):
    '''
    if first answer is present in given range of result,
    then it's a hit
    '''

    hit = 0
    for _, item in enumerate(result_ids):
        seek = true_ids[item][0]
        for x in result_ids[item][:n]:
            if x == seek:
                hit += 1
    return round(float(hit) / len(result_ids), 3)


def evaluate(id, result_id, truth, top_k, nq):
    results = np.array(result_id, dtype=object)
    batch_search_result = {}
    for k, (k1, _) in results:
        if k1 in batch_search_result.keys():
            batch_search_result[k1].append(k)
        else:
            batch_search_result[k1] = [k]
    if nq == 1:
        nq = 100
    recalls = get_recall_value(truth[id * nq:], batch_search_result, top_k)
    recallAt1 = get_recall_at_n(truth[id * nq:], batch_search_result, 1)
    if top_k < 10:
        recallAt10 = -1
    else:
        recallAt10 = get_recall_at_n(truth[id * nq:], batch_search_result, 10)
    if top_k < 50:
        recallAt50 = -1
    else:
        recallAt50 = get_recall_at_n(truth[id * nq:], batch_search_result, 50)

    return recalls, recallAt1, recallAt10, recallAt50


def createTable(tableName):
    client.execute('drop table if exists {}'.format(tableName))
    client.execute(
        'CREATE TABLE {} (id UInt32, data Array(Float32), CONSTRAINT data_len CHECK length(data) = 128) ENGINE = MergeTree primary key id'.format(
            tableName))


def importSIFT(id, insert_csv, search_data, tableName, k=50, nq=1, params="", build=True, search=True,
               stress_test_time=0):
    time_start = time.time()
    while True:
        if build:
            print(id, "start insert")
            time0 = time.time()
            insert_string = 'clickhouse-client -q \"INSERT INTO {} FORMAT CSV\" < {}'.format(tableName, insert_csv)
            os.system(insert_string)
            time1 = time.time()
            print(id, "insert time spent ", time1 - time0)
            os.system("clickhouse-client -q \"optimize table {} final\"".format(name))

        if search:
            # print(data)
            print(id, "start query")
            time0 = time.time()
            result = client.fetch(
                "SELECT id, batch_distance('topK={}',{})(data, {}) from {}".format(k, params, search_data, tableName))
            time1 = time.time()
            result = [(x[0],x[1]) for x in result]
            print(id, " search time spent: ", time1 - time0)
            acc, acc1, acc10, acc50 = evaluate(id, result, truth, k, nq)
            print(id, "intersection accuracy:{}, acc@1:{}, acc@10:{}, acc@50:{}", acc, acc1, acc10, acc50)
            return acc, acc1, acc10, acc50

        if (time.time() - time_start > stress_test_time):
            break

    return 0


def benchmark(cc, run_time, run_iteration, query_name):
    if run_iteration > 0:
        query_string = "clickhouse-benchmark -c {} -i {} --cumulative --max_query_size=262144000 --json={} --delay 0 " \
                       "<query.sql".format(cc, run_iteration, query_name)
    else:
        query_string = "clickhouse-benchmark -c {} -t {} --cumulative --max_query_size=262144000 --json={} --delay 0 " \
                       "<query.sql".format(cc, run_time, query_name)
    status = os.system(query_string)
    os.system("rm query.sql")


def generate_query_sql(k, nq, full_param, query_data, table_name):
    with open('query.sql', 'w', newline='') as f:
        if nq > 1:
            # batch query
            f.write("select id,batch_distance('topK={}',{})(data,{}) from {};\n".format(k, full_param, query_data,
                                                                                        table_name))
        elif nq == 1:
            # single query
            for query_data_sub in query_data:
                f.write("select id,distance('topK={}',{})(data,{}) from {};\n".format(k, full_param, query_data_sub,
                                                                                      table_name))

# plot will plot both qps and latency



def run_test_suite(connection_pack, k_pack, nq_pack, index_type, table_name, test_num, run_time, run_iteration=0):
    global auto_increment
    # client = "clickhouse-client"
    for connection in connection_pack:
        for k in k_pack:
            for nq in nq_pack:
                # prepare search data, if the dataset is too small, we prepare 100 queries
                search_data = []
                #if nq is 1, we make 100 queries and append them sequentially in every .sql file
                if nq == 1:
                    for i in range(100):
                        search_data.append(query[i].tolist())
                #if nq is larger than 1, we just make that many queries in every .sql file
                else:
                    for i in range(nq):
                        search_data.append(query[i].tolist())
                test_suite = "test{}".format(test_num)
                for query_param in all_index_query_param_map[test_suite][index_type]:
                    full_param = ""
                    if "IVF" in index_type:
                        full_param = "'nprobe={}'".format(query_param)
                    if "HNSW" in index_type:
                        full_param = "'ef_s={}'".format(query_param)
                    generate_query_sql(k, nq, full_param, search_data, table_name)
                    query_name = "{}-c{}-k{}-nq{}-p{}-{}".format(index_type, connection, k, nq, query_param, test_suite)
                    print(query_name)
                    query_result_path = "benchmark_result/{}.json".format(query_name)
                    benchmark(connection, run_time, run_iteration, query_result_path)
                    acc, acc1, acc10, acc50 = importSIFT(0, "", search_data, name, k, nq, full_param, False, True)
                    with open(query_result_path, "r+") as f:
                        data = json.load(f)
                        data["acc"] = acc
                        data["accAt1"] = acc1
                        data["accAt10"] = acc10
                        data["accAt50"] = acc50
                        data["c"] = connection
                        data["k"] = k
                        data["nq"] = nq
                        data["param"] = query_param
                        data["Index_type"] = index_type
                        stats = data["localhost:9000"]
                        qps = stats["statistics"]["QPS"]
                        latency_50 = stats["query_time_percentiles"]["50"]
                        latency_99 = stats["query_time_percentiles"]["99"]
                        latency_999 = stats["query_time_percentiles"]["99.9"]
                        f.seek(0, 0)
                        json.dump(data, f)
                        write_metrics = build_metric.replace('\'', '')
                        client.execute("INSERT INTO benchmark_result "
                                       "VALUES({},now(),{},{},{},{},{},{},\'{}\',\'{}\',{},{},\'{}\',{},{},{},{},{},{},{})".
                                       format(test_id, latency_50, latency_99, latency_999, qps, build_time,auto_increment,
                                              index_type, dataset, acc, full_param, write_metrics, connection, test_num,
                                              nq, acc1, acc10, acc50,k))
                        auto_increment += 1

                    # we query here


def getAll(tableName):
    s = client.execute('SELECT * FROM {}'.format(tableName))
    print(s)


def prepare_this_batch():
    global test_id
    test_id = client.fetch('select max(test_batch_id) from benchmark_result')[0][0]
    test_id += 1




############################## main logic
base, query, train, truth = load_sift10M()
os.system("mkdir benchmark_result -p")
prepare_this_batch()

# stress test
if stress_test_time > 0:
    createTable(name)
    threadlist = []
    search_data = []
    for i in range(thread_num * query_per_thread):
        search_data.append(query[i].tolist())
    ##this part create csv file for writing, if a csv is indicated, we skip this part
    if len(insert_file_name) == 0 and (stress_test_time > 0):
        insert_data = []
        for i in range(thread_num * insert_data_per_thread):
            insert_data.append((i, base[i].tolist(),))
        with open('temp.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(insert_data)
        insert_file_name = 'temp.csv'

    # devide up threads into 20% insert, 80%select
    for i in range(int(thread_num * 0.2)):
        t = threading.Thread(target=importSIFT,
                             args=(i, insert_file_name, [], name, stress_test_k, 1, "", True, False, stress_test_time))
        t.start()
        threadlist.append(t)
    for i in range(int(thread_num * 0.8)):
        t = threading.Thread(target=importSIFT, args=(
            i, insert_file_name, search_data[i * query_per_thread:(i + 1) * query_per_thread], 1, name, stress_test_k,
            "'nprobe=32'", False, True, stress_test_time))
        t.start()
        threadlist.append(t)
    for i in range(thread_num):
        threadlist[i].join()

# benchmark
else:
    for index_type in index_type_pack:
        createTable(name)
        # insert data and build index
        t3 = time.time()
        importSIFT(0, insert_file_name, [], name, 0, 1, "", True, False)
        insert_time = time.time() - t3
        print("total time taken for insert:", insert_time)
        build_metric = metrics + index_build_param_map[index_type]
        t4 = time.time()
        client.execute(
            'Alter TABLE {} add vector index v1 data TYPE {}({})'.format(name, index_type, build_metric))
        build_status = [[0]]
        build_timer = time.time()
        while build_status[0][0] != 'Built' and (time.time() - build_timer) < build_index_timeout_s:
            build_status = client.fetch(
                "select status from system.vector_indices where table = \'{}\' and name = \'v1\'".format(name))
            time.sleep(1)
        build_time = time.time() - t4
        print("total time taken for build:", build_time)

        # running test suites
        run_test_suite(connection_pack_1, k_1, nq_1, index_type, name, 1, 20)
        # accuracy is inaccurate in test2 case because topk is too big
        print("accuracy is inaccurate in test2 case because topk is too big")
        run_test_suite(connection_pack_2, k_2, nq_2, index_type, name, 2, 20)
        run_test_suite(connection_pack_3, k_3, nq_3, index_type, name, 3, 0, run_iteration=2)

        client.execute("drop table {}".format(name))
        # restart mqdb so that some memory occupied by clickhouse got released
        os.system("supervisorctl restart mqdb")
        time.sleep(20)  # we wait five minutes just so that tables could be dropped and memory cleaned
