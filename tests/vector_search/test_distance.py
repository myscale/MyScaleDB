from clickhouse_driver import Client
import sys
import time
import numpy as np

client = Client('localhost', 19000)
table = client.execute('SHOW TABLES')
print(table)

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
    print("Loading sift10M...", end='', file=sys.stderr)
    xt = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs")
    xt = np.float32(xt[:10*1000*1000].copy(order='C'))
    xb = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs")
    xb = np.float32(xb[:10*1000*1000].copy(order='C'))
    print(xb.shape)
    xq = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/query/10M_query.bvecs")
    xq = np.float32(xq.copy(order='C'))
    gt = ivecs_read("/mnt/cephfs/milvus/SIFT10M/gnd/idx_10M.ivecs")
    gt = gt.copy(order='C')
    print(gt.shape)
    print("done", file=sys.stderr)

    return xb, xq, xt, gt

def get_recall_value(true_ids, result_ids):
    """
    Use the intersection length
    """
    sum_radio = 0.0
    # print(result_ids)
    for item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        tmp = set(true_ids[item]).intersection(set(result_ids[item]))
        sum_radio = sum_radio + len(tmp) / len(result_ids[item])
        # logger.debug(sum_radio)
    return round(sum_radio / len(result_ids), 3)

def evaluate(result_id, query_data, truth, k):
    nq = query_data.shape[0]
    print("nq: ",nq)
    print("k: ",k)
    recalls = {}
    i = 1
    results = np.array(result_id)
    batch_search_result = {}
    for k, (k1, _) in results:
        if k1 in batch_search_result.keys():
            batch_search_result[k1].append(k)
        else:
            batch_search_result[k1] = [k]

    recalls = get_recall_value(truth, batch_search_result)
    return recalls

def createTable(tableName):
    client.execute('CREATE TABLE {} (id UInt32, data Array(Float32)) ENGINE = MergeTree primary key id'.format(tableName))

def importSIFT(tableName, query_num=10, build=True, search=True):
    base, query, _, truth = load_sift10M()
    if build:
        data = []
        for i in range(1000000):
            data.append((i, base[i].tolist(),))
        time0 =time.time()
        client.execute('INSERT INTO {} (id,data) VALUES'.format(tableName), data)
        time1 = time.time()
        print("time spent ", time1 - time0)

    if search:
        k = 10
        query_data = query[0:query_num]
        data = []
        for vec in query_data:
            result = client.execute("SELECT id, distance('topK={}')(data, {}) from {}".format(k, vec.tolist(), tableName))
            print("result size: ", len(result))
        # print(data)
        time0 =time.time()
        # print(result)
        time1 = time.time()
        print("time spent: ", time1 - time0)
        # print("accuracy: ", evaluate(result, query_data, truth, k))

def getAll(tableName):
    s = client.execute('SELECT * FROM {}'.format(tableName))
    print(s)

name = "test_sift1M"
# createTable(name)
importSIFT(name, 1, False, True)
