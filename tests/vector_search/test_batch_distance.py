from clickhouse_driver import Client
import sys
import time
import numpy as np
import threading
import os

client = Client('localhost', 10000)
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
    #xt = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs")
    #xt = np.float32(xt[:10*1000*1000].copy(order='C'))
    #xb = mmap_bvecs("/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs")
    #xb = np.float32(xb[:10*1000*1000].copy(order='C'))
    #print(xb.shape)
    xt = fvecs_read("/mnt/cephfs/milvus/SIFT10M/sift10M.fvecs")
    xt = np.float32(xt[:10*1000*1000].copy(order='C'))
    xb = xt
    print(xt[1])
    xq = fvecs_read("/mnt/cephfs/milvus/SIFT10M/query/10M_query.fvecs")
    xq = np.float32(xq.copy(order='C'))
    gt = ivecs_read("/mnt/cephfs/milvus/SIFT10M/gnd/idx_10M.ivecs")
    gt = gt.copy(order='C')
    print(gt.shape)
    print("done", file=sys.stderr)

    return xb, xq, xt, gt

def load_sift1M():
    print("Loading sift1M...", end='', file=sys.stderr)
    xt = fvecs_read("/mnt/cephfs/milvus/SIFT10M/sift1M.fvecs")
    xt = np.float32(xt[:1*1000*1000].copy(order='C'))
    xb = fvecs_read("/mnt/cephfs/milvus/SIFT10M/sift1M.fvecs")
    xb = np.float32(xb[:1*1000*1000].copy(order='C'))
    print(xb.shape)
    xq = fvecs_read("/mnt/cephfs/milvus/SIFT10M/query/1M_query.fvecs")
    xq = np.float32(xq.copy(order='C'))
    gt = ivecs_read("/mnt/cephfs/milvus/SIFT10M/gnd/idx_1M.ivecs")
    gt = gt.copy(order='C')
    print(gt.shape)
    print("done", file=sys.stderr)

    return xb, xq, xt, gt

base, query, train, truth = load_sift10M()

def get_recall_value(true_ids, result_ids, top_k):
    """
    Use the intersection length
    """
    sum_ratio = 0.0
    acc_req = 0.9
    # print(result_ids)
    temps =[]
    for _, item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        tmp = set(true_ids[item][:top_k]).intersection(set(result_ids[item]))
        sum_ratio = len(tmp) / len(result_ids[item])
        if sum_ratio > acc_req:
            temps.append(sum_ratio)
        # logger.debug(sum_radio)
    return len(temps)/len(result_ids)

def evaluate(id, result_id, query_data, truth, top_k):
    nq = query_data.shape[0]
    recalls = {}
    results = np.array(result_id, dtype=object)
    batch_search_result = {}
    for k, (k1, _) in results:
        if k1 in batch_search_result.keys():
            batch_search_result[k1].append(k)
        else:
            batch_search_result[k1] = [k]

    recalls = get_recall_value(truth[id*query_per_thread:], batch_search_result, top_k)
    return recalls

def createTable(tableName, metrics, indexType):
    client = "clickhouse-client --port 10000"
    if (tableName,) not in table:
        os.system('{} -q \"CREATE TABLE {} (id UInt32, data Array(Float32), CONSTRAINT data_len CHECK length(data) = 128) ENGINE = MergeTree primary key id\"'.format(client,tableName))
        os.system('{} -q \"Alter TABLE {} add vector index v1 data TYPE {}({})\"'.format(client,tableName,indexType,metrics))

def importSIFT(id, insert_csv, search_data, tableName, query_num=10, k=50,  build=True, search=True,stress_test_time=0):
   #base, query, train, truth = load_sift10M()
    local_client = Client('localhost', 10000)
    client = "clickhouse-client --port 10000"
    time_start = time.time()
    while True:
        if build:
            print(id,"   start insert")
            time0 =time.time()
            insert_string = '{} -q \"INSERT INTO {} FORMAT CSV\" < {}'.format(client,tableName,insert_csv)
            os.system(insert_string)
            time1 = time.time()
            print(id,"  insert time spent ", time1 - time0)
    
        if search:
            # print(data)
            local_client.execute("SET max_query_size = 262144000")
            print(local_client.execute("select getSetting('max_query_size')"))
            print(id,"   start query")
            time0 =time.time()
            result = local_client.execute("SELECT id, batch_distance('topK={}','acc=0.9')(data, {}) from {}".format(k, search_data, tableName))
            # print(result)
            time1 = time.time()
            print(id," search time spent: ", time1 - time0)
            print(id," percent above 0.9 acc: ", evaluate(id, result, query[id*query_num:(id+1)*query_num], truth, k))
        
        if(time.time()-time_start>stress_test_time):
            break

def getAll(tableName):
    s = client.execute('SELECT * FROM {}'.format(tableName))
    print(s)



##############################
####defined variables are here
##############################

name = "test_10M_no_normal"
metrics = "'metric_type = L2', 'compression_scheme=LZ4','mode = CPU'"
index_type = "IVFFLAT"
thread_num=1
insert_data_per_thread = 100000
k = 50
query_per_thread=100
#insert_file_name = "/mnt/cephfs/milvus/SIFT10M/sift10M.csv"
insert_file_name=""
stress_test_time = 0

#stress test 
search = True
insert = False

##############################


print("thread num: ",thread_num)
print("nq: ",query_per_thread*thread_num)
print("k: ",k)


search_data = []
for i in range(thread_num*query_per_thread):
    search_data.append(query[i].tolist())

if len(insert_file_name)==0 and (insert or stress_test_time>0):
    insert_data = []
    for i in range(thread_num*insert_data_per_thread):
        if i%2!=0:
            insert_data.append((i, base[i].tolist(),))
        else:
            insert_data.append((i, [],))
    import csv
    with open('temp.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(insert_data)
    insert_file_name = 'temp.csv'
# stree test time in second
createTable(name,metrics,index_type)
threadlist = []
if stress_test_time>0:
    #devide up threads into 20% insert, 80%select
    for i in range(int(thread_num*0.2)):
        t = threading.Thread(target=importSIFT, args=(i,insert_file_name,[], name,query_per_thread, k, True, False,stress_test_time))
        t.start()
        threadlist.append(t)
    for i in range(int(thread_num*0.8)):
        t = threading.Thread(target=importSIFT, args=(i,insert_file_name,search_data[i*query_per_thread:(i+1)*query_per_thread], name,query_per_thread, k, False, True,stress_test_time))
        t.start()
        threadlist.append(t)
    for i in range(thread_num):
        threadlist[i].join()

else:
    t3 = time.time()
    for i in range(thread_num):
        threadlist.append(threading.Thread(target=importSIFT, args=(i,insert_file_name,
            search_data[i*query_per_thread:(i+1)*query_per_thread],name, query_per_thread, k, insert, search)))
        threadlist[i].start()
    for i in range(thread_num):
        threadlist[i].join()
    print("total time:",time.time()-t3)
if insert or stress_test_time>0:
    os.system("rm tmp.csv")
