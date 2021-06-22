import sys
import time
import os
import numpy as np

client = "/server/build_docker/programs/clickhouse-client"
xb_data_path = "/mnt/cephfs/milvus/SIFT10M/sift1B.bvecs"
table_name = "test_sift10M"
data_size = 10000000
index_type = "IVFFLAT"

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

def load_sift():
    file_name = table_name + str(data_size)
    if os.path.isfile(file_name):
        print("{} exist!")
        return file_name
    

    print("Loading sift10M...", end='', file=sys.stderr)
    xb = mmap_bvecs(xb_data_path)
    xb = np.float32(xb[:10*1000*1000].copy(order='C'))
    print(xb.shape)
    data = []
    with open(file_name, 'w') as f:
        for i in range(data_size):
            f.write("\"{}\",\"{}\"\n".format(i, xb[i].tolist()))

    return file_name

def create_table():
    print("drop table {}".format(table_name))
    os.system('{} -q "DROP TABLE IF EXISTS {}"'.format(client, table_name))

    print("create table {}".format(table_name))
    os.system('{} -q "CREATE TABLE {} (id UInt32, Array(Float32), CONSTRAINT data_len CHECK length(data) = 128) ENGINE = MergeTree primary key id"'.format(client, table_name))

def insert_data():
   #base, query, train, truth = load_sift10M()
    data_file = load_sift()
    
    time0 =time.time()
    print("Start to insert {} rows into {}".format(data_size, table_name))
    insert_string = '{} -q "INSERT INTO {} FORMAT CSV" < {}'.format(client, table_name, data_file)
    os.system(insert_string)
    time1 = time.time()
    print("insert time: {}".format(time1 - time0))

def create_index():
    os.system('{} -q "ALTER TABLE {} ADD VECTOR INDEX v1 data TYPE {}"'.format(client, table_name, index_type))

create_table()
insert_data()
create_index()