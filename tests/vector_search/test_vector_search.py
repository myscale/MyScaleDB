import os

clickhouse_client = "clickhouse-client"

schema_template = "CREATE TABLE {}(docId String, headLine String, content String, createDate DateTime64(1, 'Asia/Shanghai'), author String, searchCategoryId String, level UInt16, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 1280) engine MergeTree primary key docId"

select_1_template = "SELECT docId, headLine, distance('topK = 10')(vector, {}) as d FROM {} ORDER BY d ASC"

select_2_template = "SELECT docId, headLine, distance('topK = 10')(vector, {}) as d FROM {} PREWHERE headLine like '%A%' ORDER BY d ASC"

select_3_template = "SELECT docId, headLine, distance('topK = 10')(vector, {}) as d FROM {} WHERE d < 900 ORDER BY d ASC"

test_table_name = "test_xhs1"

test_query_vector = [line.strip() for line in open("/server/tests/vector_search/query_vector.txt", 'r')][0]
# print(test_query_vector)

## connect with mqdb

def createTable(table_name, drop_table):
    if drop_table:
        try:
            os.system("{} -q \"DROP TABLE {}\"".format(clickhouse_client, table_name))
        except:
            pass
        os.system("{} -q \"{}\"".format(clickhouse_client, schema_template.format(table_name)))

def dropTable(table_name):
    os.system("{} -q \"DROP TABLE {}\"".format(clickhouse_client, table_name))
    
def createIndex(table_name, vector_name, column, type_info):
    os.system("{} -q \"ALTER TABLE {} ADD VECTOR INDEX {} {} TYPE {}\"".format(clickhouse_client, table_name, vector_name, column, type_info))

def dropIndex(table_name, vector_name):
    os.system("{} -q \"ALTER TABLE {} DROP VECTOR INDEX {}\"".format(clickhouse_client, table_name, vector_name))    
        
def insertFromCSV(table_name, file_name):
    os.system("{} -q \"INSERT INTO {} FORMAT CSV\" < {}".format(clickhouse_client, table_name, file_name))

def selectQuery(table_name, query_vector, select_query):
    os.system("{} -q \"{}\"".format(clickhouse_client, select_query.format(query_vector, table_name)))

def showTable():
    s = os.system("{} -q \"SHOW TABLES\"".format(clickhouse_client))
    print("Existing Tables: ")
    print(s)
    
def showIndex(table_name):
    s = os.system("{} -q \"SELECT * FROM system.vector_indices where table = '{}'\"".format(clickhouse_client, table_name))
    print("Show Vector Index Info")
    print(s)

## create table based on schema_template
print("create table")
createTable(test_table_name, True)
## insert data from a CSV file
print("insert")
insertFromCSV(test_table_name, "/server/tests/vector_search/res.csv")
## create vector index
print("create index")
createIndex(test_table_name, "v1", "vector", "HNSWFLAT('m = 5')")
## search based on select_template
print("select")
selectQuery(test_table_name, test_query_vector, select_1_template)
print("select with prewhere")
selectQuery(test_table_name, test_query_vector, select_2_template)
print("select filter by distance")
selectQuery(test_table_name, test_query_vector, select_3_template)




