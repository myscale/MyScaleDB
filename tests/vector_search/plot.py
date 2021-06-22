import matplotlib.pyplot as plt
import matplotx
import glob
import json
import csv
import numpy as np
from myscaledb import Client

client = Client(max_query_size=262144000)


# we take a list of tuple as input for plotting
# each tuple is (X_list, Y_list) which represent a single line in graph
# legends are the corresponding name matching each line
# X_name and Y_name correspond to each axis' name
# finally a graph_name for entire graph.
def plot_custom(graph_name="", XY_batch=[()], legends=[], X_name="", Y_name=""):

    # plot
    with plt.style.context(matplotx.styles.dufte):
        for pairs,legend in zip(XY_batch,legends):
            x_axis = [pair[0] for pair in pairs]
            y_axis = [pair[1] for pair in pairs]
            plt.plot(x_axis, y_axis, label=legend)
        plt.xlabel(X_name)
        matplotx.ylabel_top(Y_name)  # move ylabel to the top, rotate
        matplotx.line_labels()  # line labels to the right
        plt.savefig("{}.png".format(graph_name),bbox_inches="tight")
        plt.close()



def get_data_for_qps_acc_coonection_1():
    result = client.fetch("select qps, intersection_accuracy as acc, index_type"
    " from benchmark_result where test_batch_id = (select max(test_batch_id) from benchmark_result)"
                          " and test_suite=1 and connections = 1 order by datapoint_id")
    batch = {}
    for row in result:
        index_type = row["index_type"]
        if index_type not in batch:
            batch[index_type] = []
        batch[index_type].append((row["acc"],row["qps"]))
    return batch.keys(),batch.values()

def get_data_for_latency_acc_coonection_1():
    result = client.fetch("select latency50 as l, intersection_accuracy as acc, index_type"
    " from benchmark_result where test_batch_id = (select max(test_batch_id) from benchmark_result)"
                          " and test_suite=1 and connections = 1 order by datapoint_id")
    batch = {}
    for row in result:
        index_type = row["index_type"]
        if index_type not in batch:
            batch[index_type] = []
        batch[index_type].append((row["acc"],row["l"]))
    return batch.keys(),batch.values()

def get_data_for_qps_connection():
    result = client.fetch("select qps, connections as c,"
                          " index_type from benchmark_result where "
                          " test_batch_id = (select max(test_batch_id) from benchmark_result)"
                          " and test_suite=1 and (parameter = \'nprobe=8\' "
                          " or parameter = \'ef_s=100\') order by datapoint_id")
    batch = {}
    for row in result:
        index_type = row["index_type"]
        if index_type not in batch:
            batch[index_type] = []
        batch[index_type].append((row["c"], row["qps"]))
    return batch.keys(),batch.values()

def get_data_for_latency_connection():
    result = client.fetch("select latency50 as l, connections as c,"
                          " index_type from benchmark_result where "
                          " test_batch_id = (select max(test_batch_id) from benchmark_result)"
                          " and test_suite = 1 and (parameter = \'nprobe=8\' "
                          " or parameter = \'ef_s=100\') order by datapoint_id")
    batch = {}
    for row in result:
        index_type = row["index_type"]
        if index_type not in batch:
            batch[index_type] = []
        batch[index_type].append((row["c"], row["l"]))
    return batch.keys(),batch.values()


def get_data_for_latency_topk():
    result = client.fetch("select topK, latency50 as l,"
                          " index_type from benchmark_result where "
                          " test_batch_id = (select max(test_batch_id) from benchmark_result)"
                          " and test_suite=2 order by datapoint_id")
    batch = {}
    for row in result:
        index_type = row["index_type"]
        if index_type not in batch:
            batch[index_type] = []
        batch[index_type].append((row["topK"], row["l"]))
    return batch.keys(),batch.values()

def get_data_for_batched_query():
    result = client.fetch("select latency50 as l, intersection_accuracy as acc,"
                          " index_type from benchmark_result where "
                          " test_batch_id = (select max(test_batch_id) from benchmark_result)"
                          " and test_suite=3 order by datapoint_id")
    batch = {}
    for row in result:
        index_type = row["index_type"]
        if index_type not in batch:
            batch[index_type] = []
        batch[index_type].append((row["acc"], row["l"]))
    return batch.keys(),batch.values()

def plot_all():
    index_type,qpsVacc = get_data_for_qps_acc_coonection_1()
    plot_custom("qps_vs_acc_connection_1",qpsVacc, index_type, "acc", "qps")

    index_type,lVacc = get_data_for_latency_acc_coonection_1()
    plot_custom("latency_vs_acc_connection_1",lVacc, index_type, "acc", "latency@50/s")

    index_type,qpsVc = get_data_for_qps_connection()
    plot_custom("qps_vs_connection",qpsVc, index_type, "connections", "qps")

    index_type,lVc = get_data_for_latency_connection()
    plot_custom("latency_vs_connection",lVc, index_type, "connections", "latency@50/s")

    index_type,lVc = get_data_for_latency_topk()
    plot_custom("latency_vs_topk",lVc,index_type,"topK","latency@50/s")

    index_type,lVacc = get_data_for_batched_query()
    plot_custom("latency_vs_acc_at_batch_size_10000",lVacc,index_type,"acc","latency@50/s")

plot_all()
#yaxis_2 = "latency"
#index_type,latVacc = get_data_for_latency_acc()
#plot_custom("latency vs acc",latVacc, index_type, "acc", "latency")



