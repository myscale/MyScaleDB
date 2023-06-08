FROM harbor.internal.moqi.ai/mqdb/minimum_test-k8s:1.1.0

WORKDIR /home/minimum_test/lwd_feature_test 
ARG https_proxy=http://clash.internal.moqi.ai:7890 
ARG http_proxy=http://clash.internal.moqi.ai:7890
RUN wget -P /home/minimum_test/lwd_feature_test/data_files https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/minimum_test/mqdb_datasets/baidu-768-100w-angular.hdf5 
CMD ["bash", "-c", "python3 main.py --host=${HOST} --new-sql-grammar --timeout=${TIMEOUT} --run-index-type=\"${INDEX_TYPE}\" && tail -f /dev/null"]