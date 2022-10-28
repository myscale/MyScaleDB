FROM harbor.internal.moqi.ai/mqdb/mqdb-stability-benchmark-client:1.1.0

# COPY bench_query.sql /home/.
COPY benchmark.sh /home/.

WORKDIR /home/

RUN pwd
RUN ls -lh
ENTRYPOINT ["/home/benchmark.sh"]
# CMD ["/home/benchmark.sh"]
