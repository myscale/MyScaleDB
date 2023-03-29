FROM harbor.internal.moqi.ai/mqdb/mqdb-ssh-client:22.3.7.5-774714b3

COPY bench_query.sql /home/.
COPY benchmark.sh /home/.

WORKDIR /home/

RUN pwd
RUN ls -lh
ENTRYPOINT ["/home/benchmark.sh"]
# CMD ["/home/benchmark.sh"]
