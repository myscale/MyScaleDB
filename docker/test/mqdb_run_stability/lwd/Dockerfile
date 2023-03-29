FROM harbor.internal.moqi.ai/mqdb/minimum_test-ci:1.0

WORKDIR /home/minimum_test/lwd_feature_test 
CMD ["/bin/bash","-c","pwd && python3 main.py --host=${HOST} --timeout=${TIMEOUT}"]