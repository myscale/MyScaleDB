# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-smoke:2.0.1 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-smoke:2.0.1 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:2.0.1

# COPY run.sh /run.sh
# COPY process_split_build_smoke_test_result.py /

SHELL ["/bin/bash", "-c"]
CMD bash
