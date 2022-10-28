# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-smoke:1.2 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-smoke:1.2 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.2

# COPY run.sh /run.sh
# COPY process_split_build_smoke_test_result.py /

SHELL ["/bin/bash", "-c"]
CMD bash
