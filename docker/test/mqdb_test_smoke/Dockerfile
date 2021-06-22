# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-smoke:1.1 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-smoke:1.1 . --push
ARG FROM_TAG=1.1
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:$FROM_TAG

# COPY run.sh /run.sh
# COPY process_split_build_smoke_test_result.py /

SHELL ["/bin/bash", "-c"]
CMD bash
