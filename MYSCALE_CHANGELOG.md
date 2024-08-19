# MyScale Release Notes

## 2024

### [v1.7.0](https://git.moqi.ai/mqdb/ClickHouse/-/tags/myscale-v1.7.0) - 2024-08-19

Features & Improvements

- Add support for full-text search across multiple text columns.
 [#470](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/470) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Optimize queries per second (QPS) for TextSearch during inserts.
 [#473](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/473) ([Mochi Xu](https://git.moqi.ai/mochix)).

Fixs

- Correct BM25 calculation error in distributed text search.
 [#471](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/471) ([Libao Yang](https://git.moqi.ai/libaoy)).
- Resolve error when performing hybrid search on distributed tables.
 [#472](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/472) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Fix segmentation fault when executing parallel text search selects with FINAL.
 [#474](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/474) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Address various bugs in the full-text search function.
 [#475](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/475) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Fix a critical issue with removing the FTS index cache directory.
 [#477](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/477) ([Mochi Xu](https://git.moqi.ai/mochix)).


