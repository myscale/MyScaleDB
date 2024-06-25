#!/bin/bash
set -e

function get_current_memory() {
    # get the total memory in bytes
    mem_total=$(cat /proc/meminfo | grep MemTotal | awk '{print $2}')
    mem_total=$(expr $mem_total \* 1024)
    echo $mem_total
}

function init_ck_config() {
    mem_total=$(get_current_memory)
    mark_cache_ratio='0.05' # 5%
    uncompressed_cache_ratio='0.075' # 7.5%
    # primary_key_cache_ratio='0.1' # 10%
    max_bytes_to_merge_ratio='1.25' # 125%
    mark_cache_size=$(echo "$mem_total * $mark_cache_ratio" | bc | awk '{printf "%d", $0}')
    uncompressed_cache_size=$(echo "$mem_total * $uncompressed_cache_ratio" | bc | awk '{printf "%d", $0}')
    primary_key_cache_size=67108864 # 64MB
    max_bytes_to_merge_at_max_space_in_pool=$(echo "$mem_total * $max_bytes_to_merge_ratio" | bc | awk '{printf "%d", $0}')
    
    echo "<clickhouse><mark_cache_size>$mark_cache_size</mark_cache_size></clickhouse>" > /etc/clickhouse-server/config.d/mark_cache_size.xml
    echo "<clickhouse><uncompressed_cache_size>$uncompressed_cache_size</uncompressed_cache_size></clickhouse>" > /etc/clickhouse-server/config.d/uncompressed_cache_size.xml
    echo "<clickhouse><primary_key_cache_size>$primary_key_cache_size</primary_key_cache_size></clickhouse>" > /etc/clickhouse-server/config.d/primary_key_cache_size.xml
    echo "<clickhouse><merge_tree><max_bytes_to_merge_at_max_space_in_pool>$max_bytes_to_merge_at_max_space_in_pool</max_bytes_to_merge_at_max_space_in_pool></merge_tree></clickhouse>" > /etc/clickhouse-server/config.d/max_bytes_to_merge_at_max_space_in_pool.xml
}
init_ck_config