function proxy() {
    export http_proxy="http://clash.internal.moqi.ai:7890"
    export https_proxy="http://clash.internal.moqi.ai:7890"
    export no_proxy="localhost,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
}

function unproxy() {
    unset http_proxy
    unset https_proxy
    unset no_proxy
}

proxy
