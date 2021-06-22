function proxy() {
    export http_proxy="http://10.10.1.13:7890"
    export https_proxy="http://10.10.1.13:7890"
}

function unproxy() {
    unset http_proxy
    unset https_proxy
}

proxy
