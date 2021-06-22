# Builder

## 镜像

```bash
# cat /etc/systemd/system/docker.service.d/proxy.conf
[Service]
Environment="HTTP_PROXY=clash.internal.moqi.ai:7890"
Environment="HTTPS_PROXY=clash.internal.moqi.ai:7890"
Environment="NO_PROXY=localhost,127.0.0.1,10.0.0.0/8,mirror.ccs.tencentyun.com,moqi.ai,moqi.com.cn,mirrors.tuna.tsinghua.edu.cn"
```

## 编译

```bash
docker/builder/build.py --output artifacts --docker
```

## 打包

```bash
docker/builder/build.py --output artifacts --package --docker
```
