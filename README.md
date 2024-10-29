# MyDuckServer

## Dependencies

Install the required package using pip:

```bash
pip3 install "sqlglot[rs]"
```

### MacOS Specific Instructions

If you encounter the following error on MacOS:

```
error: externally-managed-environment

× This environment is externally managed
```

You can resolve it by adding the `--break-system-packages` flag:

```bash
pip3 install "sqlglot[rs]" --break-system-packages
```

## Build and Run

To build and run MyDuckServer:

```bash
make
make run
```

Then, in another terminal, connect to the MySQL server:

```bash
mysql -h127.0.0.1 -uroot -P3306 -Ac
```

## Try with Docker

Our Docker container includes both MyDuckServer and the replica setup tool. You can choose the operating mode by setting the `setup_mode` environment variable. Below are instructions for each setup mode.

### SERVER_ONLY (Default Mode)

In `SERVER_ONLY` mode, MyDuckServer runs by itself. This is the default mode.

```bash
docker run \
  --network=host \
  --privileged \
  --workdir=/home/admin \
  --detach=true \
  apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/myduckserver:latest
```

### REPLICA_ONLY

In `REPLICA_ONLY` mode, only the replica setup tool runs. The container will exit after the setup completes.

```bash
docker run \
  --network=host \
  --privileged \
  --workdir=/home/admin \
  --env=SETUP_MODE=REPLICA_ONLY \
  --env=MYSQL_HOST=<mysql_host> \
  --env=MYSQL_PORT=<mysql_port> \
  --env=MYSQL_USER=<mysql_user> \
  --env=MYSQL_PASSWORD=<mysql_password> \
  --detach=true \
  apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/myduckserver:latest
```

### COMBINED

In `COMBINED` mode, MyDuckServer starts first, followed by the replica setup tool.

```bash
docker run \
  --network=host \
  --privileged \
  --workdir=/home/admin \
  --env=SETUP_MODE=combined \
  --env=MYSQL_HOST=<mysql_host> \
  --env=MYSQL_PORT=<mysql_port> \
  --env=MYSQL_USER=<mysql_user> \
  --env=MYSQL_PASSWORD=<mysql_password> \
  --detach=true \
  apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/myduckserver:latest
```

> **Note**: Replace the Docker image repository URL once available on Docker Hub.