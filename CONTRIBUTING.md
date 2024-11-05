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
