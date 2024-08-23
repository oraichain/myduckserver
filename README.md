# MyDuckServer

## Dependencies
```
pip3 install "sqlglot[rs]"
```
To install it on macOS, if you encounter the following error:
```
error: externally-managed-environment

× This environment is externally managed
```
You may need to run the installation command with the --break-system-packages option:
```
pip3 install "sqlglot[rs]" --break-system-packages
```

## Build and Run
```
make
make run
```

In another terminal, connect to MySQL using:
```
mysql -h127.0.0.1 -uroot -P3306 -Ac;
```
