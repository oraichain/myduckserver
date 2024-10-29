# MyDuckServer MySQL Replica Setup

This guide walks you through creating a MySQL replica using MyDuckServer. Follow the steps outlined here to seamlessly integrate MyDuckServer as a replica of an existing MySQL instance.

## Prerequisites

Before you begin, ensure that:

- **MyDuckServer** is installed and running on your server.
- You have the necessary **MySQL credentials** (host, port, user, password) to set up replication.

## Getting Started

To create a MySQL replica in MyDuckServer, run the provided `replica_setup.sh` script. You will need to supply the MySQL instance connection details as parameters.

### Usage

```bash
bash replica_setup.sh --mysql_host <mysql_host> --mysql_port <mysql_port> --mysql_user <mysql_user> --mysql_password <mysql_password>
```

### Parameters

- **`--mysql_host`**: The hostname or IP address of the MySQL instance.
- **`--mysql_port`**: The port on which the MySQL instance is running.
- **`--mysql_user`**: The MySQL user that has the appropriate privileges for replication.
- **`--mysql_password`**: The password for the provided MySQL user.

## Example

```bash
bash replica_setup.sh --mysql_host 192.168.1.100 --mysql_port 3306 --mysql_user root --mysql_password mypassword
```

This command sets up MyDuckServer as a replica of the MySQL instance running at `192.168.1.100` on port `3306` with the user `root` and password `mypassword`.