# PMS Tooling

## Table of Contents
- [PMS Tooling](#pms-tooling)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [db\_gen.go](#db_gengo)
    - [Installation](#installation)
    - [Configuration](#configuration)
    - [Usage](#usage)
    - [Command-Line Flags](#command-line-flags)
  - [pms\_gen.go](#pms_gengo)
    - [Installation](#installation-1)
    - [Usage](#usage-1)
    - [Output](#output)
  - [Common Errors and Troubleshooting](#common-errors-and-troubleshooting)

## Introduction
This repository contains two CLI tools written in Go for integrating and processing data between Firestore and SQLite. The first tool (`db_gen.go`) is responsible for watching Firestore for new changes and storing them in an SQLite database. The second tool (`pms_gen.go`) fetches, processes, and transforms data from the SQLite database and writes the transformed data to JSON files.

## db_gen.go

### Installation
To install the dependencies and build the `db_gen` CLI tool, follow these steps:

1. **Install Dependencies**
   ```sh
   go get -u cloud.google.com/go/firestore
   go get -u github.com/mattn/go-sqlite3
   go get -u github.com/spf13/viper
   go get -u google.golang.org/api/iterator
   go get -u google.golang.org/api/option
   ```

2. **Build the CLI**
   ```sh
   go build -o db_gen db_gen.go
   ```

### Configuration
The `db_gen` CLI tool requires a configuration file named `.zuubrc` in your home directory. This file should be in YAML format and contain the following keys:

```yaml
environment: "STAGE" # or "PROD"
interval_ms: 5000
```

Additionally, set the following environment variables:
- `GCP_STAGE_CREDS`: Path to the Firestore credentials file for the staging environment.
- `GCP_PROD_CREDS`: Path to the Firestore credentials file for the production environment.

### Usage
Run the `db_gen` CLI tool as follows:
```sh
./db_gen --debug=false
```

This tool performs the following tasks:
- Loads configuration from `.zuubrc`.
- Connects to Firestore and SQLite.
- Watches Firestore for new changes and stores them in SQLite.
- Processes Firestore documents and tracks acknowledged documents.

### Command-Line Flags
- `--debug`: Enables debug mode to log raw Firestore documents. Default is `false`.

## pms_gen.go

### Installation
To install the dependencies and build the `pms_gen` CLI tool, follow these steps:

1. **Install Dependencies**
   ```sh
   go get -u github.com/google/uuid
   go get -u github.com/mattn/go-sqlite3
   ```

2. **Build the CLI**
   ```sh
   go build -o pms_gen pms_gen.go
   ```

### Usage
Run the `pms_gen` CLI tool as follows:
```sh
./pms_gen
```

This tool performs the following tasks:
- Connects to the SQLite database.
- Fetches and processes data for a list of bot IDs.
- Transforms the fetched data and writes it to JSON files.

### Output
The transformed data is written to the `data` directory in JSON files named after each bot ID.

## Common Errors and Troubleshooting
- **Error reading config file**: Ensure the `.zuubrc` file exists in your home directory and is correctly formatted.
- **Failed to create Firestore client**: Verify that the credentials file path is correct and the file is accessible.
- **Failed to connect to SQLite**: Ensure the SQLite database file path is correct and the file is accessible.
- **Error fetching data for botid**: Check the bot ID and ensure that there are records in the SQLite database with the specified bot ID.

If you encounter any other issues, please refer to the logs for detailed error messages and stack traces.
