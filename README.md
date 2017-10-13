# DataWatch

Features

- Runs a test query, recording failure/success, error messaging, query time, and HTTP status code (if applicable).

- Compares row counts between source and destination tables.

- For append only tables, detects of the latest row count is less then the last row count.

- Sends alerts on failures via [Slack](https://slack.com/)

### Usage

```sh
$ datawatch
Usage: datawatch [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  init-db   Initialize the DataWatch database
  run-all   Run all tests in a config file
  run-test  Run a specific test by test name in a config file
```

Run all tests in an config file:

```
Usage: datawatch run-all [OPTIONS] CONFIG_PATH

  Run all tests in a config file

Options:
  --sql-alchemy-connection TEXT
  --alerts / --no-alerts
  --help                         Show this message and exit.
```

```sh
datawatch run-all example_config.yml
```

Run a specific test in a config file, failures will exit non-zero.

```sh
Usage: datawatch run-test [OPTIONS] TEST_NAME CONFIG_PATH

  Run a specific test by test name

Options:
  --sql-alchemy-connection TEXT
  --alerts / --no-alerts
  --help                         Show this message and exit.
```

```
run-test carto_public_cases_fc example_config.yml
```

### Configration File

```yaml
connections:
  carto_public: $CARTO_PUBLIC_CONN_STRING
  carto_private: $CARTO_CONN_STRING
  geodb2_public: $GEODB2_PUBLIC_CONN_STRING
alerts:
  default: slack_warn
  slack_critical:
    type: slack
    at_channel: true
    slack_url: $SLACK_TEST_CHANNEL
  slack_warn:
    type: slack
    slack_url: $SLACK_TEST_CHANNEL
table_tests:
  - test_name: carto_public_cases_fc
    append_only: true
    connection: carto_public
    table: phl.public_cases_fc
    test_query: select * from phl.public_cases_fc where status_notes = 'Issue Resolved' limit 100
    source_connection: geodb2_public
    source_table: GIS_311.PUBLIC_CASES_FC
    alerts:
      - slack_critical
```

#### Connections

The connections section of a config file contains a set of named connections strings. Environment variables can be used by using the dollar sign ($) in front of the variable name.

```yaml
connections:
  carto_public: $CARTO_PUBLIC_CONN_STRING
  carto_private: $CARTO_CONN_STRING
  geodb2_public: $GEODB2_PUBLIC_CONN_STRING
  xyz_postgres: postgresql://localhost/mydb
```

#### Alerts

The alerts section configures alerts. Each key/value pair represents an "alerter" something that can alert. For Slack, it's a channel and other settings, like whether to use @channel in the message. Each alerter is named and can be referred to by a data test. You can also set the default, using the alerter key. Environment variables can be used by using the dollar sign ($) in front of the variable name.

```yaml
alerts:
  default: slack_warn
  slack_critical:
    type: slack
    at_channel: true
    slack_url: $SLACK_TEST_CHANNEL
  slack_warn:
    type: slack
    slack_url: $SLACK_TEST_CHANNEL
```

#### Table Tests

The table test section configures tests against tablular or table-like datasets.

```yaml
table_tests:
  - test_name: carto_public_cases_fc
    append_only: true
    connection: carto_public
    table: phl.public_cases_fc
    test_query: select * from phl.public_cases_fc where status_notes = 'Issue Resolved' limit 100
    source_connection: geodb2_public
    source_table: GIS_311.PUBLIC_CASES_FC
    alerts:
      - slack_critical
```

Options

| Key | Type | Description |
| --- | ---- | ----------- |
| test_name | string | The name of the test, this is how it is referred to in the database and alerts. Should only contain letters, number, hyphens, and underscores. |
| append_only | boolean | Is the source table append only? |
| connection | string | The connection of the test target, by key from the connections section |
| table | string | The table name (schema/account/catalog included, if necessary) of the test target table |
| test_query | string | A query, usually SQL, to run against the test target |
| source_connection | string | The connection of the source table, by key from the connections section |
| source_table | string | The source table name (schema/account/catalog included, if necessary) |
| alerts | array[string] | Alerters, from the alerts section by key, to send alerts to in the event of a failure. |