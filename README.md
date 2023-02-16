# Conduit Connector Airtable

![Logo](https://1000logos.net/wp-content/uploads/2022/05/Airtable-Logo.png)

## Overview

The Airtable Connector is one of [Conduit](https://github.com/ConduitIO/conduit) standalone plugins. It provides 
**only** a `Source` connector.

The Source Connector provides 2 modes:
- Snapshot
- CDC

Due to limitations in the Airtable API, manual configuration of an Airtable base is required by the user if CDC 
functionality is desired.
Steps to help with the configuration are provided under _CDC_ in _Source Configuration_.

**Deleted records** can not be detected. 

The Connector uses a [Airtable Golang Package](https://github.com/mehanizm/airtable) to access the Airtable API.

## How to build it

Run `make build`.

## Source Configuration

This section describes the technical details of how the connector works and walks the user through any manual
configurations required.

### Configuration Options

| parameter    | description                                                                                                                                                                                                                   | required | example                |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------------------|
| `api_key`    | API Key to connect to the Airtable Base. Can be a read-only key. <br/>[Finding/Generating API Keys](https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens#:~:text=Find/generate%20your%20API%20key) | **true** | `keyXXXXXXXXXXXXXX`    |
| `base_id`    | ID of the base to be read.<br/> [Finding Airtable Base IDs](https://support.airtable.com/docs/finding-airtable-ids)                                                                                                           | **true** | `appXXXXXXXXXXXXXX`    |
| `table_id`   | ID of the table to be read.<br/> [Finding Airtable Table IDs](https://support.airtable.com/docs/finding-airtable-ids)                                                                                                         | **true** | `tblXXXXXXXXXXXXXX`    |
| `enable_cdc` | An option to enable CDC mode for the connector. If `true`, the connector will detect changes. If `false`, only a snapshot will be taken.                                                                                      | **true** | `true`,`false`,`t`,`f` |                                       |

### CDC

In order for CDC to work, 2 new fields must be added to all records in an Airtable Table. An option to _hide_ these created fields from view is available.

#### Field 1 - "last-modified"
1. [Add a new field](https://support.airtable.com/docs/adding-a-field) to any record in the table you would like to work with.
2. Rename the newly created field from `"Field X"` to `"last-modified"`.
3. Select the field type as `Last modified time`.
4. Press `Save`.
5. **(Optional)** If you would not like the field to be visible, right-click on the field `last-modified` and select `Hide field`.

#### Field 2 - "last-modified-str"
1. [Add a new field](https://support.airtable.com/docs/adding-a-field) to any record in the table you would like to work with.
2. Rename the newly created field from `"Field X"` to `"last-modified-str"`.
3. Select the field type as `Formula`.
4. Copy and paste the following formula: `DATETIME_FORMAT(LAST_MODIFIED_TIME(),'D/MM/YYYY HH:mm:ss')`.
5. Press `Save`.
6. **(Optional)** If you would not like the field to be visible, right-click on the field `last-modified-str` and select `Hide field`.

The addition of 2 fields is required as the Airtable API does not provide a way to fetch "the latest change". `last-modified` is a computed field that automatically updates itself when a modification is made to that record specifically. The limitation is that `last-modified` does not return any information about the `seconds` value, so the `last-modified-str` reformats `last-modified` into a string with a `seconds` value that can be interpreted by the connector.  