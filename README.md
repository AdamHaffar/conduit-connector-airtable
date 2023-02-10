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
Steps to help with the configuration are provided under _Source Configuration_.

**Deleted records** can not be detected. 

The Connector uses a [Airtable Golang Package](https://github.com/mehanizm/airtable) to access the Airtable API.

## How to build it

Run `make build`.

## Source Configuration

This section describes the technical details of how the connector works and walks the user through any manual
configurations required.

### Authentication

To connect to Airtable, the user must provide the following:
- `APIKey` - [Finding/Generating API Keys](https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens#:~:text=Find/generate%20your%20API%20key)
- `BaseID` - [Finding Airtable IDs](https://support.airtable.com/docs/finding-airtable-ids)
- `TableID` - [Finding Airtable IDs](https://support.airtable.com/docs/finding-airtable-ids)

