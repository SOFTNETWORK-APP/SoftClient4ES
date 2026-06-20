# Telemetry & Privacy

SoftClient4ES measures **driver adoption** with anonymous, aggregate counts only. This page describes
the download analytics specifically; a complete, surface-by-surface telemetry overview will follow.

> This is the core-project mirror of the website page
> [Telemetry & Privacy](https://softclient4es.com/privacy/telemetry/) — kept in sync per the dual-doc
> convention.

## What is collected

When you click a **Download** button on the website's installation page, the site sends one small
beacon to a public endpoint with exactly these fields:

| Field         | Example  | Meaning                                       |
|---------------|----------|-----------------------------------------------|
| `source`      | `portal` | Where the count came from (the docs button)   |
| `driver`      | `jdbc`   | Which driver family (`jdbc` or `adbc`)        |
| `version`     | `0.1.4`  | The published artifact version                |
| `count_delta` | `1`      | One download                                  |

A timestamp is added on the server. That is the **entire** record.

## What is NOT collected

- **No IP address.** The endpoint never reads the remote address or any `X-Forwarded-For` / `Forwarded`
  header, and never stores one. The privacy guarantee is enforced by the database schema itself — the
  table has no IP, user-agent, account, or instance column.
- **No user agent, no cookies, no fingerprint.**
- **No account or identity.** The endpoint requires no login and no API key; nothing links a download
  to a person or organisation.

## How it is used

The aggregate counts power an internal adoption dashboard (downloads per day/week, broken down by
driver, version, and source). Downloads are also counted independently from the public artifact
registry (JFrog Artifactory), so the two figures cross-check each other.

The download beacon is **fire-and-forget**: it is sent asynchronously and never blocks, delays, or
fails your download. If it cannot be sent, your download still proceeds normally.

## Self-hosting

These analytics describe the **website's** download buttons only. The SoftClient4ES client libraries,
JDBC/ADBC drivers, REPL, and Arrow Flight SQL server you run in your own infrastructure do **not** phone
home for download tracking.
