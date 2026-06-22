# Trial → Paid Conversion Funnel (internal operator runbook)

> **Internal operations doc.** This is the core-project mirror of the license-server operator runbook
> `docs/ops/runbooks/trial-conversion.md` (Story 15.4), kept in sync per the dual-doc convention. It
> describes how the SoftClient4ES **license server** measures the trial → paid conversion funnel for
> the R1 launch. It is NOT a customer-facing privacy page (that is the separate Telemetry & Privacy
> page) — the funnel stores **aggregate counts only**, never any per-customer row.

## What the funnel measures

| Leg | Meaning |
|-----|---------|
| started | trials ever started |
| activated | trials that reached an active state (activation leg) |
| converted | trials that became a paid license (counted once per organisation) |
| expired | trials that expired without converting |

- **Conversion rate** = converted / started; **activation rate** = activated / started; **churn proxy**
  = expired / started.
- The legs are read from the license server's durable JDBC projection (the restart-stable
  system-of-record), not from Prometheus counters (which reset to 0 on every pod restart and are kept
  only 15 days).

## The `r1_funnel_snapshot` table (and why it exists)

Prometheus retention is 15 days, far shorter than the 6-month R1 evaluation window, so a Prometheus-only
trend would lose history. A daily job persists **one aggregate row per UTC day** into the
`r1_funnel_snapshot` table:

| Column | Type | Notes |
|--------|------|-------|
| `snapshot_date` | `DATE` (PK) | the run's UTC date — a same-day re-run UPSERTs (no duplicate) |
| `captured_at` | `TIMESTAMPTZ` | UTC instant the job ran (stable week bucketing) |
| `trials_started` / `trials_activated` / `trials_converted` / `trials_expired` | `BIGINT` | cumulative legs |
| `subscriptions_active_total` | `BIGINT` | point-in-time active-subscriptions total |
| `subscriptions_active_by_tier` | `TEXT` | JSON `{"pro": 12, ...}` tier → active count |

**Privacy:** the schema has no `organization_id`, `license_id`, customer, email, or user column — the
aggregate-only guarantee is enforced by the schema, and a test asserts it.

The daily job is **best-effort and data-integrity guarded**: if any leg read fails it SKIPS the write
(it never persists a bogus all-zero authoritative row that would corrupt the trend); the next daily run
self-heals. It runs at 06:00 UTC, staggered one hour after the driver-download poll so the two heavy
daily database reads do not collide.

## Dashboards and alerts

- **Grafana dashboard** `SoftClient4ES — Trial → Paid Funnel`: live per-week conversion / activation /
  churn rate stat panels (from Prometheus counters) plus a snapshot-backed long-term trend over the
  full R1 window (read from `r1_funnel_snapshot`, because Prometheus retention < the R1 window).
- **Alert** `TrialConversionWoWDrop` (info): fires when the 7-day conversion rate drops more than 20%
  week-over-week, gated on absolute floors (prior-week conversions > 10 AND prior-week starts > 20) so
  it cannot trip on small launch-week swings. It is additive to the existing `TrialConversionDrop`
  alert ("started but none converted") — the two catch different failures.

## Operator checks (license-server pod)

- Did today's snapshot run? `SELECT * FROM r1_funnel_snapshot ORDER BY snapshot_date DESC LIMIT 7;`
- Is the cron registered? At boot the licensing pod logs `funnel-snapshot CronTab registered`.
- Was a snapshot skipped (a leg read failed)? grep the licensing pod log for `funnel snapshot SKIPPED`.

For the full triage workflow (rule out a broken checkout before treating a drop as a funnel/product
issue), see the license-server runbook `docs/ops/runbooks/trial-conversion.md`.
