# Internal-ops dashboards — R1 success criteria

> **Internal-ops note (not customer-facing).** This page is the core-project mirror of the
> license-server back-office Grafana dashboards, kept in sync per the dual-doc convention. It points at
> the operator dashboards that live in the
> [`softclient4es-license-server`](https://github.com/SOFTNETWORK-APP/softclient4es-license-server)
> repo (`docs/ops/grafana/`). Grafana has **no public Ingress** — these dashboards are reachable only
> internally (e.g. `kubectl port-forward`). The customer-facing privacy page is
> [Telemetry & Privacy](telemetry.md); this note covers the *operator* view of the same aggregate data.

## R1 Success Criteria dashboard (Story 15.5)

`softclient4es-r1-success-criteria` (committed at
`softclient4es-license-server/docs/ops/grafana/softclient4es-r1-success-criteria.json`) is the single
**"did R1 succeed?"** readout. It does not re-instrument anything — it re-queries the durable Postgres
tables the telemetry stories populate (`download_events`, `instance_ping`, `r1_funnel_snapshot`) plus a
small operator-entered `r1_sales_signals` table. Every panel reads the Postgres datasource because each
R1 evaluation window (download 30-day rolling, paid-signups 90d, case studies 180d, funnel 6-month)
exceeds the 15-day Prometheus retention, so the durable tables are the system-of-record for the R1 trend.

### "R1 succeeded" definition

R1 succeeded when **all three top-row thresholds are green within their windows**:

| Threshold | Target | Window | Source |
|---|---|---|---|
| Driver downloads / week | **≥ 500** | trailing 7 days | `download_events.count_delta` (SUM) |
| Paid customer signups | **≥ 5** | trailing 90 days | 90-day **delta** of cumulative `r1_funnel_snapshot.trials_converted` |
| Case studies / reference customers | **≥ 3** | trailing 180 days | `r1_sales_signals` where `kind = 'case_study'` |

The adoption-breadth (downloads by source; active instances per day by product / by tier) and usage-depth
(daily JOIN total; weekly conversion + churn) panels are **context for R2 prioritisation**, not R1
pass/fail gates. Paid signups is a **90-day flow** (the trailing-90-day delta of the cumulative
`trials_converted` counter, floored at 0) — not a point-in-time active-subscriptions gauge.

### Privacy — aggregates only

No panel exposes a per-customer row or a per-instance value. The per-product / per-tier "active instances"
panels compute `COUNT(DISTINCT instance_id)` with `instance_id` appearing **only inside the aggregate** —
never in a `SELECT` list and never a `GROUP BY` key that reaches a legend. No `organization_id`,
`license_id`, or `email` column is ever read. A build-time privacy gate fails the dashboard if any panel
query leaks a bare `instance_id` projection or a forbidden PII column.

### Operating it

- **Provisioning** is automatic: the dashboard JSON is embedded into a ConfigMap by the
  `k3s-monitoring` ansible role and loaded by the Grafana sidecar into the `SoftClient4ES` folder.
- **Qualitative `r1_sales_signals` table — one-off apply (mandated).** The table has no automatic Flyway
  owner, so the operator applies it once against the licensing Postgres (idempotent):

  ```bash
  psql "$LICENSING_DATABASE_URL" -f backend/licensing/src/main/resources/db/migration/r1_sales_signals/V1__create_r1_sales_signals.sql
  ```

  Then rows are entered via `psql` INSERT or a CSV `\copy` import (the project lead keeps the human
  entry surface in a Google Sheet and imports weekly). Enter **no customer names** — this is an
  aggregate signal.
- Panels whose source table is not yet deployed ship inside collapsed *"Pending — awaiting Story 15.x"*
  rows so the dashboard never errors on load; they light up the moment their table lands.

See the dashboard README (`softclient4es-license-server/docs/ops/grafana/README.md`) for the full
threshold/privacy/populate detail.

[Back to index](README.md)
