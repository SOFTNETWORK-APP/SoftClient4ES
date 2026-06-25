# SRE cross-cluster incident triage in one SQL query

![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo_375x300.png)

*R1 blog series — Post 3 of 4 (week 2). Previously: "The JOIN matrix, explained: rows 1, 2, 3." Next: "How teams are using R1."*

<!-- Channel B (Medium / personal LinkedIn). Hero image staged in elasticsql/linkedin/ at publish time. -->

---

## 2 a.m., three regions, one incident

Your platform runs Elasticsearch per region — `prod_us`, `prod_eu`, `prod_fr`. Latency spikes in EU; checkout errors climb in the US. Are they the same incident? With a cluster per region, the honest answer is usually "open three dashboards and eyeball the timestamps."

That is the wedge R1 closes. With the federation server in front of your regional clusters, you JOIN across them in a single query and read the correlation straight across — no per-region tab-switching, no exporting to a third system first.

---

## One query instead of N dashboards

The catalog prefix routes each leg to its regional cluster. The shape below is illustrative — **substitute your own per-region index names**; the mechanics are exactly the verified cross-cluster JOIN:

```sql
-- Illustrative: per-region index names are yours to choose.
SELECT u.user_id, u.action, e.error_code, e.region
FROM `prod_us`.user_events u
JOIN `prod_eu`.error_events e
  ON u.user_id = e.user_id
WHERE e.ts > NOW() - INTERVAL '15' MINUTE
ORDER BY e.ts;
```

The narrative — correlate logs, metrics, and traces across regions — is the use case. The engine underneath is the same Row 3 multi-source coordinator from last week's post: each leg scanned independently, joined coordinator-local. No data leaves a region except the rows the JOIN actually needs.

---

## What it takes

Cross-cluster correlation runs on the federation server, which is Pro+ (it spans two or more clusters, and the cluster meter is the paywall). A single-cluster team gets the same JOIN mechanics for free within one cluster — you only need federation once an incident genuinely crosses cluster boundaries.

No perf claims here on purpose: the quantified benchmark is R1.1's beat, not this one. The point of this post is *expressiveness* — one query where there used to be a manual cross-reference.

---

## Where to go next

- **See what Pro+ includes and why federation sits there:** [editions & pricing](https://softclient4es.dev/licensing/) (LIVE).
- **Run the single-cluster JOIN first, for free:** the [JDBC quickstart](https://softclient4es.dev/integrations/jdbc/) (LIVE).
<!-- pending 17.2: /integrations/federation-helm/ (federation operator guide — web PR #8, base release-r1) -->
- **Deploy the federation server:** the federation operator guide — `https://softclient4es.dev/integrations/federation-helm/` (publishing with R1).

Next week: how teams are putting R1 to work.

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/
