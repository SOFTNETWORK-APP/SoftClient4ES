# How teams are using R1

![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo_375x300.png)

*R1 blog series — Post 4 of 4 (week 3). Previously: "SRE cross-cluster incident triage in one SQL query."*

<!-- Channel B (Medium / personal LinkedIn). Hero image staged in elasticsql/linkedin/ at publish time. -->
<!-- P6: vignettes ship anonymized by default. Replace with a named reference ONLY if the lead confirms one in writing before publish. -->

---

## Patterns, not logos

A few weeks in, the usage patterns are clear enough to share. These are anonymized vignettes drawn from opt-in telemetry signals and early conversations — the shapes are real, the names are not. (If a team agrees to be named, we will say so explicitly.)

---

### A team running analytics off live Elasticsearch

A product-analytics team had two indices — events and accounts — and a standing weekly report that JOINed them. The old workflow exported both to a warehouse on a schedule just to run that one JOIN. With R1 they point Superset at the JDBC driver and run the JOIN against the live cluster. The warehouse hop is gone; the report is current instead of a day old.

The free Community tier covers it: one JOIN, single cluster, well under the result meter.

### A team correlating across two regions

A platform team runs Elasticsearch in two regions and kept hitting the "is this one incident or two?" wall during triage. They deployed the federation server and now JOIN the two regional clusters in a single query when an incident looks cross-regional. Two clusters puts them on Pro; the value is the minutes saved per incident, not the licence line.

### A team replacing a denormalization pipeline

A data team had been denormalizing at index time — duplicating customer fields onto every order document — purely so they could "JOIN" later. With cross-index JOIN they stopped duplicating and let the query do the work. Smaller indices, no re-index when a customer attribute changes.

---

## The common thread

Every one of these started the same way: a JOIN that Elasticsearch could not do, worked around with ETL or denormalization. R1 removed the workaround. The teams that adopt fastest are the ones who already had the JOIN in their head and just needed somewhere to type it.

---

## Where to go next

- **See which tier fits your shape:** [editions & pricing](https://softclient4es.dev/licensing/).
- **What we collect, and how to opt out:** [privacy & telemetry](https://softclient4es.dev/privacy/telemetry/).

Want to be a named reference? Reach out — we would rather show your real numbers than an anonymized sketch.

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/
