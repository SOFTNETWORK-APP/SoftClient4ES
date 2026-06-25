# R1 Editorial Calendar — Blog + LinkedIn

Planning note for the R1 launch marketing waves. J+0 = R1 launch day. Blog posts are
Channel B (Medium / personal LinkedIn first; company page reshares). LinkedIn drafts live
in `elasticsql/linkedin/`. Nothing here is auto-published — each beat is posted manually
by the project lead.

## Cadence

| When | Blog post (`elasticsql/blogs/`) | LinkedIn draft (`elasticsql/linkedin/`) |
|---|---|---|
| **J+0** launch day | `announcing-r1-cross-cluster-join.md` | `r1-launch-announcement.md` |
| **J+7** week 1 | `join-matrix-walkthrough.md` | `r1-demo-teaser.md` |
| **J+14** week 2 | `sre-cross-cluster-incident-triage.md` | `r1-bi-showcase.md` |
| **J+21** week 3 | `how-customers-use-r1.md` | `r1-federation-reveal.md` |
| **J+28** week 4 | — *(Epic 18 owns this beat)* | — *(Epic 18: R1.1 Arrow zero-copy benchmark)* |

Publish on the **personal LinkedIn account first** (far higher reach than the company page),
then reshare from the company page (`linkedin.com/company/softnetwork-app`).

## Timing guards (hard, not suggestions)

### Epic-18 non-overlap guard
- The R1.1 Arrow Flight SQL **zero-copy benchmark** is Epic 18's beat, scheduled for **week 4 (J+28)**.
- The 17.8 marketing waves **END at week 3** (federation reveal). They do NOT spill into week 4.
- **The week-3 federation reveal carries NO performance claim** — no "faster", no Nx, no ms,
  no rows/sec. Cross-cluster is sold on *expressiveness* (one query instead of N dashboards),
  not speed. The speed story is reserved for R1.1 so the benchmark lands with full impact.

### R2a 90-day quiet-window hold
- **HOLD: J+0 → J+90 — no R2a teasers.** No subqueries, no CTEs, no "coming soon: WITH",
  no set-op (UNION-dedup / INTERSECT / EXCEPT) previews on any marketing surface for the
  first 90 days post-R1. This protects the R1 launch window from being diluted.
- Honest *limitation* framing (e.g. "explicit JOIN works today; subqueries land in R2a")
  is allowed and encouraged on docs pages — that is expectation-setting, not a teaser.
  The hold is on *promotional* R2a content, not on honest gap disclosure.
- **Policing owner: the project lead / founder marketer.** If launch momentum tempts an
  early R2a reveal, the lead arbitrates and the default is "wait." Same owner decides
  whether Blog Post 4 names a real customer (default: ships anonymized — see P6).

## Notes

- LinkedIn account credentials / scheduling access: open question (epic OQ#3) — drafts are
  committed regardless, so they can be posted manually at each beat even if no scheduler exists.
- Hero images for the blog posts and the LinkedIn carousel are staged in `elasticsql/linkedin/`
  (`softclient4es-carousel.pdf`, `web1.png`..`web5.png`) — reuse or refresh per beat.
