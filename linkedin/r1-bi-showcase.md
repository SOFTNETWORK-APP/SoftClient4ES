# LinkedIn — R1 BI-Tool Showcase

> **Angle** : Intégration BI — tested vs compatible. Connecter ses outils existants à Elasticsearch via JDBC / Arrow Flight SQL, cadrage honnête.
> **Cible** : Data analysts, BI engineers, équipes data platform
> **Demo** : `docker compose --profile superset-flight up`
> **Timing** : J+14 — deux semaines après le lancement

---

🔌 Your BI stack already speaks SQL. Now Elasticsearch does too.

R1 turns Elasticsearch into a SQL source your existing tools can query — with cross-index JOINs they could never do before. Here's where each tool stands, honestly:

✅ Tested — we ran them through verification:
• Apache Superset (dedicated dialect)
• DBeaver
• DataGrip
• Grafana (via Arrow Flight SQL)

🔄 Compatible — the protocol works, formal regression is best-effort:
• Tableau
• Power BI
• Metabase
• DbVisualizer

We don't upgrade a tool's tier to sound better. "Tested" means we tested it. "Compatible" means it should work and we'll tell you what to watch for.

A note for the Power BI folks: use Import mode with explicit JOIN SQL. (No, there is no magic "default driver" — you write the JOIN, the driver runs it against ES.)

Honest gap, on purpose: explicit JOIN SQL works today. Full subquery and CTE support arrives in R2a. Every compatible-tier page says so up front, so you're never surprised mid-dashboard.

Connect your tool 👉 https://softclient4es.dev/integrations/jdbc/
Tested integrations 👉 https://softclient4es.dev/integrations/superset/ · https://softclient4es.dev/integrations/dbeaver/ · https://softclient4es.dev/integrations/grafana/

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/

#Elasticsearch #BusinessIntelligence #Tableau #PowerBI #Metabase #SQL #DataAnalytics
