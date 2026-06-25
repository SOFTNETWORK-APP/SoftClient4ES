# SoftClient4ES Federation — Operator Guide

> **Canonical source:** the authoritative version of this guide ships with the federation Helm chart, at `softclient4es-federation/docs/operator-guide.md`. This page presents the same content.

The SoftClient4ES federation Helm chart deploys a **cross-cluster Arrow Flight SQL coordinator** (the *federation server*) plus one **per-ES-version sidecar** for each Elasticsearch cluster you want to federate. One `values.yaml` + `helm install` replaces 30+ minutes of hand-written HOCON, Kubernetes manifests, Secrets, and probe wiring.

> **See also:** the federation server is the query-time coordinator behind cross-cluster JOINs. For the JOIN semantics it serves — single-index, cross-index, and cross-cluster — see the [Cross-Index JOIN matrix](../sql/joins.md).

## 1. Prerequisites

- A Kubernetes cluster (v1.27+ recommended for native gRPC readiness probes; on **older clusters set `federation.probes.useGrpc: false`** for TCP readiness — no extra binary needed; see §9/§10).
- **Helm 3**.
- One or more Elasticsearch clusters **reachable from the K8s cluster** (each sidecar opens a connection to exactly one ES cluster of its compiled-against major version).
- A container runtime able to pull from **public DockerHub** (`docker.io/softnetwork/...`). For rate-limit avoidance on first install, configure `imagePullSecrets` with an authenticated DockerHub account.
- (Multi-cluster only) a **Pro or Enterprise license** — see §7. Single-cluster federation runs free on the Community tier.

> **Image availability:** the federation image `docker.io/softnetwork/softclient4es-federation` is published to public DockerHub at this release. The four sidecar images `docker.io/softnetwork/softclient4es{6,7,8,9}-arrow-flight-sql` are already published. Always pin `image.tag` to the published release tag in your `values.yaml`.

**Working directory convention.** Every `cp examples/…` and `helm install … ./softclient4es-federation` command in this guide is run **from the chart directory `softclient4es-federation/`** (where `Chart.yaml`, `examples/`, and the `./softclient4es-federation` chart path resolve). `cd` there first:

```bash
cd softclient4es-federation/          # all commands below run from here
```

The release name you pass to `helm install <release> …` (this guide uses `fed`) becomes part of the in-cluster Service DNS — `<release>-softclient4es-federation.<namespace>.svc.cluster.local`. Keep it consistent across `install`, `upgrade`, `test`, and `rollback`.

## 2. The 5-minute path — single cluster

```bash
# 1. Copy the starter example
cp examples/single-cluster/values.yaml my-values.yaml

# 2. Edit my-values.yaml — set the one sidecar's ES coordinates + version:
#    sidecars[0].elasticsearchVersion: 8
#    sidecars[0].elasticsearch.url:    https://es.mycorp.internal:9200
#    sidecars[0].elasticsearch.credentialsSecretName: my-es-secret   # see §6
#    image.tag: <published-release-tag>

# 3. Create your ES credentials Secret (raw example; §6 for SealedSecrets/ESO/Vault)
kubectl create secret generic my-es-secret \
  --from-literal=es-auth-method=basic \
  --from-literal=es-username=elastic \
  --from-literal=es-password='<password>'

# 4. Install
helm install fed ./softclient4es-federation -f my-values.yaml

# 5. Validate
helm test fed          # runs GetCatalogs; passes when the federation lists 1 catalog
```

Single-cluster is **license-free** (Community `maxClusters=1`). The federation exposes Flight SQL on `ClusterIP` port **32020**; reach it in-cluster at `fed-softclient4es-federation.<namespace>.svc.cluster.local:32020`, or expose it via an Ingress (§6, TLS). See the chart's `examples/single-cluster/README.md` for the full single-cluster narrative + topology diagram.

## 3. Multi-cluster — three regions, mixed ES versions

```bash
cp examples/three-region/values.yaml my-values.yaml
# Per-region sidecars (us-east-1 ES8 default · eu-west-1 ES8 · ap-south-1 ES9),
# each with its own elasticsearch.credentialsSecretName + auth.credentialsSecretName.

# 1. Create the Pro/Enterprise license Secret (REQUIRED — 3 clusters > Community's quota of 1; see §7).
#    Replace <PRO_OR_ENTERPRISE_JWT> with the JWT from the portal.
kubectl create secret generic sc4es-pro-license \
  --from-literal=license-key='<PRO_OR_ENTERPRISE_JWT>'

# 2. Create the per-region ES + sidecar-auth Secrets the example references
#    (one ES Secret + one sidecar-auth Secret per region; see §6 for SealedSecrets/ESO/Vault).
#    Replace every <…> with your real coordinates.
for r in us-east-1 eu-west-1 ap-south-1; do
  kubectl create secret generic sc4es-es-$r \
    --from-literal=es-auth-method=basic \
    --from-literal=es-username='<es-user>' \
    --from-literal=es-password='<es-password>'
  kubectl create secret generic sc4es-arrow-$r \
    --from-literal=arrow-bearer-token='<sidecar-bearer-token>'
done

# 3. Install (REQUIRES the Pro/Enterprise license Secret above — without it the federation CrashLoops, see §7/§10).
helm install fed ./softclient4es-federation -f my-values.yaml
helm test fed          # GetCatalogs → 3 catalogs
```

> The example's `values.yaml` references these Secrets by name (`license.secretName: sc4es-pro-license`, each region's `elasticsearch.credentialsSecretName: sc4es-es-<region>` + `auth.credentialsSecretName: sc4es-arrow-<region>`). **Create them BEFORE `helm install`** — a missing Secret is the #1 first-install failure (a loud, self-healing CrashLoop; §6/§10). The exact key names (`license-key`, `es-auth-method`/`es-username`/`es-password`, `arrow-bearer-token`) are the chart's expected Secret data keys (§6). See the chart's `examples/three-region/README.md` for the full per-region narrative (GDPR data-residency, SRE incident-triage, mixed-version migration).

Each `sidecars[]` entry deploys one Deployment + one Service and registers one `arrow.flight.federation.servers.<name>` entry (host = `<name>.<namespace>.svc.cluster.local:32010`). Mixed ES versions are first-class — the chart selects `softclient4es{6,7,8,9}-arrow-flight-sql` per `elasticsearchVersion`. **Adding a cluster** = add a `sidecars[]` entry + `helm upgrade`; **removing** = delete the entry + `helm upgrade`.

> ⚠️ **All-or-nothing readiness (SPOF).** With the default `federation.probes.useGrpc: true`, the federation is Ready only if **every** sidecar's backing ES is reachable — one down region pulls the whole federation out of its Service. For partial availability set `federation.probes.useGrpc: false` (TCP readiness on 32020); the federation then stays Ready and fails only the queries that touch the down region. See §9.

## 4. The path that is ready for the upcoming release

```bash
cp examples/heterogeneous-ready/values.yaml my-values.yaml
```

This is the three-region topology **plus commented-out `duckdb-attach` placeholders** (PostgreSQL / MySQL / Snowflake) that show how heterogeneous sources slot into the same `servers` map once heterogeneous-source support ships in the upcoming release (Quarter 1 2027). **In this release they stay commented** — `GetCatalogs` returns 3, not 6. This release has no `values.yaml` key for `duckdb-attach` (the `servers` map is ConfigMap-rendered from `sidecars[]` Flight SQL servers only). **Uncommenting them later counts toward the license quota** (the cluster count includes attach servers) — 3 ES + 3 attach = 6 > Pro's 5 → Enterprise. See the chart's `examples/heterogeneous-ready/README.md` for the preview narrative.

## 5. Configuration reference

> `values.yaml` field → HOCON path / env var. **Verify against the chart's own `values.yaml` and README; this table is the authoritative mapping.**

**Federation** (image `softclient4es-federation`; HOCON root `arrow.flight.federation.*`):

| `values.yaml` | HOCON | Env var | Default |
|---|---|---|---|
| `replicaCount` | — | — | 1 |
| `image.repository` / `image.tag` / `image.pullPolicy` | — | — | `docker.io/softnetwork/softclient4es-federation` / `appVersion` / `IfNotPresent` |
| `federation.maxMemory` | `max-memory` | `FEDERATION_MAX_MEMORY` | `512m` |
| `federation.queryTimeoutSeconds` | `query-timeout-seconds` | `FEDERATION_QUERY_TIMEOUT` | 30 |
| `federation.health.port` | `health.port` | `FEDERATION_HEALTH_PORT` | 32021 |
| `federation.health.probeTimeoutSeconds` | `health.probe-timeout-seconds` | `FEDERATION_HEALTH_PROBE_TIMEOUT` | 5 |
| `federation.duckdb.path` | `duckdb.path` | `FEDERATION_DUCKDB_PATH` | `:memory:` |
| `federation.upgradeUrl` | `upgrade-url` | `FEDERATION_UPGRADE_URL` | portal pricing URL |
| (`host`/`port` fixed by chart) | `host` / `port` | `FEDERATION_HOST` / `FEDERATION_PORT` | `0.0.0.0` / 32020 |
| `federation.probes.useGrpc` | — (probe wiring) | — | true |
| `federation.tls.{enabled,secretName}` | — (Ingress) | — | false |
| `ingress.{enabled,className,annotations,hosts,tls}` | — (Ingress) | — | false |
| `license.publicKeySecretName` (+ `license.publicKeyKey`) | — | `SOFTCLIENT4ES_LICENSE_PUBLIC_KEY` | `""` / `license-public-key` |

**Sidecar** (per `sidecars[]`; image auto-selected by `elasticsearchVersion`; HOCON root `arrow.flight.*` + elasticsql core `elastic.credentials.*`):

| `values.yaml` | HOCON / config | Env var | Default |
|---|---|---|---|
| `sidecars[].elasticsearchVersion` (`6\|7\|8\|9`) | — (image selection) | — | — |
| `sidecars[].name` | `servers.<name>` key (RFC1123, unique) | — | — |
| `sidecars[].elasticsearch.url` (or `.scheme/.host/.port`) | `elastic.credentials.*` | `ELASTIC_SCHEME` / `ELASTIC_HOST` / `ELASTIC_PORT` | — (NO single ES-URL env) |
| `sidecars[].elasticsearch.credentialsSecretName` | `elastic.credentials.*` | `ELASTIC_AUTH_METHOD` / `ELASTIC_CREDENTIALS_{USERNAME,PASSWORD,API_KEY,BEARER_TOKEN}` | — |
| `sidecars[].arrow.batchSize` | `batch-size` | `ARROW_BATCH_SIZE` | 1000 |
| `sidecars[].arrow.queryTimeoutSeconds` | `query-timeout-seconds` | `ARROW_QUERY_TIMEOUT_SECONDS` | 120 |
| `sidecars[].arrow.maxMemory` | `join.max-memory` | `ARROW_JOIN_MAX_MEMORY` | `256m` |
| `sidecars[].auth.{method,credentialsSecretName}` | `auth.*` + federation `servers.<name>.credentials` | `ARROW_AUTH_{METHOD,USERNAME,PASSWORD,BEARER_TOKEN,API_KEY}` + `CONFIG_FORCE_*` | `none` |
| `sidecars[].default` | `servers.<name>.default` | — | false (≤1 true) |
| `sidecars[].alias` | `servers.<name>.alias` | — | `name` |
| `sidecars[].tls` | `servers.<name>.tls` (outgoing) | — | false |
| `sidecars[].replicaCount` / `.resources` / `.image.{repository,tag}` | — | — | 1 / — / version-default |

**License & telemetry** (elasticsql `licensing`; HOCON root `softclient4es.*`):

| `values.yaml` | HOCON | Env var | Default |
|---|---|---|---|
| `license.secretName` → `license-key` | `softclient4es.license.key` | `SOFTCLIENT4ES_LICENSE_KEY` | `""` (Community) |
| `license.secretName` → `api-key` | `softclient4es.license.api-key` | `SOFTCLIENT4ES_API_KEY` | `""` |
| `telemetry.enabled` | `softclient4es.telemetry.enabled` | `SOFTCLIENT4ES_TELEMETRY_ENABLED` | true |

> **No env override** (NOT chart-exposed): `softclient4es.license.{connect-timeout,read-timeout,grace-period,cache-dir,refresh.enabled,refresh.interval}` and `softclient4es.license.telemetry.enabled` (the license-refresh-metrics switch — DISTINCT from the daily-ping `softclient4es.telemetry.enabled` above; do not confuse them).

## 6. Secret management — choosing a backend

The chart **never creates a `Secret`** — you create it (raw, SealedSecrets, External Secrets Operator, or Vault Agent Injector) and reference it by name. One Secret feeds **both** the sidecar (`ARROW_AUTH_*`, `ELASTIC_*`) and, for federation→sidecar auth, the federation (`CONFIG_FORCE_*` via Typesafe Config `override_with_env_vars`). The four backends are documented in depth in the chart's `docs/secret-backends.md`, with ready-to-adapt manifests under `examples/sealed-secrets/` and `examples/external-secrets/`. In short:

- **Raw `kubectl create secret`** — simplest; fine for dev / GitOps with encrypted-at-rest etcd. Keys: `es-auth-method`/`es-username`/`es-password`/`es-api-key`/`es-bearer-token` (ES), `arrow-username`/`arrow-password`/`arrow-bearer-token`/`arrow-api-key` (sidecar auth), `license-key`/`api-key` (license), `tls.crt`/`tls.key` (TLS).
- **SealedSecrets** — commit an encrypted `SealedSecret` to Git; the in-cluster controller decrypts it. Re-seal per controller (certs are per-controller; `kubeseal` CLI version must match the controller `appVersion`, not the Helm chart version).
- **External Secrets Operator (ESO)** — sync from AWS/GCP Secret Manager or Vault. Examples use `external-secrets.io/v1` (the stable API; `v1beta1` was removed at ESO v0.17).
- **Vault Agent Injector** — inject secrets as files/env via pod annotations.

> **RFC1123 sidecar names are mandatory.** The federation→sidecar credential injection mangles the sidecar `name` into a `CONFIG_FORCE_*` env path; a name with `_`/`.`/uppercase mangles wrong and the federation exits with a non-zero status on startup. Use `[a-z0-9-]` names. **A wrong/missing Secret is a loud, SELF-HEALING CrashLoop** — the pod restarts and boots Ready once the Secret materializes (e.g. after ESO syncs). Do NOT uninstall; check the Secret's data keys.

**TLS at the Ingress, not the pod.** The federation listener is plaintext-only (it serves gRPC without in-pod TLS). `federation.tls.{enabled,secretName}` drives an **Ingress** `tls:` block (cert-manager `kubernetes.io/tls`); since Flight SQL is gRPC, the Ingress controller must proxy gRPC — nginx annotation `nginx.ingress.kubernetes.io/backend-protocol: "GRPC"` (NOT `GRPCS`; the pod upstream is plaintext h2c). This is DISTINCT from `sidecars[].tls` (outgoing per-downstream client TLS → `servers.<name>.tls`). In-cluster plaintext clients hit the Service (32020) directly.

## 7. Licensing

The federation reads its license from a referenced Secret as `SOFTCLIENT4ES_LICENSE_KEY` (offline Ed25519 JWT) and/or `SOFTCLIENT4ES_API_KEY` (automated portal provisioning). The gate is a **per-platform `maxClusters` quota**, enforced at startup against the number of `servers` (sidecars):

| Tier | `maxClusters` | Federation clusters you can run |
|---|---|---|
| **Community** (no license) | 1 | single-cluster federation — **free** |
| **Pro** | 5 | up to 5 sidecars / clusters |
| **Enterprise** | unlimited | any |

Exceeding the quota → the federation logs the over-quota error and exits with a non-zero status → **CrashLoopBackOff by design** (see §10). Federation is NOT a paid feature — single-cluster is the free adoption tier; the quota is on *cluster count*. To opt out of the daily anonymous usage ping, set `telemetry.enabled: false` (→ `SOFTCLIENT4ES_TELEMETRY_ENABLED=false`); this has zero impact on functionality or your license.

> **Offline verification.** For air-gapped or strict-egress clusters, mount the Ed25519 public JWK via `license.publicKeySecretName` (key `license.publicKeyKey`, default `license-public-key`) → `SOFTCLIENT4ES_LICENSE_PUBLIC_KEY`. This lets the federation verify a Pro/Enterprise JWT entirely offline (no portal round-trip). It is gated independently of `license.secretName`.

## 8. ES-version mixing & migration

Sidecars are independent — there is **no constraint** on mixing ES 6/7/8/9 in one federation in this release. The canonical migration story: run ES 8 sidecars in two regions while you migrate one region to ES 9; flip that region's `sidecars[].elasticsearchVersion` from `8` to `9` and `helm upgrade` once the ES cluster is upgraded. Cross-version JOINs with version-specific SQL features are the one caveat — flag them in testing (advanced cross-version cases surface in customer testing).

## 9. Upgrades & rollback

- **Upgrade:** edit `values.yaml`, `helm upgrade fed ./softclient4es-federation -f my-values.yaml`. Adding/removing a sidecar re-renders the federation ConfigMap; the federation pod restarts to pick up the new `servers` map.
- **Rollback:** `helm rollback fed <revision>` (`helm history fed` to list).
- **Secret rotation** is start-time: after changing a Secret, `kubectl rollout restart deployment` for BOTH the sidecar AND the federation (one Secret feeds both — restart both or the two sides drift).
- **Probes:** native gRPC readiness needs K8s 1.27+. On clusters **older than 1.27**, set `federation.probes.useGrpc: false` (TCP readiness on 32020) — this is the recommended fallback and needs **no extra binary**. The `exec`+`grpc_health_probe` approach is NOT a drop-in: the `grpc_health_probe` binary is **not bundled in the federation image**, so you would have to bake it into a custom image (or an init-container copy) yourself before an `exec` probe can run it. For nearly all operators, `useGrpc: false` is the simpler and supported path below 1.27.

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Federation pod `CrashLoopBackOff`, log "exceeds maxClusters" / non-zero exit | More sidecars than the license tier allows (Community=1, Pro=5) | Add/upgrade the license Secret (Pro/Enterprise), or reduce `sidecars[]`. §7 |
| `CrashLoopBackOff` after uncommenting `duckdb-attach` | the cluster count includes attach servers too → over Pro's 5 | Enterprise license, or keep the attach servers commented in this release. §4 |
| `CrashLoopBackOff`, "invalid credentials" at config load | `auth.method` ≠ none with creds only in a Secret but `federation.credentialsFromEnv` off, OR a non-RFC1123 sidecar name mangled the `CONFIG_FORCE_*` path | Enable `credentialsFromEnv`; rename sidecar to `[a-z0-9-]`. §6 |
| `CrashLoopBackOff` immediately at boot, no clear error | `readOnlyRootFilesystem` with no writable `/tmp` — the DuckDB JNI native lib can't extract | Ensure the `/tmp` `emptyDir` is present (chart default); don't override the volume. |
| Federation NotReady, "could not connect to sidecar X" | Sidecar pod / its backing ES unreachable; all-or-nothing gRPC readiness pulls the whole federation | Check sidecar pod + ES; for partial availability set `federation.probes.useGrpc: false`. §3/§9 |
| Wrong Secret keys / ESO sync lag | `optional:true` env can't validate Secret CONTENTS → boot-time `validate()` CrashLoop | Fix the Secret's data keys; the pod self-heals on the next restart — do NOT uninstall. §6 |
| License validation failure (**Pro/Enterprise only**) | A license JWT WAS supplied but is malformed / clock-skewed / expired beyond the 14-day grace | Re-provision via the portal (`SOFTCLIENT4ES_API_KEY`); check the JWT and node clock. **N.B.** with NO license (blank `SOFTCLIENT4ES_LICENSE_KEY`) the federation does NOT error — it runs on Community (1-cluster quota); that fallback is expected, not a failure. Community has no API-key/refresh path, so the portal re-provision step applies ONLY to a paid license. |
| TLS cert renewal failure | Cert lives at the **Ingress** (federation pod is plaintext) | Renew the cert-manager Certificate / Ingress `tls` Secret; nginx needs `backend-protocol: "GRPC"`. §6 (TLS) |
| ES connection failure from a sidecar | Wrong `ELASTIC_SCHEME/HOST/PORT/AUTH_METHOD` or ES creds | Verify the ES Secret + reachability; remember there is NO single ES-URL — scheme/host/port are separate. §5 |
| ES-version mismatch | `elasticsearchVersion` doesn't match the backing ES major | Set the sidecar's `elasticsearchVersion` to the ES cluster's major. §8 |

### SRE incident-triage walkthrough (three-region)

1. **Alert:** dashboards/queries failing across regions. `kubectl get pods` → the `fed-softclient4es-federation` pod is `0/1 NotReady`.
2. **Confirm the SPOF:** `kubectl describe pod` shows the gRPC readiness probe failing. With `useGrpc:true`, one unreachable region makes the *whole* federation NotReady (all-or-nothing aggregate).
3. **Find the bad region:** `kubectl get pods -l app.kubernetes.io/component=sidecar` → identify the `NotReady` sidecar; `kubectl logs` it for the ES connection error.
4. **Mitigate fast:** re-apply your existing values file AND flip the probe — `helm upgrade fed ./softclient4es-federation -f my-values.yaml --set federation.probes.useGrpc=false` → the federation goes Ready and serves the two healthy regions; only queries touching the down region fail. (Per-alias readiness is a follow-up-release backlog item.) ⚠️ **Always include `-f my-values.yaml`**: a bare `helm upgrade … --set …` resets every other value to the chart default (Helm does NOT reuse the previous release's values unless you pass `-f` or `--reuse-values`), which would silently revert your three-region `sidecars[]` / license / topology mid-incident.
5. **Fix root cause:** restore the down region's ES / sidecar, then flip `useGrpc` back to `true` for fail-closed semantics.

## 11. Performance tuning *(placeholder — iterate after this release's telemetry)*

> Hard numbers land after this release's telemetry. The qualitative levers today:

- **Scale federation replicas** (`replicaCount`) for concurrent-query throughput; the federation is stateless per query (DuckDB `:memory:`).
- **Scale sidecars** (`sidecars[].replicaCount`, document HA at 3) for per-cluster availability and parallelism.
- **`federation.maxMemory` / `sidecars[].arrow.maxMemory`** size the DuckDB / JOIN engine; raise for large cross-cluster JOINs, watch pod memory limits.
- **`batchSize`** trades latency vs throughput on streaming.
- Resource recommendations per cluster-count tier: TBD — seeded after this release's telemetry.

## 12. SLA implications

Pro includes 48h support. This guide is intended to be authoritative enough that the common scenarios (install, add/remove a cluster, mixed-version migration, the over-quota and connectivity failure modes) are self-serviceable without a ticket.
