# dcre-pir

Payments Initial Responder: writes the single per-book ACK/NACK response file for one payments arrival into the per-client `onhost-resp/out` exchange directory. Spring Boot 4.1.0 / Spring Batch 6 / Java 25, launched by AGT as a short-lived Kubernetes Job.

The box caption on the payments sheet is **"Generates Initial Response"**.

## What it does

PIR is one of the two terminal stages of the payments request DAG:

```
PAYMENTS  onhost-req-pay:  PRR -> PTV -> PAI -> { PRW -> Fintegrate request
                                               || PIR -> OnHost response }
```

For one arrival it reads PRR's `tx_header` and PTV's `validation_log` from `dcre_pay`, composes a single ACK/NACK artifact (ACK means accepted-by-DCRE, never submitted-downstream, Fugu F11), and stages it atomically into the arrival client's `onhost-resp/out` directory for OnHost collection. AGT launches PIR for every business outcome of the upstream verdict stage (accepted AND file-fatal files get an initial response), causally independent of the `PRW` writer arm; no downstream DAG stage consumes PIR output. Response format is SYNTHETIC-CONTRACT pending the response-copybook recovery (Q-9).

### Why this repo exists: the payments split

PIR was forked from [dcre-cir](https://github.com/sean-huni/dcre-cir) and then REDUCED, rather than CIR being renamed, so CIR keeps its history and its repo (the maf-to-mas precedent in this estate). Design: `design-register/docs/specs/2026-08-07-payments-family-build-design.md`, SCRUM-107.

**CIR carries no `flow` constant to strip**, because it never branched on one: it composes an ACK/NACK from whatever verdicts it finds, and that logic is family-neutral. What it does carry is a COLLECTIONS-shaped identity, which is exactly the kind of thing a rename leaves behind silently, so three surfaces were repointed and are asserted by `PayFlowOnlyTest`:

- **The database.** `dcre_col` becomes `dcre_pay`. This one is the dangerous one: the two schemas are identical, so a PIR still reading `dcre_col` would compose payments responses out of collections verdicts and stay green the whole way.
- **The routes.** `onhost-req` becomes `onhost-req-pay`, and the A-45 cross-route twin case now uses the second payments route, `fint-resp-pay`. Route is part of arrival identity and is baked into the response FILENAME, so a leftover collections token names a payments response after a collections route.
- **The verdict vocabulary in the fixtures.** CIR seeds `FAIL_ACCOUNT_NOT_FOUND` and `FAIL_EXCEEDS_MANDATE_CAP`; neither is reachable on the payments leg, since an unknown account passes through to PAI and there is no mandate tier at all. The fixtures now seed `FAIL_ACCOUNT_NOT_ACTIVE` and `FAIL_EXCEEDS_RF_BALANCE`, which PTV can actually emit. The production code is verdict-agnostic, so the FIXTURES are the only place this service's family is visible, which is why the guard scans them.

Also dropped in the reduction: the hand-rolled `Dockerfile` (this repo builds its image with Paketo buildpacks, as PRR does).

`platform-response`, the shared initial-response core the design calls for (`CIR PIR MIR`), has **not** been extracted yet: see Concerns at the bottom.

## Architecture and principles

- **SOLID, 3-tier**: one responsibility per tier: `InitialResponseTasklet` (thin entry adapter: params in, one service call, status out) -> `InitialResponseService` (business tier: response composition + staging) -> `data/repo` (`TxHeaderViewRepo`, `VerdictViewRepo`: read-only view models over tables PIR does not own, grants-based R-04/R-06). Layer-first packages: `config`, `service`, `data/model`, `data/repo`.
- **12FactorApp Alignment - https://12factor.net/**: config strictly from the environment with committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process, CockroachDB and the exchange directory as attached backing resources, JVM exit code as the Batch outcome transport (`ExitCodeMain`, R-34).
- **Idempotent restart semantics**: the response filename carries the FULL arrival identity including the route token (A-45); `StagedWrite` (tmp + `ATOMIC_MOVE`) treats an existing target as a completed prior emission, so a rerun is a restart no-op (R-05), never a duplicate.

### Job structure

One job `pirJob`, one tasklet step `responseStep`, wrapped in the shared `CrdbRetryExceptionHandler("PIR")`: CockroachDB 40001 commit-time serialization aborts are retried in a fresh transaction (retry, never skip).

Job parameters:

- `arrival.id` (identifying, UUID)
- `route.id` (non-identifying, REQUIRED: arrival route token matching `[a-z0-9-]+`; part of the response identity, missing/invalid fails the job, A-45). On the payments leg this is `onhost-req-pay`.
- `fatal.reason` (optional, forces a NACK)
- `client.token` + `msg.id` (optional, headerless A-42 fallback identity)
- `outcome.hint` (optional, `BUSINESS_FILE_REJECTED` selects the R-41 policy NACK)

Response lines:

- Happy path: `ACK|<client>|<msgId>|<accepted>/<total>|ACCEPTED_BY_DCRE` plus one `REJ|<seq>|<outcome>` line per non-PASS verdict, ordered by sequence; `client` = `tx_header.initg_pty`, counts from `tx_count`.
- `fatal.reason` set, or zero verdict rows: single line `NACK|<client>|<msgId>|0/<total>|<reason>` (default reason `NO_VERDICTS`).
- `outcome.hint=BUSINESS_FILE_REJECTED`: `NACK|...|0/<total>|FILE_REJECTED_BY_POLICY` itemized with `REJ` lines (R-41 ALL_OR_NOTHING).
- Headerless arrival (A-42, PRR fataled before persisting the header): NACK with identity from `client.token`/`msg.id` job params, reason defaulting to `NO_HEADER`.

The `REJ` outcomes PIR can emit are exactly the ones PTV writes: `FAIL_ACCOUNT_NOT_ACTIVE`, `FAIL_EXCEEDS_RF_BALANCE`, `FAIL_EXCEEDS_CC_LIMIT`, `FAIL_DUPLICATE_E2E`, `FAIL_DUPLICATE_TX`. PIR itself never interprets them, but the fixtures assert them, so the list is part of this repo's contract with PTV.

Target: `<exchange-root>/<clientBase>/onhost-resp/out/<client>_<msgId>_<route>_RESP.txt`, resolved through the `ExchangeLayout` bean (per-client directory map, SCRUM-42). Resolution fails closed for an unconfigured client, so the A-42 `UNKNOWN` fallback never writes to a shared or wrong directory. An existing target is a completed prior emission: the rerun is a restart no-op (R-05), surfaced in the exit status message; the file path lands in the ExecutionContext as `responseFile`.

Outcome seam: on COMPLETED, a `JobExecutionListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (`OutcomeFileWriter`, R-33: AGT is the sole termination authority, absence is never success). `JOB_NAME` falls back to `local-pir-<executionId>`.

### Database and batch metadata

All in `dcre_pay`. No cross-database read of any kind.

Reads (grants-based, R-04/R-06): `tx_header` (PRR-owned) and `validation_log` (PTV-owned). Writes: `pir_response`, the SCRUM-58 write-ahead filename ledger and system of record for every initial ACK/NACK. One row per arrival, committed BEFORE the file is staged; `arrival_id` and `file_name` unique; `route_id` part of identity (A-45); `written_at` NULL after commit is the staged-not-written stuck signal.

Liquibase: master changelog -> `2026/08/001-pir-response.xml` (the ledger) and `2026/08/002-batch-metadata.xml` (`batch-metadata-pir.sql`, the autogenerated Spring Batch DDL under prefix `PIR_BATCH_`), with `initialize-schema: never`. Per-service history tables `pir_databasechangelog` / `pir_databasechangeloglock`.

**Both changesets guard with `onFail="CONTINUE"`, never `MARK_RAN`.** CIR's `003-cir-response` uses MARK_RAN and that shape has cost this project two defects (A-79, A-81): MARK_RAN records the skip PERMANENTLY, so a database that was merely not-yet-ready at the moment of the check never receives the change at all. The batch-metadata sqlFile carries `IF NOT EXISTS` on every one of its nine `CREATE`s plus `<validCheckSum>ANY</validCheckSum>`, and deliberately no precondition: one file creating nine objects cannot be sampled by a precondition without reading a half-applied file as complete.

An `ApplicationRunner` at `@Order(-10)` runs `StaleExecutionSweeper.abandonStale(ds, "PIR_BATCH_", 60)` before job launch, so an execution stranded in STARTED by a killed pod never blocks the relaunch with the same identity (A-39a).

### Platform modules

| Module | Version | Scope | Used for |
|---|---|---|---|
| `dcre-platform-persistence` | 0.1.0 | `implementation` | `JdbcConfig` (Spring Data JDBC base config, imported by `PirApplication`) |
| `dcre-platform-batch` | 0.1.0 | `implementation` | `ExitCodeMain` (R-34), `OutcomeFileWriter` (outcome seam), `StaleExecutionSweeper` (A-39a), `CrdbRetryExceptionHandler` (40001 retry), shared `dcre-exchange-layout.yml` classpath resource (drives the `ExchangeLayout` bean via `spring.config.import`) |

`StagedWrite` and `ExchangeLayout` come from `dcre-platform-files`, not declared directly: they arrive transitively via `dcre-platform-batch`'s `api` chain (batch brings files brings model). All platform artifacts resolve from Maven Local only.

No metrics wiring yet (no Actuator/Micrometer dependency): logs, the outcome seam file, `pir_response` and `PIR_BATCH_` metadata are the operational sources of truth.

## Prerequisites

- Java 25 (`.sdkmanrc` pins `25-tem`; Gradle 9.5.1 wrapper committed)
- Docker (Testcontainers in the test suite, Paketo image build)
- Platform libs `za.co.fnb.dcre:platform-*:0.1.0` published to Maven Local (see Quickstart)
- At runtime: a reachable CockroachDB and the exchange directory tree (`dcre-infra` locally)

## Quickstart

```bash
# 1. Publish the platform libs to Maven Local: run in each platform repo clone,
#    chain order dcre-platform-model -> dcre-platform-files -> dcre-platform-batch;
#    dcre-platform-persistence is standalone.
./gradlew publishToMavenLocal

# 2. Build + test this repo (Docker required for Testcontainers)
./gradlew build

# 3. Run one-shot against local defaults (CockroachDB on localhost:26257, dcre-infra exchange)
java -jar build/libs/pir-2.0.jar 'arrival.id=<uuid>' route.id=onhost-req-pay                      # ACK path
java -jar build/libs/pir-2.0.jar 'arrival.id=<uuid>' route.id=onhost-req-pay 'fatal.reason=<why>' # NACK path
```

A clean clone runs with NO `.env`: working dev defaults are committed in `application.yml`.

## Configuration

Precedence: committed yml default < environment variable. The per-client exchange directory map itself ships as the `dcre-exchange-layout.yml` classpath resource in `dcre-platform-batch`.

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_pay?sslmode=disable` | the payments CockroachDB via pgwire |
| `DCRE_DB_USER` / `DCRE_DB_PASSWORD` | `root` / empty | DB credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root: per-client response dirs + outcome seam |
| `DCRE_AGTOPS_DB_URL` / `_USER` / `_PASSWORD` | `…/agt_ops`, `root`, empty | heartbeat liveness stamp (M12) |
| `DCRE_AMOUNT_SCALE` | `2` | Fleet-wide flag; not read by PIR sources |
| `JOB_NAME` | `local-pir-<executionId>` | K8s-injected identity for the outcome seam |

## Testing

```bash
./gradlew test
```

Docker required: Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3`. Coverage:

- `PayFlowOnlyTest`: the fork guard. No flow discriminator, no collections route token and no DC-only verdict in either the sources or the FIXTURES; the configured database is `dcre_pay`; the ledger is `pir_response` and its changeset guards with CONTINUE rather than MARK_RAN.
- `PirJobTest`: ACK with REJ details plus restart no-op (R-05: rerun never rewrites the file), file-fatal NACK path, fail-closed on a missing `route.id` and on an unconfigured client.
- `InitialResponseServiceIT`: headerless A-42 NACKs (`NO_HEADER`, fail-closed without `client.token`), R-41 policy NACK, distinct responses for the same (client, msgId) on two payments routes, missing/invalid `route.id` fail-closed (A-45).
- `PirResponseCaptureIT`: the SCRUM-58 write-ahead ledger, including the cross-route twin producing two rows with distinct filenames.
- `PirJobConfigRetryTest`: the real `responseStep` retries commit-time 40001 aborts in a fresh transaction.
- Cucumber BDD suite (`CucumberSuiteTest`, `features/pir_initial_response.feature`): full-accept ACK, per-record rejections, file-fatal NACK, `NO_VERDICTS` NACK, ledger capture and rerun-unchanged scenarios against the real job + CockroachDB.

## Local cluster deployment

```bash
./gradlew bootBuildImage        # Paketo, BP_JVM_VERSION=25, produces dcre-pir:2.0
kind load docker-image --name dcre-dev dcre-pir:2.0
```

In the cluster AGT mints PIR as an ephemeral K8s Job with `JOB_NAME` and the identifying parameters; the JobRepository dedupes on them (restart-not-duplicate). Fleet releases are digits-only 3-component SemVer git tags, uniform across the fleet; `scripts/switch-version.sh VERSION` in `dcre-infra` switches the fleet, `scripts/env-reset.sh` resets to a clean slate.

## Concerns and follow-ups

- **`platform-response` is not extracted.** The build design sequences PIR after extracting the shared initial-response core out of CIR (`platform-response`, consumed by CIR PIR MIR). This repo was built by the clone-and-reduce method instead, so `InitialResponseService`, `ResponseLedgerWriter` and `CrdbRetry` are currently DUPLICATED between CIR and PIR. That is a real fork risk on a body of logic with fiddly A-42/A-45/R-05 semantics, and the extraction should follow before either copy is changed.
- **AGT does not know this stage exists yet.** `Stage.PIR`, the `onhost-req-pay` route and `RouteDags.PAY` are AGT edits, listed in the split report, not made here.

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Payments stages: [dcre-prr](https://github.com/sean-huni/dcre-prr), [dcre-ptv](https://github.com/sean-huni/dcre-ptv), [dcre-pai](https://github.com/sean-huni/dcre-pai), [dcre-prw](https://github.com/sean-huni/dcre-prw)
- Fork source: [dcre-cir](https://github.com/sean-huni/dcre-cir)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Infra and tooling: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register)
