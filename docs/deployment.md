# Deployment Notes

> Last updated: 2026-05-27

This project is designed as a reproducible portfolio analytics application. The
current deployment target is a containerized Streamlit dashboard that reads a
pinned Gold artifact bundle from local disk or object storage.

## Security Boundary

Streamlit is the presentation layer, not the enterprise authentication layer.
For any shared deployment, place the container behind a reverse proxy with an
identity-aware gateway.

Reference pattern:

```text
Browser
  -> HTTPS load balancer / reverse proxy
  -> OAuth2-proxy or OIDC gateway
  -> Streamlit container on private network
  -> Read-only Gold artifact volume or object-store sync
```

Minimum controls:

- Terminate TLS at the proxy or load balancer.
- Require OAuth2 or OIDC authentication before requests reach Streamlit.
- Add HSTS and CSP headers at the proxy layer.
- Keep the Streamlit container on a private network.
- Mount artifacts read-only when possible.

The repository includes `.streamlit/config.toml` with explicit headless mode,
disabled usage telemetry, disabled CORS, and enabled XSRF protection.

Example `oauth2-proxy` container configuration:

```yaml
services:
  oauth2-proxy:
    image: quay.io/oauth2-proxy/oauth2-proxy:v7.6.0
    command:
      - --provider=oidc
      - --oidc-issuer-url=${OIDC_ISSUER_URL}
      - --client-id=${OIDC_CLIENT_ID}
      - --client-secret=${OIDC_CLIENT_SECRET}
      - --cookie-secret=${OAUTH2_PROXY_COOKIE_SECRET}
      - --email-domain=*
      - --upstream=http://streamlit:8501
      - --http-address=0.0.0.0:4180
      - --reverse-proxy=true
```

Example nginx reverse-proxy fragment:

```nginx
server {
    listen 443 ssl http2;
    server_name dashboard.example.com;

    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header Content-Security-Policy "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:" always;

    location / {
        proxy_pass http://oauth2-proxy:4180;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

## Artifact Versioning

Gold Parquet files should be versioned by run identifier or snapshot date.

Recommended object-store layout:

```text
s3://<bucket>/election-gender-bias/gold/run_id=<run_id>/
  sample_leaders.parquet
  mart_exposure_metrics.parquet
  mart_primary_frame_metrics.parquet
  mart_bias_indicators.parquet
  mart_trait_metrics.parquet
  news_corpus_qa_report.json
  nlp_qa_report.json
```

Production dashboards should pin a blessed artifact version with an environment
variable such as `DASHBOARD_GOLD_URI` or by mounting only the approved bundle.
Avoid reading a moving `latest/` path without a separate approval marker.

## Health And Readiness

The Dockerfile exposes Streamlit's built-in health endpoint:

```text
http://localhost:8501/_stcore/health
```

For production readiness, add a sidecar or scheduled check that validates the
analytical artifacts, not only the web process:

```bash
python -m src.cli.verify_dashboard_artifacts --gold-dir data/gold
python -m src.cli.verify_nlp_lexicon --duckdb-path warehouse/municipal.duckdb
```

Recommended artifact checks:

- `sample_leaders.parquet` exists and has 36 rows.
- Required Gold marts exist for exposure, regression, primary frames, bias
  indicators, and trait metrics.
- `news_corpus_qa_report.json` and `nlp_qa_report.json` expose run metadata.
- The pinned artifact run matches the approved release note.

## Alerting

Pipeline observability should trigger alerts before users discover stale or
failed dashboards.

Recommended rules:

- Alert when `meta_run.status = 'failed'`.
- Alert when a scheduled run does not complete by the expected time.
- Alert when required Gold artifacts are missing from the blessed bundle.
- Alert when row-count contracts fail, especially the 36-leader sample size.
- Alert when `build_artifact_health_warnings` reports stale regression outputs,
  cache-only web enrichment, NLP lineage mismatch, or an NLP bundle mismatch.

## PDF Export

Stakeholder PDFs should be exported from a production URL with browser headers
and footers disabled. Chrome and Edge print the current URL in the page footer
unless the print dialog option is turned off; this is outside Streamlit's CSS
control. Use the authenticated reverse-proxy URL for production exports and
disable browser print headers/footers so `localhost` does not appear in public
artifacts.

Prometheus-style rule template:

```yaml
groups:
  - name: election-gender-bias-dashboard
    rules:
      - alert: DashboardArtifactStale
        expr: election_dashboard_artifact_stale == 1
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Dashboard artifact bundle is stale or internally inconsistent"
      - alert: PipelineRunFailed
        expr: increase(election_pipeline_failed_total[1h]) > 0
        labels:
          severity: critical
        annotations:
          summary: "Election gender-bias pipeline failed"
```

Datadog monitor template:

```text
Metric: election.dashboard.artifact_health.error_count
Condition: avg(last_15m) > 0
Notify: data-platform-oncall
Message: Dashboard artifact health check is failing. Verify the blessed Gold
bundle, rerun the pipeline, and inspect news_corpus_qa_report.json plus
nlp_qa_report.json.
```

Slack, email, or incident-management routing can be implemented by the
orchestrator. The dashboard should remain a consumer of governed artifacts, not
the primary monitoring system.
