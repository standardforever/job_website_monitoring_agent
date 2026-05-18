# Monitoring

The app exposes:

- `/api/observability/summary`
- `/api/observability/alerts`
- `/api/observability/metrics`

These endpoints are protected by `CLIENT_REGISTRATION_PASSWORD` using the `x-registration-password` header.

For Prometheus Operator, add a `PodMonitor` only after deciding how your cluster will inject that header
or after splitting metrics onto a separate internal-only endpoint.
