# Kubernetes Production Deployment

This is the production-oriented infrastructure path for the browser automation system.

## Architecture

```text
User
  -> Ingress
  -> API pods
  -> MongoDB source of truth
  -> Redis queue
  -> Fixed Celery worker pods pick queued work
  -> Workers request browser sessions from Selenium Hub
  -> Selenium Hub assigns Chrome node pods
  -> Workers write results to MongoDB and send email
```

MongoDB is the source of truth. Redis is only the queue transport. Selenium only provides browser capacity.

## Prerequisites

- Kubernetes cluster
- Ingress controller, such as nginx ingress
- Container registry access
- Persistent storage class
- Optional but recommended: managed MongoDB and managed Redis

## Build And Push Image

Replace the registry with your real registry:

```bash
docker build -t ghcr.io/processzero/job-website-monitoring-agent:latest .
docker push ghcr.io/processzero/job-website-monitoring-agent:latest
```

Update the image in:

```text
infra/k8s/overlays/production/kustomization.yaml
```

## Create Secrets

Copy the example:

```bash
cp infra/k8s/base/secrets.example.yaml /tmp/job-monitoring-secrets.yaml
```

Edit `/tmp/job-monitoring-secrets.yaml` and replace all placeholders.

Then apply:

```bash
kubectl apply -f /tmp/job-monitoring-secrets.yaml
```

Do not commit real secrets.

## Deploy

```bash
kubectl apply -k infra/k8s/overlays/production
```

Check pods:

```bash
kubectl -n job-monitoring get pods
kubectl -n job-monitoring get svc
kubectl -n job-monitoring get hpa
```

Watch logs:

```bash
kubectl -n job-monitoring logs deploy/api -f
kubectl -n job-monitoring logs deploy/worker -f
kubectl -n job-monitoring logs deploy/selenium-hub -f
```

## Fixed Queue Processing

This deployment does not autoscale. Redis queues excess work and fixed worker pods process it one job at a time:

```text
Redis queue grows -> workers keep processing at fixed concurrency
Redis queue drains -> workers stay ready for the next process
```

Set worker and Chrome capacity manually:

```bash
kubectl -n job-monitoring scale deploy/worker --replicas=4
kubectl -n job-monitoring scale deploy/chrome-node --replicas=4
```

## Capacity Rule Of Thumb

Start with:

```text
1 Celery worker concurrency = 1
1 Chrome node max session = 1
worker pods <= chrome-node pods
```

Example:

```bash
kubectl -n job-monitoring scale deploy/worker --replicas=4
kubectl -n job-monitoring scale deploy/chrome-node --replicas=4
```

## Production Notes

For serious production, prefer:

- MongoDB Atlas instead of in-cluster MongoDB
- Managed Redis instead of in-cluster Redis
- Dedicated browser node pool for Chrome pods
- Resource limits on every pod
- Pod disruption budgets for API and Selenium Hub
- Central log collection
- Prometheus/Grafana monitoring

## Failure Behavior

If API pod dies:

```text
Workers keep running current tasks.
Kubernetes restarts API.
```

If worker pod dies:

```text
Task remains unacked in Redis.
Mongo recovery and Celery visibility timeout protect against lost work.
```

If Chrome node dies:

```text
Browser session dies.
Worker may recover session or fail/retry depending on where it happened.
```

If Redis dies:

```text
New queueing stops.
Current workers may continue briefly, but ACK/retry state is unsafe.
```

If Mongo dies:

```text
Process state cannot be safely updated.
Workers should fail/retry instead of continuing blindly.
```

## Local Port Forwarding

API:

```bash
kubectl -n job-monitoring port-forward svc/api 8110:8110
```

Selenium Hub:

```bash
kubectl -n job-monitoring port-forward svc/selenium-hub 4445:4444
```

Redis:

```bash
kubectl -n job-monitoring port-forward svc/redis 6379:6379
```

Mongo:

```bash
kubectl -n job-monitoring port-forward svc/mongo 27017:27017
```
