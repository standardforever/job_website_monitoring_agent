# Kubernetes Infrastructure

This folder contains Kubernetes manifests for running the job monitoring agent with:

- FastAPI API pods
- Celery worker pods
- Redis queue
- MongoDB
- Selenium Hub
- Chrome node pods
- KEDA worker autoscaling

Production entrypoint:

```bash
kubectl apply -k infra/k8s/overlays/production
```

Before applying, create a real `job-monitoring-secrets` Secret from `base/secrets.example.yaml`.
Do not apply the example secret as-is.

KEDA must be installed in the cluster before applying `infra/k8s/keda`.
