# KEDA Scaling

`worker-scaledobject.yaml` is enabled by default and scales Celery workers from Redis queue depth.

`chrome-node-scaledobject.yaml` is intentionally not included in `kustomization.yaml`.
Queue-based Chrome scaling can remove browser pods while active Selenium sessions still exist.
Use it only after adding a Selenium Grid session/custom metric or a node-draining workflow.
