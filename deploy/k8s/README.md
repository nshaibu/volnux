# Volnux Kubernetes Deployment Guide

Production-ready deployment of Volnux on Kubernetes with PostgreSQL,
dual Redis instances, OTel observability, TLS ingress, autoscaling,
and nightly database backups.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                            │
│                                                                      │
│  ┌─────────────────────┐    ┌─────────────────────┐                 │
│  │    API Deployment    │    │  Worker Deployment   │                 │
│  │  (replicas: 2-5)     │    │  (replicas: 3-20)    │                 │
│  │                      │    │                      │                 │
│  │  • REST API          │    │  • Workflow engine   │                 │
│  │  • TriggerEngine     │    │  • EventBase exec    │                 │
│  │  • RehydrationMgr    │    │  • CheckpointMgr     │                 │
│  │  • Web Dashboard     │    │  • RemoteManager     │                 │
│  │  • NodeHeartbeat     │    │  • NodeHeartbeat     │                 │
│  └──────────┬──────────┘    └──────────┬───────────┘                 │
│             │                          │                              │
│             └───────────┬──────────────┘                              │
│                         │                                             │
│  ┌──────────────────────▼──────────────────────────────────────────┐ │
│  │                   Infrastructure Layer                           │ │
│  │                                                                  │ │
│  │  ┌────────────┐  ┌────────────────┐  ┌──────────────────┐      │ │
│  │  │ PostgreSQL │  │ Redis          │  │ Redis            │      │ │
│  │  │            │  │ (Checkpoints)  │  │ (Broker)         │      │ │
│  │  │ Governance │  │ • checkpoints  │  │ • Celery tasks   │      │ │
│  │  │ Audit trail│  │ • HITL queue   │  │ • ScheduleTrigger│      │ │
│  │  │ Asset cat  │  │ • cmd channel  │  │ noeviction       │      │ │
│  │  │ RBAC       │  │ • task location│  │                  │      │ │
│  │  │            │  │ volatile-lru   │  │                  │      │ │
│  │  │            │  │ AOF persist    │  │                  │      │ │
│  │  └────────────┘  └────────────────┘  └──────────────────┘      │ │
│  │                                                                  │ │
│  │  ┌────────────┐  ┌────────────────┐  ┌──────────────────┐      │ │
│  │  │   Grafana  │  │     Tempo      │  │   Prometheus     │      │ │
│  │  │ (external) │  │  (external)    │  │   (external)     │      │ │
│  │  └────────────┘  └────────────────┘  └──────────────────┘      │ │
│  │                                                                  │ │
│  │  ┌────────────────────────────────────────────────────────────┐ │ │
│  │  │              OTel Collector (in-namespace)                 │ │ │
│  │  │  Receives traces + metrics → Tempo + Prometheus            │ │ │
│  │  └────────────────────────────────────────────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## File Index

| File | Description |
|---|---|
| `00-namespace.yaml` | Namespace with labels |
| `01-secrets.example.yaml` | Secret template (copy to `secrets.yaml`) |
| `02-configmap.yaml` | Non-sensitive config + `settings.py` |
| `03-pvc.yaml` | Persistent Volume Claims |
| `04-postgres.yaml` | PostgreSQL StatefulSet + config + init scripts |
| `05-redis.yaml` | Redis checkpoints (AOF) + Redis broker (noeviction) |
| `06-rbac.yaml` | Service accounts + Roles + RoleBindings |
| `07-api-deployment.yaml` | API Deployment + Service |
| `08-worker-deployment.yaml` | Worker Deployment + Services |
| `09-otel-collector.yaml` | OTel Collector Deployment + config |
| `10-hpa-pdb-quota.yaml` | HPA + PDB + ResourceQuota + LimitRange |
| `11-network-policy.yaml` | NetworkPolicies for all components |
| `12-ingress.yaml` | Ingress with TLS (cert-manager) |
| `13-postgres-backup-cronjob.yaml` | Nightly pg_dump CronJob |
| `kustomization.yaml` | Kustomize manifest |

---

## Prerequisites

```bash
# Kubernetes 1.27+
kubectl version --client

# cert-manager (for TLS certificates)
helm repo add jetstack https://charts.jetstack.io
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set installCRDs=true

# nginx-ingress controller
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace

# Verify both are running
kubectl get pods -n cert-manager
kubectl get pods -n ingress-nginx
```

### Storage class for ReadWriteMany (workflow code volume)

The `volnux-workflows-pvc` requires a `ReadWriteMany` storage class.
Choose based on your cloud provider:

```bash
# AWS — EFS CSI driver
helm install aws-efs-csi-driver aws-efs-csi-driver/aws-efs-csi-driver \
  -n kube-system
# Then set storageClassName: efs-sc in 03-pvc.yaml

# GCP — Filestore CSI (standard-rwx)
# Enabled by default on GKE — set storageClassName: standard-rwx

# Azure — Azure Files
# Enabled by default on AKS — set storageClassName: azurefile

# Alternative: bake workflow code into the container image
# Remove the workflows PVC and volume mounts from deployments
```

---

## Deployment Steps

### 1. Configure secrets

```bash
cp deploy/kubernetes/01-secrets.example.yaml deploy/kubernetes/secrets.yaml
```

Edit `secrets.yaml` with real values. Generate secure random values:

```bash
# Password
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# JWT secret (needs to be longer)
python3 -c "import secrets; print(secrets.token_urlsafe(48))"
```

### 2. Configure hostnames

Edit `12-ingress.yaml` and replace:
- `api.volnux.example.com` → your API domain
- `dashboard.volnux.example.com` → your dashboard domain
- `webhooks.volnux.example.com` → your webhook callback domain

Edit `02-configmap.yaml` and replace:
- `your-registry/volnux-engine:2.0.0` → your container image

### 3. Create namespace and secrets

```bash
kubectl apply -f deploy/kubernetes/00-namespace.yaml
kubectl apply -f deploy/kubernetes/secrets.yaml
```

### 4. Deploy infrastructure

```bash
# ConfigMap, PVCs, PostgreSQL, Redis, RBAC
kubectl apply -f deploy/kubernetes/02-configmap.yaml
kubectl apply -f deploy/kubernetes/03-pvc.yaml
kubectl apply -f deploy/kubernetes/04-postgres.yaml
kubectl apply -f deploy/kubernetes/05-redis.yaml
kubectl apply -f deploy/kubernetes/06-rbac.yaml

# Wait for infrastructure to be ready
kubectl wait --for=condition=ready pod -l app=postgres \
  -n volnux --timeout=120s
kubectl wait --for=condition=ready pod -l app=redis-checkpoints \
  -n volnux --timeout=60s
kubectl wait --for=condition=ready pod -l app=redis-broker \
  -n volnux --timeout=60s
```

### 5. Run database migrations

Migrations run automatically via the init container in the API deployment.
To run them manually before deploying:

```bash
kubectl run volnux-migrate \
  --rm -it --restart=Never \
  --image=your-registry/volnux-engine:2.0.0 \
  --namespace=volnux \
  --env="VOLNUX_POSTGRES_PASSWORD=$(kubectl get secret volnux-secrets -n volnux -o jsonpath='{.data.postgres-password}' | base64 -d)" \
  --env="VOLNUX_REDIS_CHECKPOINT_URL=redis://redis-checkpoints.volnux.svc.cluster.local:6379/0" \
  -- volnux migrate up
```

### 6. Deploy Volnux and observability

```bash
kubectl apply -f deploy/kubernetes/07-api-deployment.yaml
kubectl apply -f deploy/kubernetes/08-worker-deployment.yaml
kubectl apply -f deploy/kubernetes/09-otel-collector.yaml
kubectl apply -f deploy/kubernetes/10-hpa-pdb-quota.yaml
kubectl apply -f deploy/kubernetes/11-network-policy.yaml
kubectl apply -f deploy/kubernetes/12-ingress.yaml
kubectl apply -f deploy/kubernetes/13-postgres-backup-cronjob.yaml
```

Or apply everything at once with Kustomize:

```bash
# Note: rename secrets.example.yaml to secrets.yaml first
kubectl apply -k deploy/kubernetes/
```

### 7. Verify deployment

```bash
# All pods running
kubectl get pods -n volnux

# Expected output:
# NAME                              READY   STATUS    RESTARTS
# postgres-0                        2/2     Running   0
# redis-checkpoints-0               2/2     Running   0
# redis-broker-xxx                  2/2     Running   0
# volnux-api-xxx                    1/1     Running   0
# volnux-api-yyy                    1/1     Running   0
# volnux-worker-xxx                 1/1     Running   0
# volnux-worker-yyy                 1/1     Running   0
# volnux-worker-zzz                 1/1     Running   0
# otel-collector-xxx                1/1     Running   0

# Health check
kubectl port-forward svc/volnux-api 8080:8080 -n volnux
curl http://localhost:8080/api/v1/health

# Or via ingress (replace with your domain)
curl https://api.volnux.example.com/api/v1/health
```

---

## Resource Sizing

| Tier | Workflows/day | API replicas | Worker replicas | PostgreSQL | Redis (checkpoints) | Redis (broker) |
|---|---|---|---|---|---|---|
| Light | < 100 | 2 | 3 | 1 CPU, 2 GB | 500m CPU, 1 GB | 250m CPU, 512 MB |
| Medium | < 1,000 | 2 | 5 | 2 CPU, 4 GB | 500m CPU, 2 GB | 250m CPU, 512 MB |
| Heavy | < 10,000 | 3 | 10 | 4 CPU, 8 GB | 1 CPU, 4 GB | 500m CPU, 1 GB |
| Enterprise | 10,000+ | 5 | 20+ | 8 CPU, 16 GB | 2 CPU, 8 GB | 1 CPU, 2 GB |
| Agent-heavy | < 1,000 (LLM) | 2 | 5 | 2 CPU, 4 GB | 1 CPU, 4 GB | 500m CPU, 1 GB |

**Agent-heavy note:** AgentEventBase workloads require higher worker memory
(LLM response buffering, concurrent tool execution). Increase worker
`limits.memory` to 16Gi and `requests.memory` to 4Gi for agent-heavy deployments.

---

## Graceful Shutdown

Workers checkpoint in-flight events before terminating. This is why
`terminationGracePeriodSeconds: 180` exceeds `CHECKPOINT_DRAIN_TIMEOUT: 120`.

The shutdown sequence on `kubectl rollout` or node drain:

```
1. Kubernetes calls preStop hook (sleep 5 + volnux shutdown --drain-timeout 110)
2. Worker stops accepting new workflow dispatches
3. In-flight events complete their current lifecycle phase
4. Checkpoint queue drains (up to 110s)
5. preStop exits
6. Kubernetes sends SIGTERM
7. Process exits cleanly
8. Kubernetes waits up to terminationGracePeriodSeconds (180s) total
```

If a worker is killed before draining (SIGKILL), affected events resume
from their last persisted checkpoint on the next available worker.

---

## Disaster Recovery

### PostgreSQL

**Daily backups:** `postgres-backup` CronJob runs at 02:00 UTC.
Backups are stored in custom format (`pg_dump -Fc`) on `postgres-backup-pvc`.
Retention: 30 daily, 12 weekly.

**Restore:**
```bash
# Copy backup file from PVC
kubectl cp volnux/$(kubectl get pods -n volnux -l app=postgres-backup \
  -o jsonpath='{.items[0].metadata.name}'):/backup/volnux-TIMESTAMP.dump \
  ./volnux-restore.dump

# Full restore
pg_restore \
  --host=POSTGRES_HOST \
  --username=volnux \
  --dbname=volnux \
  --clean --if-exists \
  --jobs=4 \
  --verbose \
  volnux-restore.dump

# Selective table restore (e.g. audit trail only)
pg_restore \
  --host=POSTGRES_HOST \
  --username=volnux \
  --dbname=volnux \
  --table=volnux_AuditEntry \
  volnux-restore.dump
```

### Redis Checkpoints

AOF persistence is enabled. On Redis pod restart, the AOF log is replayed
and all checkpoint data is restored. No manual intervention required.

**Important:** The HITL queue lives on `redis-checkpoints` (AOF persistence).
Pending human approval requests survive Redis pod restarts.

### Redis Broker

No persistence. Celery redelivers queued tasks on worker reconnection.
ScheduleTrigger tasks that were queued but not consumed are requeued on
the next cron evaluation.

### Recovery procedure after complete cluster loss

```bash
# 1. Restore PostgreSQL from latest backup (see above)
# 2. Deploy infrastructure (Redis, PostgreSQL)
# 3. Deploy Volnux (API, workers)
# 4. Redis checkpoints repopulate automatically as workflows execute
#    (in-flight workflows resume from PostgreSQL-stored checkpoints)
# 5. HITL queue: empty on fresh Redis. Workflows with pending HITL
#    requests will reach their timeout and route to the timeout branch.
#    Operators can re-trigger affected workflows if needed.
# 6. Trigger state rebuilds from TriggerConfig on engine restart.
```

---

## Monitoring

### Prometheus scrape targets

All pods expose metrics on port 9464. Configure Prometheus scrape:

```yaml
# prometheus/scrape-config.yaml
scrape_configs:
  - job_name: volnux
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [volnux]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: "true"
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        target_label: __address__
        regex: (.+)
        replacement: "${1}"
```

### Key alerts

```yaml
# Recommended Prometheus alerting rules
groups:
  - name: volnux
    rules:
      - alert: VolnuxWorkerDown
        expr: up{job="volnux", component="worker"} == 0
        for: 2m

      - alert: VolnuxCheckpointQueueHigh
        expr: volnux_checkpoint_queue_depth > 1000
        for: 5m

      - alert: VolnuxHITLQueueHigh
        expr: volnux_hitl_pending_count > 100
        for: 10m

      - alert: VolnuxHighWorkflowFailureRate
        expr: rate(volnux_event_errors_total[5m]) > 0.1
        for: 5m

      - alert: VolnuxRedisCheckpointsDown
        expr: up{job="volnux", app="redis-checkpoints"} == 0
        for: 1m
```

---

## Updating Volnux

```bash
# Update image tag in kustomization.yaml, then:
kubectl set image deployment/volnux-api \
  volnux-api=your-registry/volnux-engine:NEW_VERSION \
  -n volnux

kubectl set image deployment/volnux-worker \
  volnux-worker=your-registry/volnux-engine:NEW_VERSION \
  -n volnux

# Monitor rollout
kubectl rollout status deployment/volnux-worker -n volnux

# Rollback if needed
kubectl rollout undo deployment/volnux-worker -n volnux
```

Workers drain checkpoints before terminating during rolling updates.
No in-flight event state is lost.
