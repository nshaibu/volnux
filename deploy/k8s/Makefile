# Makefile
# Common operations for Volnux development, building, and deployment.
#
# Usage:
#   make dev         Start local development infrastructure
#   make build       Build container image
#   make deploy      Deploy to Kubernetes
#   make test        Run test suite
#   make help        Show all targets

REGISTRY     ?= your-registry
IMAGE_NAME   ?= volnux-engine
VERSION      ?= $(shell python3 -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])" 2>/dev/null || echo "2.0.0")
IMAGE        := $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
IMAGE_LATEST := $(REGISTRY)/$(IMAGE_NAME):latest
K8S_DIR      := deploy/kubernetes
NAMESPACE    := volnux

.DEFAULT_GOAL := help

# ── Colours ───────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
BLUE  := \033[34m
YELLOW:= \033[33m
RED   := \033[31m

.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "  $(BOLD)Volnux Deployment Makefile$(RESET)"
	@echo ""
	@echo "  $(BOLD)Version:$(RESET) $(VERSION)"
	@echo "  $(BOLD)Image:$(RESET)   $(IMAGE)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "  $(BOLD)%-28s %s$(RESET)\n", "Target", "Description"} \
	     /^[a-zA-Z_-]+:.*##/ { printf "  $(GREEN)%-28s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

# ══════════════════════════════════════════════════════════════════════════════
# DEVELOPMENT
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: dev
dev: ## Start local development infrastructure (PostgreSQL, Redis, OTel, Grafana)
	@echo "$(BLUE)Starting development infrastructure...$(RESET)"
	docker compose up -d postgres redis-checkpoints redis-broker otel-collector tempo prometheus grafana
	@echo ""
	@echo "$(GREEN)Infrastructure running:$(RESET)"
	@echo "  PostgreSQL:  localhost:5432"
	@echo "  Redis:       localhost:6379 (checkpoints), localhost:6380 (broker)"
	@echo "  Prometheus:  http://localhost:9090"
	@echo "  Grafana:     http://localhost:3000  (admin/admin)"
	@echo "  Tempo:       http://localhost:3200"
	@echo ""
	@echo "Start Volnux:"
	@echo "  make dev-api    Start API server"
	@echo "  make dev-worker Start worker"

.PHONY: dev-api
dev-api: ## Start Volnux API in development mode
	@echo "$(BLUE)Starting Volnux API...$(RESET)"
	docker compose --profile volnux up -d volnux-api
	@echo "$(GREEN)API running at http://localhost:8080$(RESET)"
	@echo "Swagger UI: http://localhost:8080/api/v1/docs"

.PHONY: dev-worker
dev-worker: ## Start Volnux worker in development mode
	@echo "$(BLUE)Starting Volnux worker...$(RESET)"
	docker compose --profile volnux up -d volnux-worker

.PHONY: dev-all
dev-all: ## Start all services including Volnux API and worker
	@echo "$(BLUE)Starting all services...$(RESET)"
	docker compose --profile volnux up -d

.PHONY: dev-stop
dev-stop: ## Stop all development services
	docker compose --profile volnux down

.PHONY: dev-clean
dev-clean: ## Stop all services and remove volumes (clean slate)
	@echo "$(RED)Removing all volumes — all local data will be lost.$(RESET)"
	@read -p "Continue? [y/N] " confirm && [ "$$confirm" = "y" ]
	docker compose --profile volnux down -v

.PHONY: dev-logs
dev-logs: ## Tail logs from all services
	docker compose --profile volnux logs -f

.PHONY: dev-logs-api
dev-logs-api: ## Tail Volnux API logs
	docker compose logs -f volnux-api

.PHONY: dev-logs-worker
dev-logs-worker: ## Tail Volnux worker logs
	docker compose logs -f volnux-worker

.PHONY: dev-migrate
dev-migrate: ## Run database migrations in development
	@echo "$(BLUE)Running migrations...$(RESET)"
	docker compose run --rm volnux-api migrate
	@echo "$(GREEN)Migrations complete$(RESET)"

.PHONY: dev-shell
dev-shell: ## Open a shell in the Volnux API container
	docker compose exec volnux-api /bin/bash

# ══════════════════════════════════════════════════════════════════════════════
# BUILDING
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: build
build: ## Build the Volnux container image
	@echo "$(BLUE)Building $(IMAGE)...$(RESET)"
	docker build \
	  --tag $(IMAGE) \
	  --tag $(IMAGE_LATEST) \
	  --build-arg VERSION=$(VERSION) \
	  --label "org.opencontainers.image.version=$(VERSION)" \
	  --label "org.opencontainers.image.created=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)" \
	  --label "org.opencontainers.image.source=https://github.com/your-org/volnux" \
	  --file Dockerfile \
	  .
	@echo "$(GREEN)Built $(IMAGE)$(RESET)"

.PHONY: build-no-cache
build-no-cache: ## Build without Docker layer cache
	docker build --no-cache --tag $(IMAGE) --tag $(IMAGE_LATEST) --file Dockerfile .

.PHONY: push
push: build ## Build and push image to registry
	@echo "$(BLUE)Pushing $(IMAGE)...$(RESET)"
	docker push $(IMAGE)
	docker push $(IMAGE_LATEST)
	@echo "$(GREEN)Pushed $(IMAGE)$(RESET)"

.PHONY: image-size
image-size: ## Show the size of the built image
	docker images $(IMAGE) --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

# ══════════════════════════════════════════════════════════════════════════════
# TESTING
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: test
test: ## Run the full test suite
	VOLNUX_ENV=test pytest tests/ -v --tb=short

.PHONY: test-unit
test-unit: ## Run unit tests only (fast, no infrastructure)
	VOLNUX_ENV=test pytest tests/unit/ -v --tb=short

.PHONY: test-integration
test-integration: ## Run integration tests (requires dev infrastructure)
	VOLNUX_ENV=test pytest tests/integration/ -v --tb=short

.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	VOLNUX_ENV=test pytest tests/ \
	  --cov=volnux \
	  --cov-report=html:htmlcov \
	  --cov-report=term-missing \
	  --cov-fail-under=80

.PHONY: test-agent
test-agent: ## Run agent-specific tests (requires Ollama running)
	VOLNUX_ENV=test pytest tests/agents/ -v --tb=short

.PHONY: lint
lint: ## Run linter (ruff)
	ruff check volnux/ tests/

.PHONY: format
format: ## Format code (black + isort via ruff)
	ruff format volnux/ tests/

.PHONY: typecheck
typecheck: ## Run type checker (mypy)
	mypy volnux/ --ignore-missing-imports

.PHONY: check
check: lint typecheck test ## Run lint + typecheck + tests

# ══════════════════════════════════════════════════════════════════════════════
# KUBERNETES DEPLOYMENT
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: k8s-check
k8s-check: ## Verify kubectl is configured and cluster is reachable
	@echo "$(BLUE)Checking Kubernetes connection...$(RESET)"
	kubectl cluster-info
	kubectl get nodes
	@echo "$(GREEN)Cluster reachable$(RESET)"

.PHONY: k8s-create-namespace
k8s-create-namespace: ## Create the volnux namespace
	kubectl apply -f $(K8S_DIR)/00-namespace.yaml

.PHONY: k8s-secrets
k8s-secrets: ## Apply secrets (copy secrets.example.yaml to secrets.yaml first)
	@if [ ! -f $(K8S_DIR)/secrets.yaml ]; then \
	  echo "$(RED)Error: $(K8S_DIR)/secrets.yaml not found.$(RESET)"; \
	  echo "Copy $(K8S_DIR)/01-secrets.example.yaml to secrets.yaml and fill in values."; \
	  exit 1; \
	fi
	kubectl apply -f $(K8S_DIR)/secrets.yaml

.PHONY: k8s-infra
k8s-infra: ## Deploy PostgreSQL, Redis, PVCs (infrastructure only)
	@echo "$(BLUE)Deploying infrastructure...$(RESET)"
	kubectl apply -f $(K8S_DIR)/02-configmap.yaml
	kubectl apply -f $(K8S_DIR)/03-pvc.yaml
	kubectl apply -f $(K8S_DIR)/04-postgres.yaml
	kubectl apply -f $(K8S_DIR)/05-redis.yaml
	kubectl apply -f $(K8S_DIR)/06-rbac.yaml
	@echo "$(BLUE)Waiting for infrastructure pods...$(RESET)"
	kubectl wait --for=condition=ready pod -l app=postgres \
	  -n $(NAMESPACE) --timeout=120s
	kubectl wait --for=condition=ready pod -l app=redis-checkpoints \
	  -n $(NAMESPACE) --timeout=60s
	kubectl wait --for=condition=ready pod -l app=redis-broker \
	  -n $(NAMESPACE) --timeout=60s
	@echo "$(GREEN)Infrastructure ready$(RESET)"

.PHONY: k8s-migrate
k8s-migrate: ## Run database migrations as a K8s Job
	@echo "$(BLUE)Running database migrations...$(RESET)"
	kubectl run volnux-migrate-$(shell date +%s) \
	  --rm -it --restart=Never \
	  --image=$(IMAGE) \
	  --namespace=$(NAMESPACE) \
	  --env="VOLNUX_POSTGRES_PASSWORD=$$(kubectl get secret volnux-secrets -n $(NAMESPACE) -o jsonpath='{.data.postgres-password}' | base64 -d)" \
	  --env="VOLNUX_REDIS_CHECKPOINT_URL=redis://redis-checkpoints.$(NAMESPACE).svc.cluster.local:6379/0" \
	  -- migrate
	@echo "$(GREEN)Migrations complete$(RESET)"

.PHONY: k8s-app
k8s-app: ## Deploy Volnux API, worker, OTel, HPA, PDB, NetworkPolicy
	@echo "$(BLUE)Deploying Volnux application...$(RESET)"
	kubectl apply -f $(K8S_DIR)/07-api-deployment.yaml
	kubectl apply -f $(K8S_DIR)/08-worker-deployment.yaml
	kubectl apply -f $(K8S_DIR)/09-otel-collector.yaml
	kubectl apply -f $(K8S_DIR)/10-hpa-pdb-quota.yaml
	kubectl apply -f $(K8S_DIR)/11-network-policy.yaml
	kubectl apply -f $(K8S_DIR)/12-ingress.yaml
	kubectl apply -f $(K8S_DIR)/13-postgres-backup-cronjob.yaml
	@echo "$(BLUE)Waiting for API pods...$(RESET)"
	kubectl wait --for=condition=ready pod -l app=volnux-api \
	  -n $(NAMESPACE) --timeout=120s
	kubectl wait --for=condition=ready pod -l app=volnux-worker \
	  -n $(NAMESPACE) --timeout=120s
	@echo "$(GREEN)Volnux application deployed$(RESET)"

.PHONY: deploy
deploy: k8s-check push k8s-create-namespace k8s-secrets k8s-infra k8s-migrate k8s-app ## Full deployment: build, push, deploy everything
	@echo ""
	@echo "$(GREEN)$(BOLD)Volnux deployment complete!$(RESET)"
	@echo ""
	make k8s-status

.PHONY: k8s-update
k8s-update: push ## Build, push, and rolling-update the image on existing deployment
	@echo "$(BLUE)Updating Volnux to $(VERSION)...$(RESET)"
	kubectl set image deployment/volnux-api \
	  volnux-api=$(IMAGE) -n $(NAMESPACE)
	kubectl set image deployment/volnux-worker \
	  volnux-worker=$(IMAGE) -n $(NAMESPACE)
	@echo "$(BLUE)Watching rollout...$(RESET)"
	kubectl rollout status deployment/volnux-api    -n $(NAMESPACE)
	kubectl rollout status deployment/volnux-worker -n $(NAMESPACE)
	@echo "$(GREEN)Update complete$(RESET)"

.PHONY: k8s-rollback
k8s-rollback: ## Roll back API and worker to previous version
	@echo "$(YELLOW)Rolling back deployments...$(RESET)"
	kubectl rollout undo deployment/volnux-api    -n $(NAMESPACE)
	kubectl rollout undo deployment/volnux-worker -n $(NAMESPACE)
	kubectl rollout status deployment/volnux-api    -n $(NAMESPACE)
	kubectl rollout status deployment/volnux-worker -n $(NAMESPACE)
	@echo "$(GREEN)Rollback complete$(RESET)"

.PHONY: k8s-status
k8s-status: ## Show status of all Volnux pods and services
	@echo "$(BOLD)Pods:$(RESET)"
	kubectl get pods -n $(NAMESPACE) -o wide
	@echo ""
	@echo "$(BOLD)Services:$(RESET)"
	kubectl get svc -n $(NAMESPACE)
	@echo ""
	@echo "$(BOLD)Ingress:$(RESET)"
	kubectl get ingress -n $(NAMESPACE)
	@echo ""
	@echo "$(BOLD)HPA:$(RESET)"
	kubectl get hpa -n $(NAMESPACE)

.PHONY: k8s-logs-api
k8s-logs-api: ## Tail API pod logs
	kubectl logs -f -l app=volnux-api -n $(NAMESPACE) --all-containers --prefix

.PHONY: k8s-logs-worker
k8s-logs-worker: ## Tail worker pod logs
	kubectl logs -f -l app=volnux-worker -n $(NAMESPACE) --all-containers --prefix

.PHONY: k8s-shell-api
k8s-shell-api: ## Open a shell in the API pod
	kubectl exec -it -n $(NAMESPACE) \
	  $$(kubectl get pod -l app=volnux-api -n $(NAMESPACE) -o jsonpath='{.items[0].metadata.name}') \
	  -- /bin/sh

.PHONY: k8s-shell-worker
k8s-shell-worker: ## Open a shell in a worker pod
	kubectl exec -it -n $(NAMESPACE) \
	  $$(kubectl get pod -l app=volnux-worker -n $(NAMESPACE) -o jsonpath='{.items[0].metadata.name}') \
	  -- /bin/sh

.PHONY: k8s-port-forward
k8s-port-forward: ## Port-forward the API to localhost:8080
	@echo "$(GREEN)API forwarded to http://localhost:8080$(RESET)"
	kubectl port-forward svc/volnux-api 8080:8080 -n $(NAMESPACE)

.PHONY: k8s-destroy
k8s-destroy: ## DANGER: Delete all Volnux resources including data
	@echo "$(RED)$(BOLD)WARNING: This will delete ALL Volnux resources and data.$(RESET)"
	@read -p "Type 'destroy' to confirm: " confirm && [ "$$confirm" = "destroy" ]
	kubectl delete namespace $(NAMESPACE)
	@echo "Namespace deleted"

# ══════════════════════════════════════════════════════════════════════════════
# VOLNUX CLI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: triggers
triggers: ## List all triggers via the deployed API
	@VOLNUX_API=$$(kubectl get ingress volnux-api -n $(NAMESPACE) \
	  -o jsonpath='{.spec.rules[0].host}' 2>/dev/null || echo "localhost:8080"); \
	curl -s -H "Authorization: Bearer $${VOLNUX_JWT_TOKEN}" \
	  "https://$${VOLNUX_API}/api/v1/triggers" | python3 -m json.tool

.PHONY: health
health: ## Check health of the deployed API
	@VOLNUX_API=$$(kubectl get ingress volnux-api -n $(NAMESPACE) \
	  -o jsonpath='{.spec.rules[0].host}' 2>/dev/null || echo "localhost:8080"); \
	curl -s "https://$${VOLNUX_API}/api/v1/health" | python3 -m json.tool

.PHONY: hitl-list
hitl-list: ## List pending HITL requests
	@VOLNUX_API=$$(kubectl get ingress volnux-api -n $(NAMESPACE) \
	  -o jsonpath='{.spec.rules[0].host}' 2>/dev/null || echo "localhost:8080"); \
	curl -s -H "Authorization: Bearer $${VOLNUX_JWT_TOKEN}" \
	  "https://$${VOLNUX_API}/api/v1/hitl" | python3 -m json.tool

# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: manifest-generate
manifest-generate: ## Generate volnux.manifest.json from the project
	volnux manifest generate

.PHONY: manifest-validate
manifest-validate: ## Validate volnux.manifest.json
	volnux manifest validate

.PHONY: publish
publish: manifest-validate ## Publish event/workflow packages to EventHub
	volnux publish
