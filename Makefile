.PHONY: help build up down restart logs logs-coordinator logs-dashboard test clean test-leader test-etcd test-transaction metrics health run-txns status kill-leader start-leader kill-participant2 start-participant2

help:
	@echo "3PC Project Commands"
	@echo "================================"
	@echo ""
	@echo "Build & Deploy:"
	@echo "  make build              Build all Docker images"
	@echo "  make up                 Start all services"
	@echo "  make down               Stop all services"
	@echo "  make restart            Restart all services (down + up)"
	@echo "  make status             Show status of all containers"
	@echo "  make kill-leader        Stop coordinator-1 to trigger leader re-election"
	@echo "  make start-leader       Start coordinator-1 and rejoin the election"
	@echo "  make kill-participant2  Stop participant2 to simulate node failure"
	@echo "  make start-participant2 Start participant2 after it was stopped"
	@echo ""
	@echo "Logs:"
	@echo "  make logs               Show all service logs (live)"
	@echo "  make logs-coordinator   Show coordinator logs (live)"
	@echo "  make logs-dashboard     Show dashboard logs (live)"
	@echo ""
	@echo "Testing & Verification:"
	@echo "  make test               Run Python unit tests"
	@echo "  make health             Full system health check"
	@echo "  make test-leader        Test leader election status"
	@echo "  make test-etcd          Check etcd lock holder"
	@echo "  make test-transaction   Run a single 3PC transaction"
	@echo "  make metrics            Show current dashboard metrics"
	@echo "  make run-txns           Run 10 test transactions"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean              Stop services, remove volumes & images"
	@echo ""

# ============================================================================
# BUILD & DEPLOY 
# ============================================================================

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "⏳ Waiting 25 seconds for services to be healthy..."
	sleep 25
	@echo " Services started!"

down:
	docker-compose down

restart: down up
	@echo "Restart complete"

status:
	docker-compose ps

kill-leader:
	@echo "Stopping coordinator-1 (leader)..."
	docker stop 3pc-coordinator
	@echo "coordinator-1 stopped. A new leader should be elected within ~10s."

start-leader:
	@echo "Starting coordinator-1..."
	docker start 3pc-coordinator
	@echo "coordinator-1 started. It will rejoin the election automatically."

kill-participant2:
	@echo "Stopping participant2..."
	docker stop 3pc-participant2
	@echo "participant2 stopped."

start-participant2:
	@echo "Starting participant2..."
	docker start 3pc-participant2
	@echo "participant2 started."

# ============================================================================
# LOGS 
# ============================================================================

logs:
	docker-compose logs -f

logs-coordinator:
	docker-compose logs -f coordinator

logs-dashboard:
	docker-compose logs -f dashboard

# ============================================================================
# TESTING 
# ============================================================================

test:
	PYTHONPATH=. python3 tests/test_state.py
	PYTHONPATH=. python3 tests/test_participant_state.py

test-leader:
	@echo "Testing leader election status..."
	@echo "  coordinator-1 (port 5000):"
	@curl -sf http://localhost:5000/leader-status | python3 -m json.tool 2>/dev/null || echo "    not responding"
	@echo "  coordinator-2 (port 5010):"
	@curl -sf http://localhost:5010/leader-status | python3 -m json.tool 2>/dev/null || echo "    not responding"
	@echo "  coordinator-3 (port 5020):"
	@curl -sf http://localhost:5020/leader-status | python3 -m json.tool 2>/dev/null || echo "    not responding"
	@echo ""

test-etcd:
	@echo "Checking who holds the etcd leader lock..."
	@docker exec 3pc-etcd etcdctl --endpoints=http://localhost:2379 get /3pc/leader 2>/dev/null || echo "⚠️  etcd not running"
	@echo ""

# test-transaction:
# 	@echo "Sending 1 test transaction..."
# 	@curl -s -X POST http://localhost:5000/execute-transaction \
# 		-H "Content-Type: application/json" \
# 		-d '{"participants": ["http://localhost:5011", "http://localhost:5012", "http://localhost:5013"]}' \
# 		| python3 -m json.tool
# 	@echo ""
test-transaction:
	@echo "Sending 1 test transaction..."
	@curl -s -X POST http://localhost:5000/execute-transaction \
		-H "Content-Type: application/json" \
		-d '{"participants": ["http://toxiproxy-server:5011", "http://toxiproxy-server:5012", "http://toxiproxy-server:5013"]}' \
		| python3 -m json.tool
	@echo ""
metrics:
	@echo " Current metrics from dashboard:"
	@curl -s http://localhost:8000/api/metrics | python3 -c "import sys, json; d=json.load(sys.stdin); print(f'Total: {d[\"transactions\"][\"total\"]}, Committed: {d[\"transactions\"][\"committed\"]}, Success Rate: {d[\"transactions\"][\"commit_rate\"]}%')" 2>/dev/null || echo "⚠️  Dashboard not responding"
	@echo ""

# run-txns:
# 	@echo " Running 10 transactions..."
# 	@for i in {1..10}; do \
# 		curl -s -X POST http://localhost:5000/execute-transaction \
# 			-H "Content-Type: application/json" \
# 			-d '{"participants": ["http://localhost:5011", "http://localhost:5012", "http://localhost:5013"]}' \
# 			| python3 -m json.tool; \
# 		echo "✓ Transaction $$i"; \
# 		sleep 0.3; \
# 	done
# 	@echo ""
# 	@echo " All 10 transactions completed!"
# 	@echo "Check dashboard: http://localhost:8000"
# 	@echo ""
run-txns:
	@echo " Running 10 transactions..."
	@for i in {1..10}; do \
		curl -s -X POST http://localhost:5000/execute-transaction \
			-H "Content-Type: application/json" \
			-d '{"participants": ["http://toxiproxy-server:5011", "http://toxiproxy-server:5012", "http://toxiproxy-server:5013"]}' \
			| python3 -m json.tool; \
		echo "✓ Transaction $$i"; \
		sleep 0.3; \
	done
	@echo ""
	@echo " All 10 transactions completed!"
	@echo "Check dashboard: http://localhost:8000"
	@echo ""

health:
	@echo " FULL SYSTEM HEALTH CHECK"
	@echo "======================================"
	@echo ""
	@echo "1Docker Containers:"
	@docker-compose ps
	@echo ""
	@echo "2etcd Status:"
	@docker exec 3pc-etcd etcdctl --endpoints=http://localhost:2379 endpoint health 2>/dev/null || echo "⚠️  etcd unreachable"
	@echo ""
	@echo "3Leader Status:"
	@curl -s http://localhost:5000/leader-status | python3 -m json.tool 2>/dev/null || echo "⚠️  Coordinator unreachable"
	@echo ""
	@echo "Leader Lock (etcd):"
	@docker exec 3pc-etcd etcdctl --endpoints=http://localhost:2379 get /3pc/leader 2>/dev/null || echo "⚠️  etcd unreachable"
	@echo ""
	@echo "Dashboard Metrics:"
	@curl -s http://localhost:8000/api/metrics | python3 -c "import sys, json; d=json.load(sys.stdin); print(f'Transactions: {d[\"transactions\"][\"total\"]}, Committed: {d[\"transactions\"][\"committed\"]}, Rate: {d[\"transactions\"][\"commit_rate\"]}%')" 2>/dev/null || echo "⚠️  Dashboard unreachable"
	@echo ""
	@echo "======================================"
	@echo "Health check complete!"
	@echo ""

# ============================================================================
# CLEANUP (Your Existing Command)
# ============================================================================

clean:
	docker-compose down -v --rmi all --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	rm -f data/*.db

.DEFAULT_GOAL := help