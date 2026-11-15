.PHONY: up down logs test test-local scale fmt clean

up:            ## Build and start API, worker, MongoDB and Redis
	docker compose up --build -d

down:          ## Stop everything (keeps the MongoDB volume)
	docker compose down

clean:         ## Stop everything and delete data volumes
	docker compose down -v

logs:          ## Follow API and worker logs
	docker compose logs -f api worker

scale:         ## Run three worker replicas
	docker compose up -d --scale worker=3

test:          ## Run the test suite in a container against real MongoDB + Redis
	docker compose --profile test run --rm --build tests

test-local:    ## Run tests on the host (needs MongoDB + Redis on localhost)
	pytest -q
