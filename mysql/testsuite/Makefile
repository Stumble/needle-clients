NAME := MetaTest
MYSQL_DOCKER_NAME=$(NAME)-mysql
MYSQL_PASSWORD=my-secret
MYSQL_PORT=3306

.PHONY: docker-mysql-start docker-mysql-stop
docker-mysql-start:
	docker run --name $(MYSQL_DOCKER_NAME) --rm -e MYSQL_ROOT_PASSWORD=$(MYSQL_PASSWORD) -e MYSQL_DATABASE=$(MYSQL_DB) -d -p $(MYSQL_PORT):3306 mysql:5.7
	sleep 15 # mysql needs some time to bootstrap.
docker-mysql-stop:
	docker stop $(MYSQL_DOCKER_NAME)

.PHONY: test-start-all-deps test-stop-all-deps
test-start-all-deps: docker-mysql-start
test-stop-all-deps: docker-mysql-stop

.PHONY: test-cmd test-local test
test-cmd:
	export ENV=test && \
	go test ./... -test.v -p 1

test-local: test-start-all-deps
	make test-cmd && (make test-stop-all-deps) || (make test-stop-all-deps; exit 2)

test:
	make test-cmd
