install:
	pip install --upgrade .

test:
	pytest --cov-report term --cov-report xml --cov=pachypy/ -vv tests/
	rm -f coverage.svg
	coverage-badge -o coverage.svg

build-docs:
	$(MAKE) -C docs/ html

.PHONY: install test build-docs