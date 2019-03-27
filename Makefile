install:
	pip install --upgrade .

test:
	pytest --cov-report term --cov-report xml --cov=pachypy/ -vv tests/
	bash helper/upload_codecov.sh

build-docs:
	$(MAKE) -C docs/ html

.PHONY: install test build-docs