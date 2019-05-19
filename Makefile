install:
	pip install --upgrade .

test-all:
	pytest --cov-report term --cov-report xml --cov=pachypy/ -vv tests/
	bash helper/upload_codecov.sh

test-unit:
	pytest -m "not integtest" -vv tests/

test-integ:
	pytest -m integtest -vv tests/

build-docs:
	$(MAKE) -C docs/ html

publish:
	rm -rf dist/
	python3 setup.py sdist bdist_wheel
	python3 -m twine upload dist/*

.PHONY: install test build-docs publish
