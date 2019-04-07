install:
	pip install --upgrade .

test:
	pytest --cov-report term --cov-report xml --cov=pachypy/ -vv tests/
	bash helper/upload_codecov.sh

build-docs:
	$(MAKE) -C docs/ html

publish:
	rm -rf dist/
	python3 setup.py sdist bdist_wheel
	python3 -m twine upload dist/*

.PHONY: install test build-docs publish