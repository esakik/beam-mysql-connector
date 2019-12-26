.PHONY: deploy
deploy: build
	twine upload dist/*

.PHONY: test-deploy
test-deploy: build
	twine upload -r testpypi dist/*

.PHONY: build
build:
	python setup.py sdist