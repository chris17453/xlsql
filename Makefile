# Makefile for packaging and publishing the project

.PHONY: clean build publish test

# Clean up build artifacts
clean:
	rm -rf build dist *.egg-info

# Build the source and wheel distribution
build: clean
	python setup.py sdist bdist_wheel

# Publish to TestPyPI (for testing uploads)
publish-test: build
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

# Publish to official PyPI
publish: build
	twine upload dist/*

# Run tests (if any tests exist)
test:
	pytest
