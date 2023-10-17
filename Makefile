.DEFAULT_GOAL := help

.PHONY: help
help: ## This menu
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-35s\033[0m %s\n", $$1, $$2}'

sources = aizedatatechnicaltestcase/ tests/

.PHONY: lint
lint: ## Runs linting tools
	mypy $(sources)
	flake8 $(sources)
	isort --check-only --float-to-top $(sources)
	black -l 120 --skip-string-normalization --target-version py310 --check $(sources) .

.PHONY: format
format: ## Runs formatting tools. This will rewrite source code. This will help make the relevant linting checks pass.
	isort --float-to-top $(sources)
	black -l 120 --skip-string-normalization --target-version py310 $(sources)

.PHONY: test
test: ## Runs all available unit tests (best used locally)
	pytest . -vrA

.PHONY: clean
clean: ## Remove all temporary files and artifacts
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -rf temp
	rm -rf tests/tmp
	rm -rf tests/temp
	rm -rf tests/.pytest_cache
	rm -rf tests/unit_tests/.pytest_cache
	rm -rf tests/end_to_end_tests/.pytest_cache
	rm -rf tests/.pytest_cache
	rm -rf spark-warehouse