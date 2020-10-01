.FORCE:

docs: .FORCE
	poetry run sphinx-build rst docs -b dirhtml -E -P

check:
	poetry run isort -c e2fyi
	poetry run black --check e2fyi

test: check
	poetry run python test.py

coveralls: test
	poetry run coveralls

serve-docs: docs
	cd docs/  && poetry run python -m http.server 8000

format:
	poetry run isort e2fyi
	poetry run black e2fyi