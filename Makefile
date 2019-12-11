dev:
	pipenv install --dev
	pipenv run pip install -e .

dists: requirements sdist bdist wheels

.FORCE:

docs: .FORCE
	sphinx-build rst docs -b dirhtml -E -P

requirements:
	pipenv run pipenv_to_requirements

sdist: requirements
	pipenv run python setup.py sdist

bdist: requirements
	pipenv run python setup.py bdist

wheels: requirements
	pipenv run python setup.py bdist_wheel

