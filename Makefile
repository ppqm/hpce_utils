
conda=conda
python=python
pip=./env/bin/pip

.PHONY: build check clean test

all: env dev-pip

todo:
	grep "# TODO" */*.py | sed -e 's/    //g' | sed -e 's/# TODO//'

env:
	mamba env create -f environment.yml -p ./env --quiet

dev-pip:
	${pip} install -e .

conda-install-build:
	${conda} install conda-build -c conda-forge -y

setup-pre-commit:
	${python} -m pre_commit install

pre-commit-all:
	${python} -m pre_commit run --all-files

format: pre-commit-all

test: test-unit

test-unit:
	${python} -m pytest --basetemp=".pytest" -vrs tests/

# test-ipynb:
# 	jupytext --output _tmp_script.py notebooks/example_demo.ipynb
# 	${python} _tmp_script.py

clean:
	rm -r build *.pyc __pycache__ _tmp_* *.egg-info

clean_env:
	rm -fr ./env
