# Celus Nibbler

Counter-like data reader and processor.

## Developer Docs

### Build

Poetry is used to manage python dependencies.

- [install](https://python-poetry.org/docs/#installation) poetry

- create venv
`python -m venv venv`

`source venv/bin/activate`

- to install all packages and dependecies run:
`poetry install`
    - if `poetry.lock` file has been already created previously remove this file and run the command again


- to install pre-commits (such as black, isort, etc.) run:
`pre-commit install`

### Pre-commit deployment
To pass the basic lints you may want to install pre-push hook to
pre-commit to be sure that CI won't fail in the first step.
```bash
poetry run pre-commit install -t pre-push
```

# License

Proprietary
