
# Docs Generation for HSP

The documentation is set up using [Jupyter Book](https://jupyterbook.org/). You can build the documentation locally under the `docs` directory

```
pip install -r requirements.txt
jupyter-book build .
```
This will generate a `_build` directory, where you can inspect the pages by opening `_build/html/index.html`. The version of this documentation is mirroring the `master` branch, and can be found [here](https://hylleraasplatform.gitlab.io/hylleraas/).

## How to change the documentation

Preferred workflow is to
 - create a new branch and push this to origin
 - update the files within this directory
 - build and inspect them locally
 - make sure the pre-commit step runs locally
```
pre-commit run --all-files
```
 - commit your changes
 - make a merge request on [our gitlab pages](https://gitlab.com/hylleraasplatform/hylleraas)

When the merge request is accepted, the changes will automatically be deployed.

## Integration into gitlab's CI/CD

Building the docs is currently triggered by the gitlab´s CI/CD pipeline, by including the following::

```
docs:
  image: python:3.10
  interruptible: true
  script:
    - pip install -r docs/requirements.txt
    - cd docs ; jupyter-book build .
    - mv _build/html/ ../public/
  artifacts:
    paths:
      - public
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
```

in `.gitlab-ci.yml`.
