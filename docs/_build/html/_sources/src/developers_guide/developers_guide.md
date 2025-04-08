# Developers Guide

Hylleraas Software Platform (HSP) consists of a core module and several independent modules/repositories. In particular, interfaces to external sofware, special tools and independent modules are separated from the core.

<!--![Structure of the HSP](hsp_schematics.png)-->

In general, HSP consists of a main package including meta-functionality (consisting of the core, tools and interfaces) and a set of standalone packages (modules) combining the functionality needed at the Hylleraas Centre. For more information on the individual parts of HSP, see below.

Since the HSP should be able to work with dozens of codes, it is imperative to design a new application as independent as possible while minimizing redundancies.

> For example, [hygo](<https://doi.org/10.1063/1.1673621>) is a standalone module for performing geometry optimizations, installed automatically when installing HSP. As molecule input, hygo takes simple arrays. It thus does not have modules for, e.g. reading ad converting `.xyz` or `.pdb` files to these arrays. However, this functionality is on the main platform and thus can (but not has to) be used by hygo. This way, this (meta-)functionality is available for all packages developed under the HSP while the package itself might be use independent of HSP.

In general:

*	create easy to maintain *and* user friendly software
* follow SOLID principle
* refactor as much as possible and create unit tests

## Style guidelines

All developers that are contributing to the Hylleraas Software Platform should use [pre-commit](<https://pre-commit.com>) to check there code *before* committing (see also [here](<https://daltonproject.readthedocs.io/en/latest/installation.html#developers>)). To this end, the devs should make sure that they have the current version of `.pre-commit-config.yaml` in the repository. The code can then conviently be checked by calling
```bash
$> pre-commit run --all-files
```

In general, the output of this command will help to fix the problems found. When all hooks are passed, the code is ready for git commit (and push).

### Handling exceptions

<!--In general, it is advised to use [built-in python exceptions](<https://docs.python.org/3/library/exceptions.html>). As starting point, see this [blog article](<https://eli.thegreenplace.net/2008/08/21/robust-exception-handling/>) and this [tutorial](<https://www.tutorialsteacher.com/python/error-types-in-python>).-->

<!--A major point of error handling is carbage connection.

The two major points for error handling on the HSP are

1. description
2. garbage connection
-->

In HSP, we try to catch errors at low levels using custom error types in order to not confuse the user with program-specific errors. In this way, e.g., input errors can be uncovered more easily. Exceptions, i.e.,  events that occur rarely, should be handeled with ``try...except...else..finally`` clauses to ensure readability and garbage collection. Otherwise, ``if...elif...else`` constructions can be used together with raised exceptions or in a monadic manner where the methods in a program process and return objects indicating the success of the tasks.

### Python style guide checklist

1. follow SOLID principle
1. use descriptive variable names
1. catch errors at low levels with descriptive error messages
1. use appropriate data classes


## Testing

All functionalities added to HSP should be tested. All tests should be located in a `tests` directory having the following structure:
```
tests
├── __init__.py
├── conftest.py
├── data
│   ├── dummy.txt
│   └── water.xyz
├── requirements.txt
└── test_molecule.py
```

Tests of different modules are placed in seperated files with prefix `test_`.
[Fixtures](<https://docs.pytest.org/en/stable/explanation/fixtures.html#about-fixtures>) used by several tests should be striped down as far as possible and placed into `conftest.py`, external data needed can be placed in side the `data`folder. The (empty) file `__init__.py` is used by gitlab-CI to compute coverage correctly. The developers should make sure that the requirements.txt file is up to date with the version of the main repository.

The test suite can then invoked by
```bash
$> pytest -sv --cov-report term-missing --cov=SOURCEDIR  tests/
```
where `SOURCEDIR` is the location of the source code. The output will also display the coverage of tests together with the line numbers of missing statements. We aim for a [relatively high](<https://martinfowler.com/bliki/TestCoverage.html>) coverage.


>Note: it is generally positive if the test has much more line than the actual code.


>Note: In general high coverage, fast testing, small datasets are preferred.

### Local testing of gitlab-CI

Follow the install instructions on [gitlab-CI](<https://gitlab.com/hylleraasplatform/hylleraas/-/settings/ci_cd>) (remember to copy the token). If you want to use a docker image, remember to register it during the installing process. After install, go to the hylleraas directory and run `gitlab-runner exec shell test` for running the test stage of the gitlab-CI locally, very useful for debugging.


## Documentation

All parts of the HSP should be documented. Currently, [sphinx](<https://www.sphinx-doc.org/en/master/>) is used for generating the html files including API reference. A typical documentation folder looks like this
```
docs
├── Makefile
├── requirements.txt
└── source
    ├── conf.py
    ├── index.rst
    ├── installation.rst
    ├── tutorials
    │   ├── ex1.rst
    │   ├── ex2.rst
    │   ├── ex3.rst
    │   └── troubleshooting.rst
    └── tutorials.rst
```
where `Makefile`, `requirements.txt` and `source/conf.py` should be sunchronized with the HSP main documentation. The file `source/index.rst` file should always be present (if not otherwise indicated in `source/conf.py`) and in [reStructuredText](<https://docutils.sourceforge.io/rst.html>) format. All other files can be written in a simpler [Markdown](<https://daringfireball.net/projects/markdown/>) language or directly in html.

The generation of the documentation will be checked through the gitlab-CI. A local version can be build inside the `docs` directory using
```bash
$> make html
```
and then inspected by opening `docs/build/html/index.html` in a browser of choice.

### API documentation

Developers are expected to add in-code docstrings follwing the [PEP 257](<https://peps.python.org/pep-0257/>) guidlines in [numpy-style](<https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html>). This will be enforced during the `lint` check as part of the gitlab-CI/CD using [pydocstyle](<http://www.pydocstyle.org/en/stable/>). A simple example is:
```python
def gen_molecule(molecule_str: str):
    """Generate molecule object for LONDON.

    Parameters
    ----------
    molecule_str : str
        string containing coordinates (in bohr) of molecule of .xyz-type.

    Returns
    -------
    list
        atoms
    np.array
        coordinates (in angstrom)

    """
```
## Package management / Dependencies

Naturally, python applications will require to use functionalities from other packages.
However, in view of the diversity of applications present in HSP, it is advisable to minimise the number of packages to-be installed with the hylleraas packages.
To achieve this, we will work with different flavours of packages.
In the beginning there will be only two, 'core' and 'full'.

### Find all dependencies

Create a fresh virtual environment, e.g. using conda: ``conda create -n python3.8 python=3.8``.
Since for this package python >= 3.8 should be supported (as of September 2023), this version was chosen as reference.
Install and try to import the package in the environment to find packages that are not installed (obsolete packages should be found by ``pre-commit``), e.g.
``conda activate python3.8`` and
``python -m pip install . ; python -c 'import hyset'``.

Use, e.g. [pipdeptree] (https://github.com/tox-dev/pipdeptree) to identify the packages that have been installed in this environment:

```bash
(python3.8) foobar@machine hyset% pipdeptree --python $(which python)
hyset==0.1
├── fabric [required: Any, installed: 3.2.2]
│   ├── decorator [required: >=5, installed: 5.1.1]
│   ├── Deprecated [required: >=1.2, installed: 1.2.14]
│   │   └── wrapt [required: >=1.10,<2, installed: 1.15.0]
│   ├── invoke [required: >=2.0, installed: 2.2.0]
│   └── paramiko [required: >=2.4, installed: 3.3.1]
│       ├── bcrypt [required: >=3.2, installed: 4.0.1]
│       ├── cryptography [required: >=3.3, installed: 41.0.3]
│       │   └── cffi [required: >=1.12, installed: 1.15.1]
│       │       └── pycparser [required: Any, installed: 2.21]
│       └── PyNaCl [required: >=1.5, installed: 1.5.0]
│           └── cffi [required: >=1.4.1, installed: 1.15.1]
│               └── pycparser [required: Any, installed: 2.21]
├── nest-asyncio [required: Any, installed: 1.5.8]
├── paramiko [required: Any, installed: 3.3.1]
│   ├── bcrypt [required: >=3.2, installed: 4.0.1]
│   ├── cryptography [required: >=3.3, installed: 41.0.3]
│   │   └── cffi [required: >=1.12, installed: 1.15.1]
│   │       └── pycparser [required: Any, installed: 2.21]
│   └── PyNaCl [required: >=1.5, installed: 1.5.0]
│       └── cffi [required: >=1.4.1, installed: 1.15.1]
│           └── pycparser [required: Any, installed: 2.21]
├── scp [required: Any, installed: 0.14.5]
│   └── paramiko [required: Any, installed: 3.3.1]
│       ├── bcrypt [required: >=3.2, installed: 4.0.1]
│       ├── cryptography [required: >=3.3, installed: 41.0.3]
│       │   └── cffi [required: >=1.12, installed: 1.15.1]
│       │       └── pycparser [required: Any, installed: 2.21]
│       └── PyNaCl [required: >=1.5, installed: 1.5.0]
│           └── cffi [required: >=1.4.1, installed: 1.15.1]
│               └── pycparser [required: Any, installed: 2.21]
└── tqdm [required: Any, installed: 4.66.1]
pip==23.2.1
setuptools==68.0.0
wheel==0.38.4
```
As we can see, both ``fabric`` and  ``scp`` depend on compatible versions of ``paramiko`` (``Any`` and ``>=2.4``), so we could in principle remove this dependency from the installation.
Even more comfortable is to use [pipreqs](https://github.com/bndr/pipreqs) to get a list of dependencies:

```bash
(python3.8) foobar@machine hyset% pipreqs . --use-local --ignore tests/,build/ --print --mode gt
Please, verify manually the final list of requirements.txt to avoid possible dependency confusions.
Fabric>=3.2.2
nest_asyncio>=1.5.5
scp>=0.14.5
tqdm>=4.62.2
INFO: Successfully output requirements
```

### Identifying core dependencies

Dependencies that are not required in ``core`` maybe removed usind a ``try...except`` construct, e.g.:

```python
try:
    import nest_asyncio
except ImportError:
    nest_asyncio = None
...
            if nest_asyncio is None:
                raise ImportError('nest_asyncio must be installed to run ' +
                                  'concurrently in an event loop')
            nest_asyncio.apply()
```

### Cross-checking core dependencies

Packages like ``numpy`` are used by several packages. In order to find the minimal required version, one could use the reverse search of [pipdeptree] (https://github.com/tox-dev/pipdeptree):

```bash
(python3.8) foobar@machine hyset % pipdeptree --reverse --packages numpy
numpy==1.23.5
├── ase==3.22.1 [requires: numpy>=1.15.0]
│   └── pycp2k==0.2.2 [requires: ase]
│       └── hyif==0.1 [requires: pycp2k]
│           └── hylleraas==0.0.1 [requires: hyif]
├── autograd==1.4 [requires: numpy>=1.12]
│   └── hylleraas==0.0.1 [requires: autograd]
├── daltonproject==0.1a0 [requires: numpy>=1]
│   └── hylleraas==0.0.1 [requires: daltonproject]
├── deepmd-kit==2.2.1 [requires: numpy]
├── dpdata==0.2.13 [requires: numpy>=1.14.3]
├── geometric==0.9.7.2 [requires: numpy>=1.11]
...
```

This leaves us with ``numpy>=1.15.0`` as minimal requirement.
__Note:__ be aware of conflicting dependencies, e.g. ``[required: >=0.2.6,<2.0.0, installed: 2.0.1]`` and consider refactoring.

### Migration to ``pyproject.toml``

An easy starting point to migrate from the old ``setup.py``/``setup.cfg`` structure to
using ``pyproject.toml`` is provided by [ini2toml](https://ini2toml.readthedocs.io/en/latest/readme.html):

```python
from ini2toml.api import Translator
profile_name = "setup.cfg"

with open(profile_name, 'rt') as f :
    original_contents_str = f.read()
toml_str = Translator().translate(original_contents_str, profile_name)
print(toml_str)
```
### Integrate flavours in ``pyproject.toml``

Assume we want to create the three flavours ``core``, ``async`` and ``full`` for ``hyset``.

* ``core`` should be the default installation wich packages installed via ``requirements.txt``
* ``async`` should be ``core`` plus ``nest_asyncio>=1.5.5``
* ``full`` should be ``core`` plus ``async``

The following files implement this behaviour:

```python
(python3.9) foobar@machine hyset % cat pyproject.toml
[build-system]
requires = [ "setuptools>=61.2",]
build-backend = "setuptools.build_meta"

[project]
name = "hyset"
version = "0.1"
description = "computational settings for hylleraas software platform"
readme = "README.md"
requires-python = ">=3.8"
dynamic = ["dependencies"]

[project.optional-dependencies]
core = []
async = ["nest_asyncio>=1.5.5"]
full = ["hyset[core, async]"]

[project.license]
text = "LICENSE"

[project.scripts]
executable-name = "package.module:function"

[tool.setuptools]
zip-safe = true
include-package-data = true

[tool.setuptools.dynamic]
dependencies = { file = ["requirements.txt"] }

[tool.settings]
known_third_party = ["fabric", "hyobj", "numpy", "paramiko", "scp", "tqdm"]

[tool.setuptools.package-data]
"*" = [ "*.txt", "*.rst",]
hello = [ "*.msg",]
````

```bash
(python3.9) foobar@machine hyset % cat requirements.txt
Fabric>=3.2.2
scp>=0.14.5
tqdm>=4.62.2
```

### Double-check installation

In a fresh environment (see [here](#find-all-dependencies)) try yo install the flavours:

```bash
python -m pip install .
python -m pip freeze | xargs python -m pip uninstall -y
python -m pip install '.[core]'
python -m pip freeze | xargs python -m pip uninstall -y
python -m pip install '.[async]'
python -m pip freeze | xargs python -m pip uninstall -y
python -m pip install '.[full]'
```

## Interfaces

Interfaces connect external sofware to the core objects of HSP, e.g., ``Molecule`` and ``Method``. They provide access to basic methods associated with ``Method``, like computation of energy of molecule or train a dataset.

 `MethodLike` objects consist of a collection of information about the computation in json-style dictionary format as well as the methods that are defined in the interface. That is, all methods defined in the interface will be bound to a ``Method`` instance. Note, that some Modules, e.g. the geometry optimizer hygo expect specific methods (in this particular case, ``get_energy()`` and ``get_gradient()``.
 During instantiation, ``Method`` will pass a ``ComputationSettings`` object to the interface which should be used throughout.

The ``Interface`` class provides two kinds of methods.

1. Methods (functions) bound to the class **object**, i.e. these methods will be called *before* a class instance is initiated. Examples are ``get_input_molecule()`` and ``get_input_method()``. These routines are used by the core objects ``Molecule`` and ``Method``, respectively to read the input (mostly of legacy code) from the corresponding input file(s). In case of ``get_input_molecule()``, the ``Molecule`` class needs this to instantiate a Molecule. For ``get_input_method()``, the input is read and processes and *then*, an ``Interface`` class instance is created and the corresponding methods are bound to the ``Method`` class instance. Since these methods are needed before instantiation of the ``Interface`` class, thus independent on class variables but belonging to the class, these are decorated with ``@classmethod``.

	> Note: ``get_input_molecule()`` and ``get_input_method()`` represent the interfaces between the program input and the python kernel. Therefore, they are mostly needed for legacy (i/o driven) code and obsolete for programs providing a python interface.

2. Methods (functions) bound to the class **instance**, e.g. ``get_energy()``. These methods could make use of instance variables, e.g. a ``self.molecule`` provided by ``__init__()``. However, in most cases, it makes sense to separate the method from the Molecule, i.e. one should be able to call ``my_method.get_energy() ``with *different* molecules without creating a new ``my_method`` ``Method`` class instance. This means that for most applications, the ``Interface`` class would *not* need a molecule as input (this might be different for other applications where the method explicitly depends on the computable object, e.g. for a super-interface (or meta-interface) which redirects to a particular interface depending on the kind of computable object).
	>Note: this construction is also particularly important for mixing different Method class instance methods.


### Must-haves

All interfaces must implement the following routines.

- ``__init__()``: initialize interface with ``ComputeSettings``etc.
- ``setup()``: set up a calculation, must return a ``RunSettings`` object
- ``parser()``: parse results, takes a ``ComputeResult`` object and returns a ``dict``.

`

### Interface checklist

- [ ] use Runner from ``compute_settings`` for kernel and userland interactions
- [ ] work with absolute paths using the ``pathlib`` package
- [ ] provide (and test) parsers for in- and output
- [ ] make use of abstract base classes
- [ ] provide complete docstrings
- [ ] take care of units
- [ ] provide a version check
- [ ] provide proper error handling
- [ ] provide a testing environment, e.g. dockerfile if necessary



<!-- **For most quantum chemical codes, one can summarize:**

* if the program is i/o driven, provide the ``classmethods``  ``get_input_method()`` and, if applicable, ``get_input_molecule()``.
* if the method is not explicitly dependent on the molecule, don’t use it for instantiation, instead use something like following:

  ```python
  class MyInterface:
      def __init__(self, method):
          ...

      @classmethod
      def gen_molecule(cls, atoms, coordinates):
          ...
          return

      def get_energy(self, molecule):
          mymol = MyInterface.gen_molecule(molecule.atoms, molecule.coordinates)
          ...
          return energy
  ```

  In this example, the function ``get_energy()`` will take a molecule object with attributes ``atoms`` and ``coordinates`` (this is standard) and calls ``gen_molecule()`` to generate the molecule ``mymol`` as needed by the external program: this might be a special object, a string for an input file, etc. With this molecule, an input is created, the energy is computed and finally returned.

  > Note: ``gen_molecule()`` is *not* the same as ``gen_input_molecule()``: The latter *reads* a molecular input, while the first *creates* or *updates* a representation of the molecule using input atoms and coordinates -->


<!--An example interface to the program 'myprog', stored in `./hylleraas/intefaces/myprogram.py` could look like this (remember though to inclue [docstrings](#api-documentation)):

	class MyProgram:

		def __init__(self):
			...
			self.molecule = ...
			...

		@classmethod
		def get_input_molecule(cls, *args):
			atoms = ...
			coordinates = ...
			return atoms, coordinates

		@classmethod
		def get_input_method(cls, *args):
			dict = ...
			return dict

		def restart(self, *args, **kwargs):
			...

		def get_energy(self, *args):
			...
			return energy

		def update_coordinates(self, coordinates):
			"""Update coordiantes on interface side."""
			self.molecule.coordinates = coordinates.ravel().reshape(-1,3)
			...
-->
<!-- * to test a newly, generated interface, one can explicitely use it by ``Method(..., interface=MyInterface)``
* in order to make your interface available for HSP, add the corresponding mapping in ``./hylleraas/interfaces/__init__.py``:
  ```python
  from .myinterface import MyInterface
  PROGRAMS.update({'myprog': 'MyInterface'})
  ```

> Note: Even though there is a very rudamentray generic restart function in the major Interface class, it is highly advised to create a custom one in the interface. -->

### Example


```python
from ....manager import NewRun  # implements self.run() and self.arun()
from ....utils import unique_filename
from ...abc import HylleraasQMInterface
from .output_parser import Parser

class MyIf(HylleraasQMInterface, NewRun)


    def __init__(self,
                 method: dict,
                 compute_settings: Optional[ComputeSettings] = None):

        # set default ComputeSettings to 'local'
        if compute_settings is None:
            self.compute_settings = create_compute_settings()
        else:
            self.compute_settings = compute_settings


        self.version: str = (self.check_version() if method.pop(
            'check_version', True) else None)

        self.OutputParser = Parser

    # process lists
    @run_sequentially
    def setup(self, molecule: MoleculeLike, **kwargs) -> RunSettings:
        """Set up the calculation.

        Parameters
        ----------
        molecule : MoleculeLike
            Molecule to be used in Calculation.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        RunSettings
            RunSettings object.

        """
        run_settings = deepcopy(self.run_settings)
        if 'run_opt' in kwargs.keys():
            run_settings = replace(run_settings, **kwargs.pop('run_opt'))

        if molecule is None:
            return replace(run_settings, **setup_dict)


        setup_dict: Dict[str, Any] = {'program': 'myprog'}

        input_str = kwargs.get('program_opt', {}).get('input', '')

        mol_str = self.gen_system_str_from_molecule(molecule)
        mol_filename = unique_filename(mol_str.split('\n')) + '.xyz'

        # Note: File handling will be simplified soon:
        #
        # file_handler = FileHandler(run_settings)
        # mol_file = file_handler.file(name=mol_filename, content=mol_str)
        # files_to_write = [mol_file]

        mol_file = run_settings.abs_path(mol_filename)
        files_to_write = [(mol_file, mol_str)]

        input_file = unique_filename(input_str.split('\n')) + '.inp'
        input_file = run_settings.abs_path(input_file)
        output_file = input_file.with_suffix('.out')

        files_to_write.append((input_file, input_str))

        setup_dict.update({
            'program': program,
            'files_to_write': files_to_write,
            'output_file': output_file,
            'args': ['-i', str(input_file)],
        })

        return replace(run_settings, **setup_dict)

    @run_sequentially
    def parse(self, output: ComputeResult, **kwargs) -> dict:
        """Parse MyIf output.

        Parameter
        ---------
        output: `:obj:ComputeResult`
            result of the computation
        kwargs: dict
            additional keyword arguments

        Returns
        -------
        dict
            parsed results

        """
        results: dict = {}
        if output.output_file:
            results.update(self.OutputParser.parse(output.output_file))
        if output.stdout:
            results.update(self.OutputParser.parse_stdout(output.stdout))
        if output.stderr:
            # raise RuntimeError(output.stderr)
            # warnings.warn(output.stderr)
            results['stderr'] = output.stderr

        return results

    @run_sequentially
    @convert_units('energy')
    def get_energy(self, molecule: MoleculeLike, **kwargs):
        """Compute energy."""
        result = self.run(molecule, **kwargs)
        return result['energy']

```
