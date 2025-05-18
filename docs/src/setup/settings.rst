********
Overview
********

.. mermaid::

    %%{
      init: {
        'theme': 'base',
        'themeVariables': {
          'primaryColor': '#2b38ff',
          'primaryTextColor': '#fff',
          'primaryBorderColor': '#17d9ff',
          'lineColor': '#17d9ff',
          'secondaryColor': '#17d9ff',
          'tertiaryColor': '#fff'
        }
      }
    }%%

    classDiagram
        RunSettings --|> ComputeSettings
        RunSettings --|> Interface : setup()

        namespace hyset {
          class ComputeSettings{
            +str scheduler
            +int ntasks
            +int cpus_per_task
            +int memory_per_cpu
            ...
          }
          class RunSettings{
            +str program
            +list args
            +str output_file
            ...
            run()
          }
        }
        namespace hyif {
          class Interface{
            ...
            +setup()
            +run()
            +parse()
          }
        }


`ComputeSettings` is a dataclass containing the general settings for a hsp
calculation, e.g. generated from
`hyset.create_compute_settings()`. `RunSettings` inherits from `ComputeSettings`
and gets updated using the `setup()` function from the program interface,
e.g., the interface sets the program name and the arguments to be passed to the
program.
The `RunSettings` instance is the input for running the calculation.



.. u
.. wrapper `run()` function from the interface which performs the three steps: setup,
.. run and parsing. The `setup()` function defined in the interface is used to set

..  The `run()` function is a wrapper for
.. performing the three steps: setup, run and parsing. The `setup()` function
.. defined in the interface is used to set up the calculation, e.g., setting the
.. program name and the arguments to be passed to the program. The `RunSettings`
.. instance is

.. The `run()` function defined in the Interface is a wrapper for performing the
.. three steps setup, run and parsing.


******************
ComputeSettings
******************

**Date**: |today|

As of today, this is the following class diagram for the
`ComputeSettings` classes in `hyset`:


.. mermaid::

    %%{
      init: {
        'theme': 'base',
        'themeVariables': {
          'primaryColor': '#2b38ff',
          'primaryTextColor': '#fff',
          'primaryBorderColor': '#17d9ff',
          'lineColor': '#17d9ff',
          'secondaryColor': '#17d9ff',
          'tertiaryColor': '#fff'
        }
      }
    }%%

    classDiagram

        ComputeSettings <|-- ComputeSettingsBase
        ComputeSettingsBase <|-- LocalArch
        ComputeSettingsBase <|-- RemoteArch
        note for ComputeSettingsBase "composition"

        class ComputeSettings{
        <<Abstract>>
        }

        class ComputeSettingsBase{
          +str logger
          +str print_level
          ....
          +bool force_recompute
          ....
          +str scheduler
          ...
          +int ntasks
          +int cpus_per_task
          +int memory_per_cpu
          ...
          +str database
          ...
          +str work_dir_local
        }

        class LocalArch{
        #str arch_type
        run()
        }

        class RemoteArch{
          #str arch_type
          +str host
          +str user
          +int port
          ...
          +str work_dir_remote
          run()
        }


This structure is necessary because in hyset, `LocalArch` and `RemoteArch`
have their respective runners which are getting replaced by `hyrun.run()`.
However, the `ComputeSettings` class can be refactored to give
(currently status in branch `use_rsync_multiple`):

.. mermaid::

    %%{
      init: {
        'theme': 'base',
        'themeVariables': {
          'primaryColor': '#2b38ff',
          'primaryTextColor': '#fff',
          'primaryBorderColor': '#17d9ff',
          'lineColor': '#17d9ff',
          'secondaryColor': '#17d9ff',
          'tertiaryColor': '#fff'
        }
      }
    }%%

    classDiagram

        ComputeSettings <|-- ComputeSettingsGeneral
        ComputeSettings <|-- ComputeSettingsResources
        ComputeSettings <|-- ComputeSettingsEnvironment
        ComputeSettings <|-- ComputeSettingsLogger
        ComputeSettings <|-- ComputeSettingsDirectories
        ComputeSettings <|-- ComputeSettingsDatabase
        ComputeSettings <|-- ComputeSettingsScheduler
        ComputeSettings <|-- ComputeSettingsConnection
        ComputeSettings <|-- ComputeSettingsConda
        ComputeSettings <|-- ComputeSettingsContainer
        ComputeSettings <|-- ComputeSettingsProgram
        ComputeSettings <|-- ComputeSettingsFiles

        class ComputeSettings{
        }
        class ComputeSettingsGeneral{
          +str arch_type
          +bool dry_run
          +bool force_recompute
          +bool wait
          ...
          get_hash()
        }
        class ComputeSettingsResources{
          +int ntasks
          +int ntasks_per_node
          +int cpus_per_task
          +int memory_per_cpu
          ...
          set_resources()
        }
        class ComputeSettingsEnvironment{
          +str env
          +str env_vars
          +list add_to_path
          +list add_to_ld_library_path
          ...
          set_environment()
        }
        class ComputeSettingsLogger{
          +str logger
          +str print_level
          ...
          set_logger()
        }
        class ComputeSettingsDirectories{
          +str work_dir_local
          +str work_dir_remote
          +str scratch_dir_local
          +str scratch_dir_remote
          +str data_dir_local
          +str data_dir_remote
          ...
          set_dirs()
        }
        class ComputeSettingsDatabase{
          +str database
          ...
          set_database()
        }
        class ComputeSettingsScheduler{
          +str scheduler_type
          +str slurm_account
          ...
          set_scheduler()
        }
        class ComputeSettingsConnection{
          +str connection_type
          +str file_transfer
          +str host
          +str user
          +int port
          ...
          set_connection()
        }
        class ComputeSettingsConda{
          +str conda_env
          ...
          set_conda()
        }
        class ComputeSettingsContainer{
          +str container_image
          ...
          set_container()
        }
        class ComputeSettingsProgram{
          +str program
          +list launcher
          +list args
          ...
        }
        class ComputeSettingsFiles{
          +str output_file
          +str stdout_file
          +str stderr_file
          ...
          set_files()
        }



*****************
ComputeSettings2
*****************


Simplifying the `ComputeSettings` class by condensing options gives `ComputeSettings2` as to be used by `hyrun.run()`:

.. mermaid::

    %%{
      init: {
        'theme': 'base',
        'themeVariables': {
          'primaryColor': '#2b38ff',
          'primaryTextColor': '#fff',
          'primaryBorderColor': '#17d9ff',
          'lineColor': '#17d9ff',
          'secondaryColor': '#17d9ff',
          'tertiaryColor': '#fff'
        }
      }
    }%%

    classDiagram

        ComputeSettings2 <|-- ComputeSettingsGeneral
        ComputeSettings2 <|-- ComputeSettingsDirectories
        ComputeSettings2 <|-- ComputeSettingsProgram
        ComputeSettings2 <|-- ComputeSettingsFiles

        class ComputeSettings2{
        +dict database = ComputeSettingsDatabase()
        +dict scheduler = ComputeSettingsScheduler()
        +dict connection = ComputeSettingsConnection()
        +dict conda = ComputeSettingsConda()
        +dict container = ComputeSettingsContainer()
        +dict resources = ComputeSettingsResources()
        +dict environment = ComputeSettingsEnvironment()
        +dict logger = ComputeSettingsLogger()
        ...
        }
        class ComputeSettingsGeneral{
          +str arch_type
          +bool dry_run
          +bool force_recompute
          +bool wait
          ...
          get_hash()
        }
        class ComputeSettingsDirectories{
          +str work_dir_local
          +str work_dir_remote
          +str scratch_dir_local
          +str scratch_dir_remote
          +str data_dir_local
          +str data_dir_remote
          ...
          set_dirs()
        }
        class ComputeSettingsProgram{
          +str program
          +list launcher
          +list args
          ...
        }
        class ComputeSettingsFiles{
          +str output_file
          +str stdout_file
          +str stderr_file
          ...
          set_files()
        }

The latter has the advantage that one can

- simplify the instantiation of the submodules, maybe with presets, e.g. `scheduler = 'saga'` would automatically set the `scheduler_type` and
  `slurm_account` attributes, etc.
- possibility to test submodules. For example the `connection` dictionary is used to set up the connection to the remote server using `hytools.connection` and can thus be tested separately.
- reduce the number of attributes in the `ComputeSettings` class

The former has the advantage for the user that

- one does not have to deal with the submodules and can just set the attributes in the  `ComputeSettings` class.



.. The following attributes will be set in the `ComputeSettings` class:


.. .. code-block:: python
..     :caption: ComputeSettings

..     # general
..     arch_type: str  # local or remote
..     dry_run: bool
..     force_recompute: bool
..     wait: bool
..     # parse_remotely: bool (planned for future)
..     # logging
..     logger: hyset.logger.Logger
..     print_level: str
..     # directories
..     work_dir_local: Union[str, pathlib.Path]
..     work_dir_remote: Union[str, pathlib.Path]
..     scratch_dir_local: Union[str, pathlib.Path]
..     scratch_dir_remote: Union[str, pathlib.Path]
..     data_dir_local: Union[str, pathlib.Path]
..     data_dir_remote: Union[str, pathlib.Path]
..     sub_dir: Union[bool, str, pathlib.Path]
..     # resources
..     ntasks: int
..     ntasks_per_node: int
..     cpus_per_task: int
..     memory_per_cpu: int
..     job_time: Union[str, datetime.timedelta]
..     env: Union[dict, str, list]
..     env_vars: Union[dict, str, list]
..     add_to_path: list
..     add_to_ld_library_path: list
..     post_cmd: Union[str, List[str]]
..     pre_cmd: Union[str, List[str]]
..     # database
..     database: Union[str, dict]
..     # submission
..     scheduler: Union[str, dict]
..     connection: Union[str, dict]


.. .. role:: python(code)
..    :language: python

.. `arch_type` is a string that can be either :python:`local` or :python:`remote`.
.. This is needed from some interfaces to determine if a calculation is running
.. locally or remotely.
.. The next attributes are self-explanatory, e.g. :python:`wait` is a boolean
.. that determines if the program should wait for the calculation to finish or not.

.. Next, the directories are set explicitly. The defaults for the local directories
.. are :python:`pathlib.Path.cwd()`, the values for the remote directories are
.. (up to the username) predefined for sigma2 clusters. It is thus rarely
.. that the user has to set them manually. Furthermore, not all directories are
.. needed for all calculations, e.g. :python:`scratch_dir_local` is not needed
.. for remote calculations. One could therefore think about making these
.. attributes optional, i.e. condense them into a dictionary and set them
.. conditionally. However, it is not clear if this is a good idea, since the user
.. would then have to set the dictionary keys manually.

.. The next attributes are related to the resources needed for the calculation,
.. including the computation environment.

.. `database` is a string or a dictionary with :python:`database = 'mydb'` equals
.. to :python:`database = {'name': 'mydb'}`. Here it is unclear if there are more
.. attributes needed for the database, e.g. the type, a connection etc.
.. In view of storing the `ComputeSettings`, we prefer dictionaries instead of
.. `hydb.Database` instances.

.. A similar problem arises with the `scheduler` attribute. For example,
.. :python:`scheduler = 'slurm'` would also require a `slurm_account` which
.. is not needed for other schedulers.

.. The same applies to the `connection` attribute. Note that internally,
.. all connection-related attributes are already stored in a dictionary, e.g.
.. :python:`{'host': 'saga.sigma2.no', 'max_connect_attempts': 3, 'timeout': 60}`.



create_compute_settings()
----------------------------


`ComputeSettings` is a dataclass which can instantiated using the kwargs, e.g.
with a dictionary. Alternatively, or when using presets, for example the
ones  provides for sigma2 clusters, the user can use the function
`create_compute_settings(args, **kwargs)`. The first argument refers to the
presets and is either a dictionary, a filename (json or yaml) or
a keyword in the provided presets, e.g. `saga`, `lumi`, `local` or `docker`.
The presets then get overwritten by the kwargs. The function returns a
`ComputeSettings` instance.


setting environment variables
--------------------------------

Probably one of the most complicated things in the `ComputeSettings` class is
the handling of environment variables. The user can set them in the
`ComputeSettings` class using the `env_vars` attribute. This can be a