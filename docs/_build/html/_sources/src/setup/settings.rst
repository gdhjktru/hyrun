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
        RunSettings <-- Interface : setup()
        Runner <|-- Interface 
        RunSettings --> Result : run()
        Interface --> Output : parse()
        Result --> Output
        namespace hyset {
          class ComputeSettings{
            +str scheduler
            +int ntasks
            +int cpus_per_task
            +int memory_per_cpu
            ...
          }
          class RunSettings{
          }
        }
        namespace hyif {
          class Interface{
            +str program
            +list args
            +str output_file
            ...
            +setup()
            +run()
            +parse()
          }
        }
        namespace hyrun {
          class Runner{
          run()
          }
          class Result{
          +int job_id
          +int output_file
          ...
          }

        }
        class Output{
          +float energy
          ....
        }

`ComputeSettings` is a dataclass containing the general settings for a hsp
calculation, e.g. generated from 
`hyset.create_compute_settings()`. `RunSettings` inherits from `ComputeSettings`
and gets updated using the `setup()` function the program interface, 
e.g., the interface sets the program name and the arguments to be passed to the
program.
The `RunSettings` class is then used to run the calculation using hyrun.
Finally, the result from hyrun is parsed using the `parse()` function defined
by the interface to give the output.

The `run()` function defined in the Interface is a wrapper for performing the 
three steps setup, run and parsing.
