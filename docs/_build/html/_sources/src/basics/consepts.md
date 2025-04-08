# Basic consepts

To run a calculation in HSP you need to:

 - Define the system (`Molecule` or `PeriodicSystem`)
 - Define a method object (`Method`)
 - *Optionally:* define a computational setting (`computational_setting`)
 - Perform the calculation (`Compute`)

Here we outline these basic steps, before we delve into the details in the next sections. To use
HSP you need to import the hylleraas module. We will throughout import this as hsp according to

```
        import hylleraas as hsp
```

## Defining the system

You can define a molecule in HSP with

```
	my_molecule = hsp.Molecule(molecule)
```

where `molecule` can be a filename, xyz- or Similes-type string, list, dictionary or class instance (see [Molecule](molecule) section).

## Defining the method

Next you can define a method object by

	my_method = hsp.Method(input, program = 'myprog')

where `input` can be a dictionary, string or a filename. The resulting instance `my_method` contains all information needed to set up a computation (see [Method](method) section).

> By default the Method object will assume a `local` computational setting, where the program is assumed to reside locally on your computer, and be accessible through your regular environment (ie. available in your PATH). However, you can easily define your computatioal setting yourself, where you can choose between local, Conda, Docker or SLURM environments (see the [Computational Setting](computational-setting) section).

## Perform the calculation

Finally, a computation can now be performed by using the `Compute` object (see [Compute](compute) section):

```
 	my_result = hsp.Compute('something', my_method, molecule = my_molecule)
```

 where 'something' has to be replaced by, e.g. 'geometry_optimization'.
