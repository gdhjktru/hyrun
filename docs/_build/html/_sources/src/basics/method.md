# Method

For performing computations, the generation of a hylleraas Method object is required. This contains all relevant information about a calculation including the necessary methods to perform a computation. The input of a Method requires either the name of the program as defined by its interface or the interface itself.

* read input from dict, file (generic or via interface) or class instance

```python
import hylleraas as hsp
my_method = hsp.Method({'qcmethod' : 'HF', 'basis' : 'def2-TZVP'}, program = 'lsdalton')

```

* use a particular interface

```python
import hylleraas as hsp
my_method = hsp.Method({'myparam' : 1.0}, interface=MyInterface)
```

* add function to method instance using '+' operator

```python
import hylleraas as hsp
method1 = hsp.Method({'qcmethod' : 'HF', 'basis' : 'def2-TZVP'}, program = 'lsdalton')
method2 = hsp.Method({'qcmethod' : 'HF', 'basis' : 'def2-SVP'}, program = 'lsdalton')
method3 = method1 + method2.get_gradient
```
* remove function from method instance using '-' operator

```python
method3 = method1 - method1.get_gradient
```

Furthermore :
* check if function is contained in method instance using 'in' operator
* check if two methods are identical using '==' operator
<!--* * generic restart or defined via interface -->
<!--* (needs either program name or explicit interface and depending on the input also a molecule)-->
