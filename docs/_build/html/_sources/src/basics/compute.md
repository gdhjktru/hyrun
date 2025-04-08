# Compute

The Compute input will take a molecule and method input to perform a composite compuation. For example, while the Method class contains `elementary` method for computing energy and gradient, a geometry-optimzations is composed of these methods of a Method/Interface instance.



* perform geometry optimziation using hygo


		import hylleraas as hsp
		mymol=hsp.Molecule('H -0.0 0.152061 -0.987977\n O -0.0 -0.216631 -0.112321\n H 0.0 0.56457 0.680297\n')
		mymethod = hsp.Method({'basis': 'def2-SVP', 'qcmethod': 'HF' }, molecule = mymol, program = 'lsdalton')
		mygeo = hsp.Compute('geometry_optimization', molecule = mymol, method = mymethod, options = {'optimizer': 'geometric'})

<!--*
> for dalton , remeber `DALTON_LAUNCHER=mpirun -np 1 -mca btl ^tcp` and to set `DALTON_SCRATCH` *-->
