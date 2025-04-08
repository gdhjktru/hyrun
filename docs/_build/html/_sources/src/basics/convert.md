# convert
To convert between different representations of a molecule, use the convert function:
<!--* Hylleraas to daltonproject
-->* xyz to pdb, zmat, smiles, e.g. `pdb_string = hylleraas.convert(mymol, 'xyz', 'pdb')`
* zmat to xyz

		import hylleraas as hsp
		mymol = hsp.Molecule('./examples/ch3cooh.zmat')
		zmat = hsp.convert(mymol, 'xyz', 'zmat')
		assert zmat[1][2] == mymol.distance(0,1)
		xyz = hsp.convert(zmat, 'zmat', 'xyz')
		mymol2 = hsp.Molecule({'atoms': xyz[0], 'coordinates': xyz[1]})
		assert mymol == mymol2

*) needs script xyz2mol.py in path (the same we got from Hannes in Skibotn)
