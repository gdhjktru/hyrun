# visualization
mogli is broken for osx (because OpenGL ist broken) and now also nglview is broken (since the new ipywidget version 8 has been released). As alternative we have

	hylleraas.view_molecule_browser(molecule, orbital, resolution)

molecule is either hylleraas molecuel instance or the string 'demo' (one has to be in the hylleraas directory to get this to work). Orbital is a (optional) string of a text file containing the orbital and resolution its resolution in one dimension (integer, usually between 10 and 30). Because we are still working on the performance, we show the demo of the orbital, but the molecule viewer can be used without restrictions.
