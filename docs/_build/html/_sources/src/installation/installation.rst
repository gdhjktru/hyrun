.. _installation:

============
Installation
============

The `Hylleraas Software Platform`_ (HSP) is a Python code and may be installed
using `pip`_. To install the lastest development version of HSP using pip, run
the following command:

.. code-block:: bash

   pip install git+https://gitlab.com/hylleraasplatform/hylleraas.git

If you need more control over which Python version you are installing under
(for example in case you have multiple different Python versions installed on
your machine), you can specify with e.g. :code:`/usr/bin/python3.10 -m pip`
instead of just :code:`pip`.

This will install the full Python framework needed to run the HSP, including
dependencies. It will not however install the various codes that HSP is
interfaced to. For information about how to find installation instructions for
the various interfaced codes, please refer to
:ref:`external_software`.

.. admonition:: Disclaimer

   Please be aware that HSP is currently in its early development stages and
   should be considered experimental. While we strive for robustness and are
   committed to improving the code, we cannot guarantee its performance or
   reliability at this time. Consequently, we accept no responsibility for its
   consistency or any outcomes resulting from its use. We appreciate your
   understanding and welcome feedback to help address issues as they are
   identified.

.. _`Hylleraas Software Platform`: https://gitlab.com/hylleraasplatform/hylleraas
.. _`pip`: https://pip.pypa.io

Developer installation
======================

.. admonition:: Note
   :class: sidebar note

   If you are only trying to install the Hylleraas Software Platform, we
   recommend using pip, see `installation`_. Building HSP from source is not
   recommended. It is primarily intended for developers, contributors, and
   advanced users.

The `Hylleraas Software Platform`_ may be built from source by cloning the
open-source Gitlab source repository. The code is available at
`https:/www.gitlab.com/hylleraasplatform/hylleraas`_.

To clone the repository and install the code in `developer mode`_, run the
following commands:

.. code-block:: bash

   git clone https://gitlab.com/hylleraasplatform/hylleraas.git
   cd hylleraas
   pip install --editable .

Depending on your version of `Setuptools`_ and `pip`_, you may need to specify
:code:`--config-settings editable_mode=strict` in the :code:`pip install`. Note
that :code:`editable_mode=strict` will prevent *new files* you add from being
automatically added to the installed Python package. In case of new files you
will then have to manually re-install the package in question.

Developer installation of sub-modules
-------------------------------------

In order to make changes to the sub-modules (such as e.g. :code:`hyset`,
:code:`hyobj`, or :code:`hylleraas-interfaces`) you will need to developer
install them separately. Note that installing the main :code:`hylleraas`
Python module automatically installs the sub-modules, so you will have to
either manually uninstall the sub-module in question or use
:code:`pip --force-reinstall` for the sub-module.

For example, a developer installation of :code:`hyset` could be achieved by
the following:

.. code-block:: bash

   git clone https://gitlab.com/hylleraasplatform/hylleraas.git
   cd hylleraas
   python3 -m pip install -e . --config-settings editable_mode=strict

   cd ..
   git clone https://gitlab.com/hylleraasplatform/hyset.git
   cd hyset
   python3 -m pip uninstall -y hyset
   python3 -m pip install -e . --config-settings editable_mode=strict


.. _`Setuptools`: https://setuptools.pypa.io/en/latest/index.html
.. _`https:/www.gitlab.com/hylleraasplatform/hylleraas`: https://gitlab.com/hylleraasplatform/hylleraas
.. _`developer mode`: https://setuptools.pypa.io/en/latest/userguide/development_mode.html


Full dev installation of all sub-modules
----------------------------------------

The following code may be used to automatically perform a developer
installation of the main :code:`hylleraas` module, as well as all sub-modules
into a new `conda`_ environment:

.. code-block:: bash

   conda create -y -p ./hsp python=3.10
   conda activate ./hsp
   cd hsp

   git clone https://gitlab.com/hylleraasplatform/hylleraas.git
   git clone https://gitlab.com/hylleraasplatform/hyset.git
   git clone https://gitlab.com/hylleraasplatform/hyobj.git
   git clone https://gitlab.com/hylleraasplatform/hylleraas-interfaces.git hyif

   cd hylleraas
   python3 -m pip install -e . --config-settings editable_mode=strict
   cd ..

   python3 -m pip uninstall -y hyset hyobj hyif

   cd hyif
   python3 -m pip install -e . --config-settings editable_mode=strict
   cd ..

   cd hyset
   python3 -m pip install -e . --config-settings editable_mode=strict
   cd ..

   cd hyobj
   python3 -m pip install -e . --config-settings editable_mode=strict
   cd ..

.. _`conda`: https://conda.org/
