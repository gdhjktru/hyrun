.. _user_guide:

**********
User guide
**********

The **Hylleraas Software Platform** (HSP) is a Python package for performing
chemical calculations with various software, and for easily combining such
calculations into complex workflows. Using only high-level Python functionality
and requiring little to no knowledge of the underlying simulation software, it
is possible *through the HSP* to setup and run cutting-edge chemical
calculations—locally or offloaded to a remote server.

Submodules
----------
The HSP is modular and subdivided into several modules, each of which handle
different aspects of setting up, running, and analyzing the output from
chemical calculations.

=============== ================================================================
Module          Description
=============== ================================================================
`hylleraas`     The main *Hylleraas* module is the only one the user will
                normally interact with.
`hyif`          The *Hylleraas interfaces* module contains interfaces to
                external simulation software.
`hyset`         The *Hylleraas settings* module contains functionality for
                setting up and running calculations locally or remotely.
`hydb`          The *Hylleraas database* module contains functionality for
                storing and retrieving calculation results and keeping track of
                on-going simulations.
`hyobj`         The *Hylleraas objects* module contains classes for
                representing chemical systems and their properties, as well as
                machine learning datasets of such systems.
`hygo`          The *Hylleraas geometry optimization* module contains
                functionality for running software-agnostic quantum geometry
                optimizations.
`hyal`          The *Hylleraas active learning* module contains functionality
                for running software-agnostic active learning.
=============== ================================================================

Development
-----------
The **Hylleraas Software Platform** represents an active and evolving project
aimed at bolstering the diverse spectrum of research endeavors undertaken
within the `Hylleraas Centre`_. With an emphasis on facilitating seamless
integration among various software development and computational initiatives,
HSP aspires to enable sophisticated multi-scale simulations that extend across
a vast range of length and time scales—from single atoms to billions, and from
attoseconds to milliseconds.

Beyond its primary mission, HSP endeavors to make a meaningful contribution to
the broader scientific community. This is achieved by fostering a sustainable,
flexible, and user-friendly platform that will continue to serve users and
developers well beyond the lifespan of the Hylleraas Centre itself. Embracing
an open-source philosophy, the platform is freely accessible to all interested
parties.

We encourage contributions from those who find value in this project. Whether
through direct code contributions, suggestions for improvements, or sharing
insights that could benefit others, your involvement is warmly welcomed. While
formal contribution guidelines are in development, we are ready to share our
general guidelines upon request.

.. _Hylleraas Centre: https://www.hylleraas.no/
