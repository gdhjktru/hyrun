.. _school2025:

=================================================
Installing software for the Hylleraas School 2025
=================================================

For the 2025 iteration of the Hylleraas School, **we require you to install some
software locally on your machine before the start of the school on Jan 6th.**

We have made available a script that will automatically prepare and install and
test everything you need.

.. note::

    Running this script will change your preferred terminal to automatically
    activate a *base* Anaconda virtual environment on startup. If you prefer to
    **not** have this happen, you can answer *no* when asked during the
    installation, or run

    .. code-block:: bash

        conda config --set auto_activate_base false

    after installing.

.. role:: raw-html(raw)
   :format: html

.. tabs::

    .. tab:: Linux

        Download the `installation_script`_. Either copy the contents into a
        text editor of your choice and save the file, or run the following
        command in a terminal. On most Linux systems you can open a terminal
        with :raw-html:`<kbd>^ ctrl</kbd>` + :raw-html:`<kbd>⎇ alt</kbd>` +
        :raw-html:`<kbd>t</kbd>`.

        .. code-block:: bash

            curl -o $HOME/school_2025_install.sh https://gist.githubusercontent.com/mortele/e1148bdc05704c9e127d04c059baa19e/raw/2b3bec25248945fcead86100bb8302c1de3dedd1/school_2025_install.sh

        Next, run the script by running the following command in the terminal:

        .. code-block:: bash

            bash $HOME/school_2025_install.sh

    .. tab:: MacOS

        Download the `installation_script`_. Either copy the contents into a
        text editor of your choice and save the file, or run the following
        command in a terminal. On most MacOS systems you can open a terminal by
        pressing :raw-html:`<kbd>⌘ cmd</kbd>` + :raw-html:`<kbd>space</kbd>`
        and typing `terminal`.

        .. code-block:: bash

            curl -o $HOME/school_2025_install.sh https://gist.githubusercontent.com/mortele/e1148bdc05704c9e127d04c059baa19e/raw/2b3bec25248945fcead86100bb8302c1de3dedd1/school_2025_install.sh

        Next, run the script by running the following command in the terminal:

        .. code-block:: bash

            bash $HOME/school_2025_install.sh

    .. tab:: Windows

        Install `Windows Subsystem for Linux`_ (WSL) if you have not already by
        opening a Windows PowerShell terminal. You can do this by searching the
        start menu (:raw-html:`<kbd>win</kbd>`) for `PowerShell`. Run the
        following command in the PowerShell terminal:

        .. code-block:: bash

            wsl --install

        Once complete, open a WSL terminal by searching the start menu for
        `WSL`.

        Download the `installation_script`_ by running the following command in
        the WSL terminal:

        .. code-block:: bash

            curl -o $HOME/school_2025_install.sh https://gist.githubusercontent.com/mortele/e1148bdc05704c9e127d04c059baa19e/raw/2b3bec25248945fcead86100bb8302c1de3dedd1/school_2025_install.sh

        Next, run the script by running the following command in the WSL
        terminal:

        .. code-block:: bash

            bash $HOME/school_2025_install.sh

.. attention::

    After running the install script, please take 30 seconds to fill in this
    `form`_. **If you encountered any errors:** Please also email `Morten`_
    morten.ledum@kjemi.uio.no for help.

.. _`form`: https://docs.google.com/forms/d/e/1FAIpQLSeMOaiojGwz2J2ELTQ8NVeXbLl7vJhpWELzM5e8Zge83pGZMA/viewform?usp=dialog
.. _`Morten`: morten.ledum@kjemi.uio.no
.. _`installation_script`: https://gist.github.com/mortele/e1148bdc05704c9e127d04c059baa19e
.. _`Windows Subsystem for Linux`: https://learn.microsoft.com/en-us/windows/wsl/install
