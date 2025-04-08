.. _ssh_setup:

==================================================
Setup offloading of computations to remote servers
==================================================

The `Hylleraas Software Platform`_ (HSP) can run computations for you on remote
servers. This is useful if you have access to a powerful supercomputer cluster
or a server with e.g. GPUs. The HSP uses the `SSH`_ protocol to communicate
with the remote server.

To setup offloading of computations to a remote server, you need to have the
following:

1. A remote server with SSH access.
2. A user account on the remote server.
3. A public SSH key on the remote server.

The following steps will guide you through the setup process.

1. Generate a new SSH key pair on your local machine using the `ssh-keygen`_
command. An SSH key pair consists of a public key and a private key. The public
key is copied to the remote server, while the private key is kept on your local
machine. The private key is used to authenticate you when connecting to the
remote server. The public key is used by the remote server to verify your
identity.

.. code-block:: bash

   ssh-keygen -t rsa -b 4096

Note that this command is run in the terminal. You may be asked for a passphrase
to protect your private key. You can leave this empty if you want to skip the
passphrase. You may also be asked to specify a location to save the keys. If you
leave it blank keys will be saved in the default location which is usually
:code:`~/.ssh/id_rsa`. This is recommended.

2. Copy the public key part of the SSH key to the remote server. This will allow
the remote server to verify your identity when you connect to it. You can do
this using the `ssh-copy-id`_ command. Replace :code:`USERNAME` with your
username and :code:`remote-server` with the address of the remote server.

.. code-block:: bash

   ssh-copy-id USERNAME@remote-server

3. You may now test the connection and verify that you can connect to the remote
server without being asked for a password. You can do this by running the
following command:

.. code-block:: bash

   ssh USERNAME@remote-server

.. _`Hylleraas Software Platform`: https://gitlab.com/hylleraasplatform/hylleraas
.. _`SSH`: https://en.wikipedia.org/wiki/Secure_Shell
.. _`ssh-keygen`: https://man.openbsd.org/ssh-keygen.1
.. _`ssh-copy-id`: https://linux.die.net/man/1/ssh-copy-id


Example using the Norwegian national high-performance computing infrastructure
==============================================================================

The following example demonstrates how to setup offloading of computations to
the Norwegian `Sigma2`_ supercomputer clusters. `NRIS`_ (Norwegian research
infrastructure services) operate the national HPC

.. _`Sigma2`: https://www.sigma2.no/about-us
.. _`NRIS`: https://www.sigma2.no/about-us

1. Generate a new SSH key pair on your local machine:

.. code-block:: bash

   ssh-keygen -t rsa -b 4096

You will be asked for a passphrase, a location, and a filename. You can leave
these empty to use the default values--hit return four times to use the
defaults.

2. Copy the public key to the supercomputer cluster `Saga`_:

.. code-block:: bash

   ssh-copy-id USERNAME@saga.sigma2.no

Replace :code:`USERNAME` with your username on the supercomputer cluster. You
will be asked for your password on the supercomputer cluster. Enter your
password and hit return.

3. Test the connection:

.. code-block:: bash

   ssh USERNAME@saga.sigma2.no

You should now be connected to the supercomputer cluster without being asked for
a password and you are ready to offload computations to the supercomputer
cluster using the `Hylleraas Software Platform`_.

.. _`Saga`: https://www.sigma2.no/documentation/hpc_machines/saga.html
