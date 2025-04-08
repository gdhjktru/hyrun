
## Two-Factor Authentication (2FA) /  One-time-pad (OTP)

Two-factor authentication as introduced on [Betzy, Fram and Saga](https://documentation.sigma2.no/getting_help/two_factor_authentication.html) is implemented in the Hylleraas Software Platform using SSH multiplexing. That is, an initial connection creates a socket file in the `~/.ssh` folder which is then reused for all subsequent connections. This socket has a finite lifetime and thus has to be recreated if necessary. Upon socket creation, the user has to physically type-in the one-time password.

```{warning}
Currently, this works only with the branch hyset@2fa_prep and the most recent hytools@main
```

### Configuration

The example below shows how currently ssh multiplexing is called for sending jobs:
```python
from hyif import Xtb
from hyobj import Molecule
from hyset import create_compute_settings as ccs
import os

mymol = Molecule({'atoms': ['O', 'H', 'H'],
                    'coordinates': [[0,0,0], [0,1,0], [0,1,1]]})
myset = ccs('saga',
            connection_type='ssh_2fa',
            modules=['xtb/6.4.1-intel-2021a'],
            print_level='debug',
            user=os.getlogin(),
            memory_per_cpu=2000)
myxtb = Xtb(compute_settings=myset)
print(myxtb.get_energy(mymol))
```

The important keyword is `connection_type` which stears how SSH connections are handled during Runtime. A Connection of type `ssh_2fa` will, if necessary, try to create the socket and the job will be run as usual.


```{note}
The default connection type `ssh_fabric` does not allow multiplexing.
```

### Jupyter

Since prompting is [not supported in Jupyter](https://github.com/jupyterlab/jupyterlab/issues/14041), The above workflow would not work within a Jupyter environment if no socket is present.
Thus, the socket has to be created *outside* the Jupyter environment. To this end, hytools installs the following script:
```bash
create_ssh_socket --help
```

The following command with create a socket with a lifetime of 1hrs 40mins (= 6000 seconds):

```bash
create_ssh_socket --host saga.sigma2.no --user $(whoami) --port 22 --timeout 6000
```


### ~/.ssh/config

As described [here](https://documentation.sigma2.no/getting_help/two_factor_authentication.html), it is possible to store the socket configuration locally in `~/.ssh/config`.
Per default, HSP configures the SSH connection using command line options instead of using this file, i.e. the user does *not* have to create this file. However, if the file exists and the host alias matches the hostname used in the `RunSettings`, it is possible for HSP to use this configuration.

```{note}
Custom configurations might not be supported!
```


<!-- ```{seealso}
See Add a link to your repository for more information.
``` -->
