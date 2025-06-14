# requires pytest-regressions installed (pip install pytest-regressions)
import os
from pathlib import Path

import pytest
from hyif import Xtb
from hyobj import Molecule
from hyset import create_compute_settings as ccs

# from hyrun.runner import fetch_results, get_status
from hyrun.runner import run


@pytest.fixture(scope='session')
def original_datadir(request) -> Path:
    """Return the original data directory."""
    config = request.config
    return config.rootpath / config.getini('original_datadir')


@pytest.fixture(scope='session')
def datadir(request) -> Path:
    """Return the data directory."""
    config = request.config
    return config.rootpath / config.getini('datadir')


default_tolerance = dict(atol=1e-6, rtol=1e-6)
keys_to_extract = ['energy']

default_cs = {'print_level': 'debug',
              'force_recompute': True,
              'database': 'mydb'}
compute_settings = {
    'local': ccs('local', **default_cs),
    # 'docker': ccs('docker', container_image='xtb', **default_cs),
    # 'conda': ccs('conda', conda_env='base', **default_cs),
    'saga': ccs('saga', modules=['xtb/6.4.1-intel-2021a'],
                user=os.getlogin(),
                memory_per_cpu=2000, progress_bar=False, **default_cs)
    }
molecules = {'water': Molecule('O')}


def calculate(compute_settings, mol, keys_to_extract=keys_to_extract):
    """Generate data."""
    x = Xtb(compute_settings=compute_settings,
            check_version=False, properties=['gradient'])
    setup = x.setup(mol)
    output = run(setup)
    try:
        # result = x.parse(output)
        result = x.parse(output[0])[0]
        return {key: result[key] for key in keys_to_extract}
    except Exception as e:
        print(e)
        return {}


def test():
    """Generate data."""
    w = molecules['water']
    local = compute_settings['local']
    saga = compute_settings['saga']

    mols = [w, [w, w]]
    scheds = [local, saga]
    setup_list = []
    for sched in scheds:
        x = Xtb(compute_settings=sched,
                check_version=False,
                properties=['gradient'])
        for mol in mols:
            setup = x.setup(mol)
            setup_list.append(setup)
    output = run(setup_list)
    print(output)

# @pytest.mark.parametrize('mol',
#                          list(molecules.values()),
#                          ids=list(molecules.keys()))
# @pytest.mark.parametrize('cs',
#                          list(compute_settings.values()),
#                          ids=list(compute_settings.keys()))
# def test_all(cs, mol, num_regression, request):
#     """Test all."""
#     data = calculate(cs, mol)
#     # check if value is identical to the reference calculated with same
# method
#     num_regression.check(data,
#                          basename=f'{request.node.name}_{mol.hash}',
#                          default_tolerance=default_tolerance)
#     # check that all values are identical to local reference
#     num_regression.check(data,
#                          basename=f'{mol.hash}',
#                          default_tolerance=default_tolerance)


# # @pytest.mark.parametrize('mol',
# #                          list(molecules.values()),
# #                          ids=list(molecules.keys()))
# # @pytest.mark.parametrize('cs',
# #                          list(compute_settings.values()),
# #                          ids=list(compute_settings.keys()))
# # def test_fetch_jobs(cs, mol, num_regression, request):
# #     """Test fetch_jobs."""
# #     cs.wait = False
# #     cs.database = 'mydb'
# #     _ = calculate(cs, mol)
# #     from time import sleep
# #     s = get_status([-1], database='mydb')
# #     while s[0] != 'COMPLETED':
# #         sleep(10)
# #         s = get_status([-1], database='mydb')
# #         print('status', s)
# #     assert s[0] == 'COMPLETED'
# #     o = fetch_results([-1], database='mydb')
# #     x = Xtb(check_version=False)
# #     result = x.parse(o[0])[0]
# #     print('parsed fetched result', result)
# #     data = {key: result[key] for key in keys_to_extract}
# #     num_regression.check(data,
# #                          basename=f'{request.node.name}_fetch_results',
# #                          default_tolerance=default_tolerance)

# @pytest.mark.parametrize('mol',
#                          list(molecules.values()),
#                          ids=list(molecules.keys()))
# @pytest.mark.parametrize('cs',
#                          list(compute_settings.values()),
#                          ids=list(compute_settings.keys()))
# def test_rerun(cs, mol, num_regression, request):
#     """Test all."""
#     data = calculate(cs, mol)
#     # check if value is identical to the reference calculated with same
# method
#     num_regression.check(data,
#                          basename=f'{request.node.name}_{mol.hash}',
#                          default_tolerance=default_tolerance)
#     # check that all values are identical to local reference
#     num_regression.check(data,
#                          basename=f'{mol.hash}',
#                          default_tolerance=default_tolerance)
