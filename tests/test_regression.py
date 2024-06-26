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
    output = run(setup, parser=x)
    print(output)
    try:
        # result = x.parse(output)
        result = x.parse(output[0])[0]
        print('parsed result', result)
        return {key: result[key] for key in keys_to_extract}
    except Exception as e:
        print(e)
        return {}


@pytest.mark.parametrize('mol',
                         list(molecules.values()),
                         ids=list(molecules.keys()))
def test_gen_jobs(mol):
    """Test gen_jobs."""
    from hydb import Database

    from hyrun.runner import gen_jobs
    x = Xtb(check_version=False)
    jobs_ref = gen_jobs([x.setup(mol) for _ in range(2)])
    jobs_ref[0]['database'] = None
    # test generating from a list of db_ids
    jobs0 = gen_jobs([-2, -1], database='mydb')
    wd = str(jobs0[0]['job'].tasks[0].work_dir_local)
    wd_ref = str(jobs_ref[0]['job'].tasks[0].work_dir_local)
    assert wd == wd_ref

    # test generating from a list of Job objects
    # load jobs from db
    db = Database('mydb')
    db_ids = [-2, -1]
    jobs_db = []
    for id in db_ids:
        db_id = db._db_id(id)
        entry = db.get(key='db_id', value=db_id)
        job = db.dict_to_obj(entry)
        job.db_id = db_id
        jobs_db.append(job)
    jobs1 = gen_jobs(jobs_db)
    assert jobs1[0]['job'] == jobs0[0]['job']


@pytest.mark.parametrize('mol',
                         list(molecules.values()),
                         ids=list(molecules.keys()))
@pytest.mark.parametrize('cs',
                         list(compute_settings.values()),
                         ids=list(compute_settings.keys()))
def test_all(cs, mol, num_regression, request):
    """Test all."""
    data = calculate(cs, mol)
    # check if value is identical to the reference calculated with same method
    num_regression.check(data,
                         basename=f'{request.node.name}_{mol.hash}',
                         default_tolerance=default_tolerance)
    # check that all values are identical to local reference
    num_regression.check(data,
                         basename=f'{mol.hash}',
                         default_tolerance=default_tolerance)


# @pytest.mark.parametrize('mol',
#                          list(molecules.values()),
#                          ids=list(molecules.keys()))
# @pytest.mark.parametrize('cs',
#                          list(compute_settings.values()),
#                          ids=list(compute_settings.keys()))
# def test_fetch_jobs(cs, mol, num_regression, request):
#     """Test fetch_jobs."""
#     cs.wait = False
#     cs.database = 'mydb'
#     _ = calculate(cs, mol)
#     from time import sleep
#     s = get_status([-1], database='mydb')
#     while s[0] != 'COMPLETED':
#         sleep(10)
#         s = get_status([-1], database='mydb')
#         print('status', s)
#     assert s[0] == 'COMPLETED'
#     o = fetch_results([-1], database='mydb')
#     x = Xtb(check_version=False)
#     result = x.parse(o[0])[0]
#     print('parsed fetched result', result)
#     data = {key: result[key] for key in keys_to_extract}
#     num_regression.check(data,
#                          basename=f'{request.node.name}_fetch_results',
#                          default_tolerance=default_tolerance)

@pytest.mark.parametrize('mol',
                         list(molecules.values()),
                         ids=list(molecules.keys()))
@pytest.mark.parametrize('cs',
                         list(compute_settings.values()),
                         ids=list(compute_settings.keys()))
def test_rerun(cs, mol, num_regression, request):
    """Test all."""
    data = calculate(cs, mol)
    # check if value is identical to the reference calculated with same method
    num_regression.check(data,
                         basename=f'{request.node.name}_{mol.hash}',
                         default_tolerance=default_tolerance)
    # check that all values are identical to local reference
    num_regression.check(data,
                         basename=f'{mol.hash}',
                         default_tolerance=default_tolerance)
