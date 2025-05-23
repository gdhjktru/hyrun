def pytest_addoption(parser):
    """Add options to pytest."""
    parser.addini('datadir',
                  'my own datadir for pytest-regressions')
    parser.addini('original_datadir',
                  'my own original_datadir for pytest-regressions')
