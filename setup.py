from setuptools import find_packages, setup

setup(name='epics_rdb_archiver_access',
      version='0.1.dev0',
      description='Python package to access the EPICS RDB Archiver',
      url='https://github.com/pklaus/epics_rdb_archiver_access',
      author='Philipp Klaus',
      author_email='klaus@physik.uni-frankfurt.de',
      packages = find_packages(),
      py_modules=[],
      install_requires=['psycopg2-binary'],
      zip_safe=True)
