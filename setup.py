from setuptools import find_packages, setup

# Monkey patching setuptools' safe_name function
# to allow a package name containing underscores
import pkg_resources
pkg_resources.safe_name = lambda x: x

try:
    import pypandoc
    LDESC = open('README.md', 'r').read()
    LDESC = pypandoc.convert_text(LDESC, 'rst', format='md')
except (ImportError, IOError, RuntimeError) as e:
    print("Could not create long description:")
    print(str(e))
    LDESC = ''

setup(name='beautyacc',
      version='0.1.dev0',
      description='Python package to access the EPICS RDB Archiver',
      long_description=LDESC,
      url='https://github.com/pklaus/beautyacc',
      author='Philipp Klaus',
      author_email='klaus@physik.uni-frankfurt.de',
      packages = find_packages(),
      py_modules=[],
      install_requires=['psycopg2-binary'],
      zip_safe=True,
      classifiers = [
          'Development Status :: 4 - Beta',
          'Operating System :: OS Independent',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Topic :: Scientific/Engineering :: Physics',
      ]
)
