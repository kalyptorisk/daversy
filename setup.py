# bootstrap setuptools
import sys, ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages

# py2exe support
PY2EXE_PACKAGES = ['daversy.command', 'daversy.db', 'daversy.db.oracle', 'gzip', 'decimal']
PY2EXE_SCRIPTS  = ['src/daversy/dvs.py', 'src/daversy/tools/dvstool_oracle.py']

try:
    import py2exe
    py2exe_opts = { 'console' : PY2EXE_SCRIPTS,
                    'options' : { 'py2exe': { 'dll_excludes': 'oci.dll',
                                              'packages':  PY2EXE_PACKAGES } }
    }
except ImportError:
    py2exe_opts = {}

# get the version
sys.path.append('src')
import daversy

# setup configuration
setup(
    name = 'daversy',
    version = daversy.VERSION,
    author = 'Ashish Kulkarni',
    author_email = 'ashish.kulkarni@kalyptorisk.com',
    description = 'Daversy is a source control tool for relational databases.',
    license = 'GPL',
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Version Control',
        'Topic :: Utilities'
    ],

    install_requires = ['cx_Oracle >= 4.2', 'lxml >= 1.0.3'],

    package_dir = {'': 'src'},
    packages = find_packages('src', exclude=['ez_setup']),

    entry_points = {
        'console_scripts': [
            'dvs = daversy.dvs:main',
            'dvstool_oracle = daversy.tools.dvstool_oracle:main'
        ]
    },

    **py2exe_opts
)
