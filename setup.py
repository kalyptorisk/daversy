import sys, setuptools

# get the version
sys.path.append('src')
import daversy

METADATA = {
    'name'        : 'daversy',
    'version'     : daversy.VERSION,
    'author'      : 'Ashish Kulkarni',
    'author_email': 'ashish.kulkarni@kalyptorisk.com',
    'url'         : 'https://github.com/kalyptorisk/daversy',
    'description' : 'Daversy is a source control tool for relational databases.',
    'package_dir' : {'': 'src'},
    'license'     : 'GPLv2+',
    'classifiers' : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Version Control',
        'Topic :: Utilities'
    ]
}

SETUPTOOLS = METADATA.copy()
SETUPTOOLS.update({
    'packages'        : setuptools.find_packages('src'),
    'install_requires': ['cx_Oracle >= 5.0', 'lxml >= 3.4.0'],
    'entry_points'    : {
        'console_scripts': [
            'dvs = daversy.dvs:main',
            'dvstool_oracle = daversy.tools.dvstool_oracle:main'
        ]
    }
})

if __name__ == '__main__':
    setuptools.setup(**SETUPTOOLS)
