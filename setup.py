from setuptools import setup, find_packages

# Read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='excelsql',  # Your package name
    version='0.1.33',  # Initial version
    description='A Python driver to use SQL on Excel workbooks with CRUD, using SQLite as a middleman',
    long_description=long_description,
    long_description_content_type='text/markdown',  # This is important for PyPI
    url='https://github.com/chris17453/xlsql',  # Update with your repo URL
    author='Charles Watkins',
    author_email='chris@watkinslabs.com',
    license='BSD 3',
    packages=find_packages(where='src'),  # Specify where to find the source code
    package_dir={'': 'excelsql'},  # Tell setuptools the source directory is `src`
    install_requires=[
        'pandas',
        'openpyxl',
        'pyexcel-xls',
        'pyexcel',
        'sqlalchemy',
        'pysqlite3',
        'xlrd',
        'xlwt',
        'pyxlsb'
    ],
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    include_package_data=True, 
)
