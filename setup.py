
from setuptools import setup, find_packages

with open ('README.md', 'r' )   as f:
    desscription = f.read()

setup(
    name='combinatorixPy',
    version='1.12',
    packages=find_packages( where = "src"),
    package_dir={'': 'src'},
    description='Development of Numerical Features/Descriptors to Describe Complex Materials for Machine Learning Modeling',
    long_description= desscription,
    long_description_content_type='text/markdown',
    author='Rahel Mahini',
    author_email='Rahil.Ashtarimahini@ndsu.edu',
    url='https://github.com/bakhras/MixtureDescriptors-II',
    license = " GPL-3.0",

    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',     
        'Topic :: Scientific/Engineering :: Chemistry',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.10',
    ],

    python_requires='>=3.6',
    install_requires = [
        # List your project's dependencies here.
        # They will be installed by pip when your project is installed.
        'numpy', 
        'pandas',
        'dask[array]',
        'dask[dataframe]',
        'dask[distributed]'
    ],

    extras_requires={
        "dev":["twine>=4.0.2"]
        }

)



