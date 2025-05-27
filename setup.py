from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Database connection package'
LONG_DESCRIPTION = 'Database connection package'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="dbconnect", 
        version=VERSION,
        author="Marcelo Pereira Barbosa",
        author_email="<mpbarbosa@gmail.com>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=['oracledb'], # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        
        keywords=['python', 'first package'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)