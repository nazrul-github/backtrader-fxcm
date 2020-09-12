import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fxcm",
    version="0.0.1",
    description="Integrate fxcm API into backtrader",
    long_description=long_description,
    license='GNU General Public License Version 3',
    url="https://github.com/zeroq/backtrader-fxm",
    packages=setuptools.find_packages(),
    install_requires=[
        'backtrader>=1.9',
        'pyyaml',
        'fxcmpy',
        'python-socketio'
    ],
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Programming Language :: Python :: 3"
    ],
    python_requires='>=3.6'
)
