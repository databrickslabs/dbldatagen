"""A setuptools based setup module
"""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

package_long_description = """###Databricks Labs Spark Test Data Generator###
    """

setuptools.setup(
    name="databrickslabs-testdatagenerator",
    version="0.10.1-b5",
    author="Ronan Stokes, Databricks",
    author_email="ronan.stokes@databricks.com",
    description="Databricks Labs -  PySpark Test Data Generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/data-generator",
    packages=['databrickslabs_testdatagenerator',
              'databrickslabs_testdatagenerator.distributions'
              ],
    install_requires=[
        'pyspark>=2.4.0',
        'numpy',
        'pandas',
        'pyarrow>=0.8.0'
    ],
    # packages=setuptools.find_packages(exclude=['contrib', 'unit_tests', 'tests']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Test Data Generator",
        "Synthetic Data Generator"
    ],
    python_requires='>=3.6',
)
