"""A setuptools based setup module
"""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

package_long_description = """###Databricks Labs Spark Test Data Generator###
    """

setuptools.setup(
    name="databrickslabs-testdatagenerator-labs-candidate",
    version="0.9.03",
    author="Ronan Stokes, Databricks",
    author_email="ronan.stokes@databricks.com",
    description="Databricks Labs -  PySpark Test Data Generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databricks",
    packages=['databrickslabs_testdatagenerator'],
    install_requires=[
        'pyspark>=2.4.0'],
    # packages=setuptools.find_packages(exclude=['contrib', 'unit_tests']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
