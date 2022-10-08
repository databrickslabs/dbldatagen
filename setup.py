"""A setuptools based setup module
"""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

package_long_description = """###Databricks Labs Spark Data Generator###

    The Databricks Labs Data Generator can generate realistic synthetic data sets at scale for testing, 
    benchmarking, environment validation and other purposes.
    """

setuptools.setup(
    name="dbldatagen",
    version="0.2.2-dev0",
    author="Ronan Stokes, Databricks",
    author_email="ronan.stokes@databricks.com",
    description="Databricks Labs -  PySpark Synthetic Data Generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/data-generator",
    packages=['dbldatagen',
              'dbldatagen.distributions'
              ],
    license="Databricks License",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators"
    ],
    python_requires='>=3.7.5',
)
