"""A setuptools based setup module
"""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

package_long_description = """###Databricks Labs Spark Test Data Generator###
    """

setuptools.setup(
    name="databricks-datagen",
    version="0.10.1-dev12",
    author="Ronan Stokes, Databricks",
    author_email="ronan.stokes@databricks.com",
    description="Databricks Labs -  PySpark Test Data Generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/data-generator",
    packages=['databricks_datagen',
              'databricks_datagen.distributions'
              ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Test Data Generator",
        "Synthetic Data Generator",
        "Data Generation"
    ],
    python_requires='>=3.7.5',
)
