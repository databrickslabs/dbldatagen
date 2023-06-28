"""A setuptools based setup module
"""
import setuptools
import re

START_TAG=r"^\s*\<\!--.*exclude\s+package.*--\>\s*$"
END_TAG=r"^\s*\<\!--.*end\s+exclude\s+package.*--\>\s*$"

with open("README.md", "r") as fh:
    # exclude lines from readme that dont apply to publication in package
    # for example navigation bar in Readme refers to github relative paths
    long_description = fh.read()
    modified_description_lines = []
    marked = False

    for line in long_description.split("\n"):
        if re.match(START_TAG, line) or re.match(END_TAG, line):
            marked = True
        if not marked:
            modified_description_lines.append(line)
        if  re.match(END_TAG, line):
            marked = False

    long_description = "\n".join(modified_description_lines)

package_long_description = """###Databricks Labs Spark Data Generator###

    The Databricks Labs Data Generator can generate realistic synthetic data sets at scale for testing, 
    benchmarking, environment validation and other purposes.
    """

setuptools.setup(
    name="dbldatagen",
    version="0.3.5",
    author="Ronan Stokes, Databricks",
    description="Databricks Labs -  PySpark Synthetic Data Generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/data-generator",
    project_urls={
        "Databricks Labs": "https://www.databricks.com/learn/labs",
        "Documentation": "https://databrickslabs.github.io/dbldatagen/public_docs/index.html"
},
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
    python_requires='>=3.8.10',
)
