# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
from databrickslabs_testdatagenerator import *
from databrickslabs_testdatagenerator.distributions import *
import os
import sys
sys.path.insert(0, os.path.abspath('../../databrickslabs_testdatagenerator'))
sys.path.insert(0, os.path.abspath('../../databrickslabs_testdatagenerator/distributions'))


# -- Project information -----------------------------------------------------

project = 'Test Data Generator'
copyright = '2020, Databricks Inc'
author = 'Databricks Inc'

# The full version, including alpha/beta/rc tags
release = "0.10.0-prerel1"  # DO NOT EDIT THIS DIRECTLY!  It is managed by bumpversion


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    #'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'numpydoc',  # handle NumPy documentation formatted docstrings. Needs to install
    'recommonmark',
    'sphinx_markdown_builder'
    #'matplotlib.sphinxext.plot_directive',  # For visualize plot result
    #'nbsphinx',  # Converts Jupyter Notebook to reStructuredText files for Sphinx.
    # For ipython directive in reStructuredText files.
    #'IPython.sphinxext.ipython_console_highlighting',
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown'
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'build/*', 'Thumbs.db', '.DS_Store', '**.ipynb_checkpoints']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# The master toctree document.
master_doc = 'index'

# -- Options for auto output -------------------------------------------------

autoclass_content = 'class'
autosummary_generate = False

add_module_names=False

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = "../tdg-logo-medium.png"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_css_files = [
    'css/tdg.css',
]

#html_sidebars={
#    '**' : [ 'globaltoc.html']
#}

html_theme_options= {
    "external_links": [
            { "name": "Databricks Labs",
              "url" : "https://github.com/databrickslabs"
              }
        ]
}

numpydoc_show_class_members=True
numpydoc_show_inherited_class_members=False
numpydoc_class_members_toctree=False
numpydoc_attributes_as_param_list=True
