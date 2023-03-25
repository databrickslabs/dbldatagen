.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Installation Notes
==================

The data generator framework can be installed into your Databricks environment by
one of the following methods:

* Installation from the PyPi package
* Installing and building directly from the Databricks Labs Github repository
* Installing the Python wheel file into your environment

Installing from PyPi
--------------------

To install the `dbldatagen` package from PyPi, add a cell to your notebook with the following code:

.. code-block::

   %pip install dbldatagen

This will install the PyPi package and works in regular notebooks, Delta Live Tables pipeline notebooks, and works on
the community edition.

If working using the command line, you can issue the following command to install within your environment.

.. code-block::

   pip install dbldatagen


Installing from Databricks Labs repository source
-------------------------------------------------

When developing with the Databricks notebook environment, you can use the notebook-scoped library install
 to install and build from the source in the Databricks Labs Github repository.

To do this, add and execute the following cell at the start of your notebook:

.. code-block::

   %pip install git+https://github.com/databrickslabs/dbldatagen@current

By default, this will install a fresh build from the latest release based on the ``master`` branch.
You can install from a specific branch by appending the branch identifier or tag to the GitHub URL.

.. code-block::

   %pip install git+https://github.com/databrickslabs/dbldatagen@dbr_7_3_LTS_compat

The following tags will be used to pick up specific versions:

* `current` - the latest build from the master branch + doc changes and critical bug fixes
* `stable` - the latest release from the master branch (with changes for version marking and documentation only).
* `preview` - a preview build of forthcoming features (typically from the `develop` branch)

.. note::
   In rare cases, if there are critical bug fixes, these will be incorporated into `stable` version.

.. seealso::
   See the following links for more details:

   * `Azure documentation on notebook scoped libraries <https://docs.microsoft.com/en-us/azure/databricks/libraries/notebooks-python-libraries#install-a-library-from-a-version-control-system-with-pip/>`_

   * `AWS documentation on notebook scoped libraries <https://docs.databricks.com/libraries/notebooks-python-libraries.html#id5>`_

   * `VCS support in pip <https://pip.pypa.io/en/stable/cli/pip_install/>`_

Installing older releases
-------------------------
Prior to the availability of PyPi releases, the release process included  Python wheels in the  GitHub release assets.

You can download a specific Python wheel directly from the GitHub releases for these cases.

The complete set of available releases can be accessed `here <https://github.com/databrickslabs/dbldatagen/releases>`_.

You can install a specific wheel using either `%pip install` or the manual method.

To install a Python wheel  from a specific release with `pip` use the following syntax:

.. code-block::

   %pip install https://github.com/databrickslabs/dbldatagen/releases/download/v021/dbldatagen-0.2.1-py3-none-any.whl

Replace the reference to the `v021` wheel with the reference to the appropriate wheel as needed.

To install older releases from PyPi, include a release qualifier in the Pip install command.

For example:

.. code-block::

   %pip install dbldatagen==v0.3.0

All new releases will be released via PyPi only.

Manual installation
^^^^^^^^^^^^^^^^^^^
Older releases may still be installed manually, however the use of one of the Pip install based methods is recommended.

The following steps outline how to install an older binary release manually

* **Locate the wheel file in the Databricks Labs data generator releases**

* **Download the wheel artifact from the releases**
   * Select the desired release
   * Select the wheel artifact from the release assets
   * Download it

* **Create library entry in workspace**
   * Create the workspace library
   * Upload previously downloaded wheel

* **Attach library to cluster**

Additional information
^^^^^^^^^^^^^^^^^^^^^^

.. seealso::
   See the following links for more details:

   * `Azure documentation on libraries <https://docs.microsoft.com/en-us/azure/databricks/libraries/>`_

   * `AWS documentation on libraries <https://docs.databricks.com/libraries/index.html>`_

