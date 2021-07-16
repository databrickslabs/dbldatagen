.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Installation notes
==================

The data generator framework can be installed into your Databricks environment by
either

* installing the Python wheel file into your environment
* In a future update, installing the package from a public repository such as PyPi

Installing from pre-built release wheel
---------------------------------------

* **locate the wheel file in the Databricks Labs data generator releases**

.. image:: _static/images/locating_releases.png
   :width: 300
   :alt: Screenshot of releases in Databricks Labs data generator project
   :align: center

* **download the wheel artifact from the releases**
   * select the desired release
   * Select the wheel artifact from the release assets
   * download it

.. image:: _static/images/downloading_release.png
   :width: 300
   :alt: Downloading the release artifact
   :align: center

* **Create library entry in workspace**
   * create workspace library
   * upload previously downloaded wheel

.. image:: _static/images/creating_library.png
   :width: 300
   :alt: Creating a library in your workspace
   :align: center

* **Attach library to cluster**

.. image:: _static/images/attaching_library.png
   :width: 300
   :alt: Attaching library to cluster
   :align: center

Additional information
^^^^^^^^^^^^^^^^^^^^^^

.. seealso::
   See the following links for more details:

   * `Azure documentation on libraries <https://docs.microsoft.com/en-us/azure/databricks/libraries/>`_

   * `AWS documentation on libraries <https://docs.databricks.com/libraries/index.html>`_


Installing package from public repository
-----------------------------------------

To be added