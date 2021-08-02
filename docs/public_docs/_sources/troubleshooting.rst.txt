.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Troubleshooting
===============

To aid in debugging data generation issues, you may use the `explain` method of the
data generator class to produce a synopsis of how the data will be generated.

If run after the `build` method was invoked, the output will include an execution history explaining how the
data was generated.

See:

  * :data:`~dbldatagen.data_generator.DataGenerator.explain`
