# Contributing to the Test Data Generator
We happily welcome contributions to *data-generator*. 

We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## License

When you contribute code, you affirm that the contribution is your original work and that you 
license the work to the project under the project's open source license. Whether or not you 
state this explicitly, by submitting any copyrighted material via pull request, email, or 
other means you agree to license the material under the project's open source license and 
warrant that you have the legal authority to do so.

## Building the code

The code has been tested with Python 3.65. It relies on setting up a virtual enviroment 
so it may not build correctly for earlier versions. 

The resulting library has been verified against Databricks runtime 6.2 and Apache Spark 2.4.4

Make sure that the `SPAR`
Run `make buildenv` from the main project directory to set up your build environmemt

Run  `make clean dist` from the main project directory.

## Running unit tests

Run  `make install tests` from the main project directory to run the unit tests.
