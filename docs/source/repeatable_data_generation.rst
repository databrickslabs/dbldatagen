.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Repeatable data generation
==========================

One of the basic principles of the data generator is that all data can be generated multiple times and
produce the same results unless a column is marked ``random`` and no random seed is being used. By default
a predefined random seed will be used for all random columns - so by definition all data is repeatable.

All columns will use the same random seed unless the seed method is specified to be 'hash_fieldname' or the seed is
overridden at the column level. In the case of the use of the 'hash_fieldname' generation method,
it will use a hash value of the field name so that each column has a different seed.

To generate true random values, the random seed of -1 must be specified, either at the data spec level or at the
individual column level. When specified at the individual column level, it only applies to that column.

If columns are not marked random - they will produce a repeatable set of data. For most columns, as the columns
are produced by a deterministic transformation on the corresponding base columns, the data will always be repeatable.

For columns generated using an inherently random process such as those produced with the template generation, ILText
and text data generator plugins, the random process will be seeded with a constant value unless the corresponding
column specification is marked as ``random``.

If a random seed is provided, either as an argument to the DataGenerator instance specification,
or as option on the column specification, the random seed will be applied to fields when random data generation is used.

By default, a default random seed is used unless a specific random seed is provided.


Application to generation of Change Data Capture(CDC) data
----------------------------------------------------------
To be added.

Application to generation of multiple tables
--------------------------------------------

One common scenario is the need to be able to generate multiple tables
with consistent primary and foreign keys to model join or merge scenarios.

By generating tables with repeatable data, we can generate multiple versions of the same data for different tables and
ensure that we have referential integrity across the tables.

Examples to be added.
