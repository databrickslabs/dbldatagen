.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Extending Text Generation
=========================

This feature should be considered ``Experimental``.

The ``PyfuncText``,  ``PyfuncTextFactory`` and ``FakerTextFactory`` classes provide a mechanism to expand text
generation to include
the use of arbitrary Python functions and 3rd party data generation libraries.

The following example illustrates extension with the open source Faker library using the
extended syntax.

.. code-block:: python

   from dbldatagen import DataGenerator, fakerText
   from faker.providers import internet

   shuffle_partitions_requested = 8
   partitions_requested = 8
   data_rows = 100000

   # partition parameters etc.
   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

   my_word_list = [
   'danish','cheesecake','sugar',
   'Lollipop','wafer','Gummies',
   'sesame','Jelly','beans',
   'pie','bar','Ice','oat' ]

   fakerDataspec = (DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
               .withColumn("name", percentNulls=0.1, text=fakerText("name") )
               .withColumn("address", text=fakerText("address" ))
               .withColumn("email", text=fakerText("ascii_company_email") )
               .withColumn("ip_address", text=fakerText("ipv4_private" ))
               .withColumn("faker_text", text=fakerText("sentence", ext_word_list=my_word_list))
               )
   dfFakerOnly = fakerDataspec.build()

   dfFakerOnly.write.format("delta").mode("overwrite").save("/tmp/test-output")

Lets look at the various features provided to do this.

Extended text generation with Python functions
----------------------------------------------

The ``PyfuncText`` object supports extending text generation with Python functions.

It allows specification of two functions

- a context initialization function to initialize shared state
- a text generation function to generate text for a specific column value

This allows integration of both arbitrary Python code and of 3rd party libraries into
the text generation process.

For more information, see :data:`~dbldatagen.text_generator_plugins.PyfuncText`

.. note::

  The performance of text generation using external libraries or Python functions may be substantially slower than
  the base text generation capabilities. However it should be sufficient for generation of tables of up to
  100 million rows on a medium sized cluster.

  Note that we do not test compatibility with specific libraries and no expectations are made on the
  repeatability of data when generated using external functions or libraries.

Example 1: Using a custom Python function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following code shows use of a custom Python function to generate text:


.. code-block:: python

   from dbldatagen import DataGenerator, PyfuncText
   partitions_requested = 4
   data_rows = 100 * 1000

   # the initialization function
   def initPluginContext(context):
      context.prefix = "testing"

   # the data generation function
   text_generator = (lambda context, value: context.prefix + str(value))

   pluginDataspec = (DataGenerator(spark, rows=data_rows, partitions=partitions_requested,
                     randomSeedMethod="hash_fieldname")
                     .withColumn("text",
                                 text=PyfuncText(text_generator,
                                 initFn=initPluginContext))
                    )

   dfPlugin = pluginDataspec.build()
   dfPlugin.show()

Extended text generation with 3rd party libraries
-------------------------------------------------

The same mechanism can be used to make use of the capabilities of 3rd party libraries.

The ``context`` object can be initialized with any arbitrary properties that may be referenced
during the execution of the text generation function.

This can include use of session or connection objects, lookup dictionaries etc.
As a separate context instance is created for each worker node process for each PyfuncText text generator,
the object does not have to be pickled or serialized across process boundaries.

By default the context is shared across calls to the underlying Pandas UDF that generates the text.
If the context properties cannot be shared across multiple calls, you can specify that the context is recreated for
each Pandas UDF call.

Example 2: Using an external text data generation library
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following code shows use of an external text generation library  to generate text.

In this case, the example is using the ``Faker`` library.

.. note ::
   The ``Faker`` library is not shipped as part of the data generator and the user is responsible for installing it
   on a cluster or workspace, if using. There is no testing of specific 3rd party libraries for compatibility
   and some features may not function correctly or at scale.

To install ``Faker`` in a Databricks notebook, you can use the ``%pip`` instruction in a notebook cell.
For example:

.. code-block::

   %pip install Faker

The following code makes use the of ``Faker`` library to generate synthetic names, email addresses,
IP addresses and credit card numbers.

.. code-block:: python

   from dbldatagen import DataGenerator, PyfuncText
   from faker import Faker
   from faker.providers import internet

   shuffle_partitions_requested = 36
   partitions_requested = 96
   data_rows = 10 * 1000 * 1000

   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

   def initFaker(context):
     context.faker = Faker(locale="en_US")
     context.faker.add_provider(internet)

   ip_address_generator = (lambda context, v : context.faker.ipv4_private())
   name_generator = (lambda context, v : context.faker.name())
   address_generator = (lambda context, v : context.faker.address())
   email_generator = (lambda context, v : context.faker.ascii_company_email())

   fakerDataspec = (DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
               .withColumn("name",
                           percentNulls=0.1,
                           text=PyfuncText(name_generator , initFn=initFaker))
               .withColumn("address",
                           text=PyfuncText(address_generator, initFn=initFaker))
               .withColumn("email",
                           text=PyfuncText(email_generator, initFn=initFaker))
               .withColumn("ip_address",
                           text=PyfuncText(ip_address_generator , initFn=initFaker))
               )
   df1 = fakerDataspec.build()

   df1.write.format("delta").mode("overwrite").save("/tmp/dbldatagen/fakerData")


Supporting extended syntax for 3rd party library integration
------------------------------------------------------------

Use of the `PyfuncTextFactory` class allows the use of the following constructs:

.. code-block:: python

 # initialization (for Faker for example)

 # setup use of Faker
 def initFaker(ctx):
   ctx.faker = Faker(locale="en_US")
   ctx.faker.add_provider(internet)

 FakerText = (PyfuncTextFactory(name="FakerText")
             .withInit(initFaker)        # determines how context should be initialized
             .withRootProperty("faker")  # determines what context property is passed to fn
             )

 # later use ...
 .withColumn("fake_name", text=FakerText("name") )
 .withColumn("fake_sentence", text=FakerText("sentence", ext_word_list=my_word_list) )

 # translates to generation of lambda function with keyword arguments
 # or without as needed
 .withColumn("fake_name",
             text=FakerText( (lambda faker: faker.name( )),
                             init=initFaker,
                             rootProperty="faker",
                             name="FakerText"))
 .withColumn("fake_sentence",
             text=FakerText( (lambda faker:
                                 faker.sentence( **{ "ext_word_list" : my_word_list} )),
                             init=initFaker,
                             rootProperty="faker",
                             name="FakerText"))

By default, when the text generation function is called, the context object is passed to the
text generation function. However, if a root property is specified, it is interpreted the name of a property
of the context to be passed to the text generation function.

How does the string based access work?

If a string is specified to the PyfuncTextFactory in place of a text generation function or lambda function,
it is interpreted as the name of a method or property to access on the root object.

By default, the string is interpreted as the name of a method. But if you need to access a property of the root object,
you can use the syntax below (example is hypothetical and does not refer to any specific library).

.. code-block:: python

 .withColumn("my_property", text=MyLibraryText("myCustomProperty", isProperty=True) )


For more information, see :data:`~dbldatagen.text_generator_plugins.PyfuncTextFactory`

Faker specific library integration
----------------------------------

Finally, the ``FakerTextFactory`` provides a Faker specific version of the ``PyfuncTextFactory`` class
that initializes the Faker library and allows specification of locales and providers.

You will still need to install Faker as it is not included in the binaries.

If you are not customizing the FakerTextFactory, you can use ``fakerText`` to get the default faker text factory.

The following example will generate Italian localized text (where the underlying Faker provider supports it)
interspersed with use of the default faker text factory.


.. code-block:: python

   from dbldatagen import FakerTextFactory, DataGenerator, fakerText
   from faker.providers import internet

   shuffle_partitions_requested = 8
   partitions_requested = 8
   data_rows = 100000

   # setup use of Faker
   FakerTextIT = FakerTextFactory(locale=['it_IT'], providers=[internet])

   # partition parameters etc.
   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

   my_word_list = [
   'danish','cheesecake','sugar',
   'Lollipop','wafer','Gummies',
   'sesame','Jelly','beans',
   'pie','bar','Ice','oat' ]

   fakerDataspec = (DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
               .withColumn("italian_name", percentNulls=0.1, text=FakerTextIT("name") )
               .withColumn("name", percentNulls=0.1, text=fakerText("name") )  # uses default
               .withColumn("address", text=FakerTextIT("address" ))
               .withColumn("email", text=FakerTextIT("ascii_company_email") )
               .withColumn("ip_address", text=FakerTextIT("ipv4_private" ))
               .withColumn("faker_text", text=FakerTextIT("sentence") )
               )
   dfFakerOnly = fakerDataspec.build()

   dfFakerOnly.write.format("delta").mode("overwrite").save("/tmp/test-output-IT")

For more information, see :data:`~dbldatagen.text_generator_plugins.FakerTextFactory`


