# databrickslabs_testdatagenerator.text_generators module

This file defines various text generation classes and methods

<!-- !! processed by numpydoc !! -->

### class ILText(paragraphs=None, sentences=None, words=None)
Bases: `databrickslabs_testdatagenerator.text_generators.TextGenerator`

Class to generate Ipsum Lorem text paragraphs, words and sentences


* **Parameters**

    
    * **paragraphs** – Number of paragraphs to generate. If tuple will generate random number in range


    * **sentences** – Number of sentences to generate. If tuple will generate random number in tuple range


    * **words** – Number of words per sentence to generate. If tuple, will generate random number in tuple range


### Methods

| `classicGenerateText`(seed)

 | ”


 |
| `generateText`(seed, default_seed)

                 | generate text for seed based on configuration parameters.

                                                                                                                                                                   |
| `getAsTupleOrElse`(v, default_v, v_name)

           | get value v as tuple or return default_v is v is None

                                                                                                                                                                       |
| `pandasGenerateText`(v)

                            | pandas udf entry point for text generation

                                                                                                                                                                                  |
| **randomGauss**

                                      |                                                                                                                                                                                                                             |
<!-- !! processed by numpydoc !! -->

#### classicGenerateText(seed)
”
classic udf entry point for text generation


* **Parameters**

    **seed** – seed value to control generation of random numbers


<!-- !! processed by numpydoc !! -->

#### generateText(seed, default_seed)
generate text for seed based on configuration parameters.

As it uses numpy, repeatability is restricted depending on version of the runtime
:param seed: list or array-like set of seed values
:param default_seed: seed value to use if value of seed is None or null
:returns: list or Pandas series of generated strings of same size as input seed

<!-- !! processed by numpydoc !! -->

#### static getAsTupleOrElse(v, default_v, v_name)
get value v as tuple or return default_v is v is None

<!-- !! processed by numpydoc !! -->

#### pandasGenerateText(v)
pandas udf entry point for text generation


* **Parameters**

    **v** – pandas series of seed values for random text generation



* **Returns**

    Pandas series of generated strings


<!-- !! processed by numpydoc !! -->

#### randomGauss(bounds)
<!-- !! processed by numpydoc !! -->

### class TemplateGenerator(template)
Bases: `databrickslabs_testdatagenerator.text_generators.TextGenerator`

This class handles the generation of text from templates


* **Parameters**

    **template** – template string to use in text generation


### Methods

| `classicGenerateText`(v)

                           | entry point to use for classic udfs

                                                                                                                                                                                         |
| `pandasGenerateText`(v)

                            | entry point to use for pandas udfs

                                                                                                                                                                                          |
| `valueFromSingleTemplate`(base_value, gen_template)

 | Generate text from a single template

                                                                                                                                                                                        |
<!-- !! processed by numpydoc !! -->

#### classicGenerateText(v)
entry point to use for classic udfs

<!-- !! processed by numpydoc !! -->

#### pandasGenerateText(v)
entry point to use for pandas udfs

<!-- !! processed by numpydoc !! -->

#### valueFromSingleTemplate(base_value, gen_template)
Generate text from a single template


* **Parameters**

    
    * **base_value** – underlying base value to seed template generation.
    Ignored unless template outputs it


    * **gen_template** – template string to control text generation


<!-- !! processed by numpydoc !! -->

### class TextGenerator()
Bases: `object`

Base class for text generation classes

<!-- !! processed by numpydoc !! -->
