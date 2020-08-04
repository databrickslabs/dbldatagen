# databrickslabs_testdatagenerator.nrange module

<!-- !! processed by numpydoc !! -->

### class NRange(min=None, max=None, step=None, until=None)
Bases: `object`

Ranged numeric interval representing the interval min .. max inclusive

A ranged object can be uses as an alternative to the min, max, step parameters to the DataGenerator withColumn and withColumn objects.
Specify by passing an instance of NRange to the data_range parameter.


* **Parameters**

    
    * **min** – Minimum value of range. May be integer / long / float


    * **max** – Maximum value of range. May be integer / long / float


    * **step** – Step value for range. May be integer / long / float


    * **until** – Upper bound for range ( i.e max+1)


You may only specify a max or until value not both.

For a decreasing sequence, use a negative step value.

### Methods

| `getContinuousRange`()

 | Convert range to continuous range

 |
| `getDiscreteRange`()

                               | Convert range to discrete range

                                                                                                                                                                                             |
| `isEmpty`()

                                        | Check if object is empty (i.e all instance vars of note are None

                                                                                                                                                            |
| `isFullyPopulated`()

                               | Check is all instance vars are populated

                                                                                                                                                                                    |
<!-- !! processed by numpydoc !! -->

#### getContinuousRange()
Convert range to continuous range

<!-- !! processed by numpydoc !! -->

#### getDiscreteRange()
Convert range to discrete range

<!-- !! processed by numpydoc !! -->

#### isEmpty()
Check if object is empty (i.e all instance vars of note are None

<!-- !! processed by numpydoc !! -->

#### isFullyPopulated()
Check is all instance vars are populated

<!-- !! processed by numpydoc !! -->
