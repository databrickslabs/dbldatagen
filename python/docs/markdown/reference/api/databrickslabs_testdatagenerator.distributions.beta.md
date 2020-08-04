# databrickslabs_testdatagenerator.distributions.beta module

This file defines the statistical distributions related classes

The general pattern will be as follows:

Distribibution will be defined by class such as NormalDistribution

Will have to handle the following cases:


* columns with a set of discrete values


* columns with a real valued boundaries


* columns with a min and max value (and  optional step)

For all cases, the distribution may be defined with:

min-value, max-value, median / mean and some other parameter

Here are the parameterisations for each of the distributions:

exponential: unbounded range is 0 - inf (but effective range is 0 - 5?)

    min, max , rate or mean

normal: main range is mean +/- 3.5 x std (values can occur up to mean +/- 6 x std )

gamma: main range is mean +/- 3.5 x std (values can occur up to mean +/- 6 x std )

beta: range is zero - 1

There are multiple parameterizations
shape k, and scale (phi)
shape alpha and rate beta (1/scale)
shape k and mean miu= (k x scale)

Key aspects are the following


* how to map mean from mean value of column range


* how to map resulting distribution back to data set


* Key decisions


* any parameters mean,median, mode refer to absolute values in data set


* any parameters mean_value, median_value, mode_value refer to value in terms of range


* so if a column has the values [ online, offline, outage, inactive ] and mean_value is offline


* this may be translated behind the scenes to a normal distribution (min = 0, max = 3, mean=1, std=2/6)


* this will essentially make it a truncated distribution


* ways to map range of values to distribution


* a: scale range to values, if bounds are predictable


* b: truncate (making values < min= min , > max= max) - which may cause output to have different distribution than expected


* c: discard values outside of range

    
        * requires generation of more values than required to allow for discarded values


        * can sample correct values to fill in missing data


* d: modulo - will change distribution


* high priority distributions are normal, exponential, gamma, beta

<!-- !! processed by numpydoc !! -->

### class Beta(mean=None, std=None, min=None, max=None, rectify=True, std_range=3.5, round=False)
Bases: `object`

### Methods

| **generate**

 |  |
| **test_bounds**

                                      |                                                                                                                                                                                                                             |
<!-- !! processed by numpydoc !! -->

#### generate(size)
<!-- !! processed by numpydoc !! -->

#### test_bounds(size)
<!-- !! processed by numpydoc !! -->
