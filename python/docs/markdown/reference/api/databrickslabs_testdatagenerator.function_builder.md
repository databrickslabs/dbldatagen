# databrickslabs_testdatagenerator.function_builder module

<!-- !! processed by numpydoc !! -->

### class ColumnGeneratorBuilder()
Bases: `object`

Helper class to build functional column generators of specific forms

### Methods

| `mkCdfProbabilities`(weights)

 | make cumulative distribution function probabilities for each value in values list

 |
| `mkExprChoicesFn`(values, weights, …)

              | build an expression of the form \`\` case       when rnd_column <= weight1 then value1       when rnd_column <= weight2 then value2       …

                                                                                   |
<!-- !! processed by numpydoc !! -->

#### classmethod mkCdfProbabilities(weights)
make cumulative distribution function probabilities for each value in values list

a cumulative distribution function for discrete values can uses
a  table of cumulative probabilities to evaluate different expressions
based on the cumulative  probability of each value occurring

The probabilities can be computed from the weights using the code

    `weight_interval = 1.0 / total_weights
    probs = [x \* weight_interval for x in weights]
    return reduce((lambda x, y: cls._mk_list(x) + [cls._last_element(x) + y]), probs)`

but Python provides built-in methods (itertools.accumulate) to compute the accumulated weights
and dividing each resulting accumulated weight by the sum of the total weights will give the cumulative
probabilities.

For large data sets (relative to number of values), tests verify that the resulting distribution is
within 10% of the expected distribution when using Spark SQL Rand() as a uniform random number generator.
In testing, test data sets of size 3000 \* number_of_values rows produce a distribution within 10% of expected ,
while datasets of size 10,000 x number of values gives a repeated
distribution within 5% of expected distribution.

Example code to be generated (pseudo code):

    # given values value1 .. valueN, prob1 to probN
    # cdf_probabilities = [ prob1, prob1+prob2, … prob1+prob2+prob3 .. +probN]
    # this will then be used as follows

    prob_occurrence = uniform_rand(0.0, 1.0)
    if prob_occurrence <= cdf_prob_value1 then value1
    elif prob_occurrence <= cdf_prob_value2 then value2
    …
    prob_occurrence <= cdf_prob_valueN then valueN
    else valueN

see:

<!-- !! processed by numpydoc !! -->

#### classmethod mkExprChoicesFn(values, weights, seed_column, datatype)
build an expression of the form
\`\` case

> > when rnd_column <= weight1 then value1
> > when rnd_column <= weight2 then value2
> > …
> > when rnd_column <= weightN then  valueN
> > else valueN
> > end\`\`

> based on computed probability distribution for values.

> In Python 3.6 onwards, we could use the choices function but this python version is not
> guaranteed on all Databricks distributions

<!-- !! processed by numpydoc !! -->
