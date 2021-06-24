# Generating data that conforms to a known statistical distribution

By the default, data that is being generated at random uses a uniform random number generator. 

Sometimes it is useful to generate data that conforms to a known distribution. 

While use of the `weights` option with discrete value lists can be used to introduce skew, this can be awkward to 
manage for large sets of values.

To enable this, we support use of known distributions for randomly generated data on any field.

When the field is not numeric, the underlying seed value will generated to conform to the known distribution before being converted 
to the appropriate type as per usual semantics. 

Note that the distribution will be scaled to the possible range of values

The following distributions are supported:
- normal or Gaussian distribution
- Beta distribution
- Gamma distribution
- Exponential distribution

> Note the `distribution` option will have no effect for values that are not randomly generated as
> per use of the `random` option
