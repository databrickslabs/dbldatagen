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
- 

> Note the `distribution` option will have no effect for values that are not randomly generated as
> per use of the `random` option

The test data generator uses data ranges to constrain the values for generated data. 

By default, the data is only constrained to the range of the fields data type. 

Additionally, unless the `percent_nulls` option is used, the value `null` will not be generated for a field value.

The range of values for the generated data may be controlled in the following ways:
- Specifying the `unique_values` option to control the unique values for a column
- Specifying the `minValue`, `maxValue` and `step` options for a column
- Specifying the `begin`, `end` and `interval` options for data and timestamp valued columns
- Specifying an explicit set of values via the `values` option
- Using a specific range object for a column

> Each of the mechanisms for constraining the range of values determines the possible range of values
> for a given column. It does not guarantee that all values will be generated.


