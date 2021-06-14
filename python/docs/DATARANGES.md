# Understanding and using data ranges

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

The set of actual values generated will be further constrained by the possible values of the underlying seed column. 
For example if a column is specified to have 10 possible values but the seed column only generates two values 
and random value generation is not used, then there will only be two values generated.

## Precedence rules for constraining the range of values

Some of the options can lead to conflicting information about the range of possible values. 

The following rules apply to any ranged data specifications:

- if the `unique_values` option is used
- If both a range object is specified and any of the options for `begin`, `end` , `interval` are specified, 
the relevant value in the range object is used
- If both a range object is specified and any of the options for `minValue`, `maxValue` , `step` are specified, 
the relevant value in the range object is used
