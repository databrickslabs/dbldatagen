# databrickslabs_testdatagenerator.utils module

This file defines the DataGenError classes and utility functions

These are meant for internal use only

<!-- !! processed by numpydoc !! -->

### exception DataGenError()
Bases: `Exception`

Used to represent data generation errors

<!-- !! processed by numpydoc !! -->

### ensure(c, msg='condition does not hold true')
ensure(c, s) => throws Exception(s) if c is not true


* **Parameters**

    
    * **c** – condition to test


    * **msg** – Message to add to exception if exception is raised



* **Raises**

    DataGenError exception if condition does not hold true



* **Returns**

    Does not return anything but raises exception if condition does not hold


<!-- !! processed by numpydoc !! -->

### mkBoundsList(x, default)
make a bounds list from supplied parameter - otherwise use default


* **Returns**

    list of form [x,y]


<!-- !! processed by numpydoc !! -->

### topologicalSort(sources, initial_columns=None, flatten=True)
Perform a topological sort over sources

Used to compute the column test data generation order in terms of ordering according to dependencies


* **Parameters**

    
    * **sources** – list of `(name, set(names of dependencies))` pairs


    * **initial_columns** – force `initial_columns` to be computed first


    * **flatten** – if true, flatten output list



* **Returns**

    list of names in dependency order. If not flattened, result will be list of lists


<!-- !! processed by numpydoc !! -->
