<!-- Test Data Generator documentation master file, created by
sphinx-quickstart on Sun Jun 21 10:54:30 2020.
You can adapt this file completely to your liking, but it should at least
contain the root `toctree` directive. -->
# Test Data Generator documentation

The Databricks Labs Test Data Generator project provides a convenient way to
generate large volumes of synthetic test data from within a Databricks notebook
(or regular Spark application).

By defining a test data generation spec, either in conjunction with an existing schema
or through creating a schema on the fly, you can control how test data is generated.

As the data generator generates a PYSpark data frame, it is simple to create a view over it to expose it
to Scala or R based Spark applications also.


* Documentation from code


    * databrickslabs_testdatagenerator package


        * Subpackages


            * databrickslabs_testdatagenerator.distributions package


        * Submodules


            * databrickslabs_testdatagenerator.column_generation_spec module


            * databrickslabs_testdatagenerator.column_spec_options module


            * databrickslabs_testdatagenerator.data_analyzer module


            * databrickslabs_testdatagenerator.data_generator module


            * databrickslabs_testdatagenerator.daterange module


            * databrickslabs_testdatagenerator.function_builder module


            * databrickslabs_testdatagenerator.nrange module


            * databrickslabs_testdatagenerator.schema_parser module


            * databrickslabs_testdatagenerator.spark_singleton module


            * databrickslabs_testdatagenerator.text_generators module


            * databrickslabs_testdatagenerator.utils module


        * Module contents


* Release Notes and related docs


    * Contributing and building


        * License


        * Building the code


            * Setting up your build environment


            * Building the Python wheel


            * Creating the HTML documentation


            * Running unit tests


        * Using the Project


        * Coding Style


    * Using the Test Data Generator


        * General Overview


        * Tutorials and examples


        * Basic concepts


            * Generating the test data


        * Creating a simple test data set


            * Building Device IOT Test Data


    * Release Notes


        * Requirements


        * Version 0.10.0-prerel2


            * Features


    * Code dependencies
