# Databricks Labs Test Data Generator

The Databricks labs test data generator is a Spark based solution for generating 
realistic synthetic data. It uses the features of Spark dataframes and Spark SQL 
to generate test data. As the output of the process is a dataframe populated 
with test data , it may be saved to storage in a variety of formats, saved to tables 
or generally manipulated using the existing Spark Dataframe APIs.

> NOTE: This document does not cover all of the classes and methods in the codebase.
>  For further information on classes and methods contained in  these modules, and 
> to explore the python documentation for these modules, feel free to clone the repo, 
> and generate the python documentation using `pydoc`
>
> Using the command `pydoc3 -p 8080` in a terminal window on your machine and then navigating in your browser to 
> http://localhost:8080/databrickslabs_testdatagenerator.html 
will allow browsing of the dynamically generated documentation. 

## General Overview

The Test Data Generator is a Python Library that can be used in several different ways:
1. Generate a test data set for an existing Spark SQL schema. 
2. Generate a test data set adding columns according to specifiers provided
3. Start with an existing schema and add columns along with specifications as to how values are generated

The test data generator includes the following features:

* Specify number of rows to generate
* Specify numeric, time and date ranges for columns
* Generate column data at random or from repeatable seed values
* Generate column data from list of finite column values optionally with weighting of how frequently values occur
* Use template based generation and formatting on string columns
* Use SQL based  expression to control or augment column generation
* Script Spark SQL table creation statement for dataset 





## Full Automation

At the highest level of the API, the FamilyRunner, using defaults, requires only a Spark Dataframe and an Array of InstanceConfigurations to be supplied to the object instantiation.

With the recommended PipelineAPI method .executeWithPipeline(), a SparkML Pipeline will be created, wrapping all transformations used in building the feature vector,
hyperparameter tuning, and best selected model will be returned.

### Family Runner API
This example shows configuring 3 seperate tuning runs (RandomForest Classifier, Logistic Regression, and XGBoost Classifier):
```scala
import com.databricks.labs.automl.executor.config.ConfigurationGenerator
import com.databricks.labs.automl.executor.FamilyRunner

val runName = "Automated-Model-Run-1"

val configurationOverrides = Map(
  "labelCol" -> "my_label",
  "tunerParallelism" -> 6,
  "tunerKFold" -> 3,
  "tunerTrainSplitMethod" -> "stratified",
  "scoringMetric" -> "areaUnderROC",
  "tunerNumberOfGenerations" -> 6,
  "tunerNumberOfMutationsPerGeneration" -> 8,
  "tunerInitialGenerationMode" -> "permutations",
  "tunerInitialGenerationPermutationCount" -> 18,
  "tunerFirstGenerationGenePool" -> 18,
  "mlFlowModelSaveDirectory" -> s"dbfs:/automl/$runName",
  "inferenceConfigSaveLocation" -> s"dbfs:/automl/inference/$runName",
  "mlFlowExperimentName" -> s"/Users/benjamin.wilson@databricks.com/AutoMLDemo/MLFlow/$runName"
)

val runConfiguration = Array("RandomForest", "LogisticRegression", "XGBoost")
    .map(x => ConfigurationGenerator.generateConfigFromMap(x, "classifier", configurationOverrides))

val pipelineRunner = FamilyRunner(spark.table("mlDatabase.featureData"), runConfiguration).executeWithPipeline()
```

Shown above are **typical override values** for the configuration overrides.  Each value does have a default assigned to it,
but the logging paths for default values may not be where you would like the artifacts to be stored.  
The `mlFlowExperimentName` in particular will just log the mlflow data to the parent directory of the notebook being used
to execute this code from.  This may not be desired, so it is recommended to override this value to a central location.

#### Automation Return Values

##### The Run Results, consisting of type: `FamilyFinalOutputWithPipeline`

Which are of type:
```scala
case class FamilyFinalOutputWithPipeline(
  familyFinalOutput: FamilyFinalOutput,
  bestPipelineModel: Map[String, PipelineModel],
  bestMlFlowRunId: Map[String, String] = Map.empty
)
```
These return types are:
* familyFinalOutput: `FamilyFinalOutput`
```scala
case class FamilyFinalOutput(modelReport: Array[GroupedModelReturn],
                             generationReport: Array[GenerationalReport],
                             modelReportDataFrame: DataFrame,
                             generationReportDataFrame: DataFrame,
                             mlFlowReport: Array[MLFlowReportStructure])
```
* bestPipelineModel -> The SparkML Pipeline (custom flavor, but still serializable) for each of the model families that have been tested.
  This artifact pipeline can be directly used for inference in a chained tune-per-run approach, or retrieved via the mlflow accessor API
  and used to perform inference on a data set with at least the same columns as the original data set (additional columns CAN be present for inference, but missing columns are not permitted)
* bestMlFlowRunId -> Map for the best result for each of the  model families.  This ID can be externally stored, if desired, so that a simple log-store interface can be used
to retrieve the appropriate pipeline from the tuning run for inference purposes.



The data within the familyFinalOutput consists of:
* modelReport -> Array[GroupedModelReturn]
```scala
case class GroupedModelReturn(modelFamily: String,
                              hyperParams: Map[String, Any],
                              model: Any,
                              score: Double,
                              metrics: Map[String, Double],
                              generation: Int)
```
* generationReport -> A Spark Dataframe representation of the Generational Average Scores, consisting of 2 columns:
    * `Generation[Int]`
    * `Score[Double]`

* modelReportDataFrame -> A Spark Dataframe representation of the Generational Run Results, consisting of 5 columns:

    * model_family[String]
    * model_type[String]
    * generation[Int]
    * generation_mean_score[Double]
    * generation_std_dev_score[Double]

* mlFlowReport -> Array[MLFlowReportStructure] that contains the following structures:
```scala
case class MLFlowReturn(client: MlflowClient,
                        experimentId: String,
                        runIdPayload: Array[(String, Double)])

case class MLFlowReportStructure(fullLog: MLFlowReturn, bestLog: MLFlowReturn
```

```text
NOTE: If using MLFlow integration, all of this data, in raw format, will be recorded and stored automatically.
```

## Configuration Generator API

The purpose of the configuration generator is to provide a means of overriding the default values of the automl toolkit.
The full configuration for the application utilizes a grouped configuration approach, isolating separate similar stage
configs in groups of similar relevance.  Due to the sheer number of tasks that are being performed, the complexity of
the internal processes, and the demoralizing idea of setting nested case class configurations, this interface allows for
a Map to be configured with individual overrides to change this structure internally.
In the following section, the configuration parameters will be shown and explained.


### Model Family

Setter: `.setModelingFamily(<String>)`

```text
Default: "RandomForest"

Sets the modeling family (Spark Mllib) to be used to train / validate.
```

> For model families that support both Regression and Classification, the parameter value
`.setModelDistinctThreshold(<Int>)` is used to determine which to use.  Distinct values in the label column,
if below the `modelDistinctThreshold`, will use a Classifier flavor of the Model Family.  Otherwise, it will use the Regression Type.

Currently supported models:
* "XGBoost" - [XGBoost Classifier](https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html#) or [XGBoost Regressor](https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html#)
* "RandomForest" - [Random Forest Classifier](http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier) or [Random Forest Regressor](http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression)
* "GBT" - [Gradient Boosted Trees Classifier](http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier) or [Gradient Boosted Trees Regressor](http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression)
* "Trees" - [Decision Tree Classifier](http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier) or [Decision Tree Regressor](http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-regression)
* "LinearRegression" - [Linear Regressor](http://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression)
* "LogisticRegression" - [Logistic Regressor](http://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression) (supports both Binomial and Multinomial)
* "MLPC" - [Multi-Layer Perceptron Classifier](http://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier)
* "SVM" - [Linear Support Vector Machines](http://spark.apache.org/docs/latest/ml-classification-regression.html#linear-support-vector-machine)

* "LightGBM" (currently suspended, pending library improvements to LightGBM) [LightGBM](https://github.com/Azure/mmlspark/blob/master/docs/lightgbm.md)
> NOTE: The automl-toolkit has the interfaces available for tuning LightGBM, but due to limitations in the underlying 
>thread management in LightGBM, asynchronous instantiations of models causes extreme instability to Spark.  If fixed in the future,
>this model will be moved to an [Accessible-Experimental] state.

### Generic Config

```scala
case class GenericConfig(var labelCol: String,
                         var featuresCol: String,
                         var dateTimeConversionType: String,
                         var fieldsToIgnoreInVector: Array[String],
                         var scoringMetric: String,
                         var scoringOptimizationStrategy: String)
```

#### Label Column Name

Setter: `.setLabelCol(<String>)`  Map Name: `'labelCol'`

```text
Default: "label"

This is the 'predicted value' for use in supervised learning.  
    If this field does not exist within the dataframe supplied, an assertion exception will be thrown
    once a method (other than setters/getters) is called on the AutomationRunner() object.
```
> NOTE : this value should always be defined by the end user.
#### Feature Column Name

Setter: `.setFeaturesCol(<String>)`  Map Name: `'featuresCol'`

```text
Default: "features"

Purely cosmetic setting that ensures consistency throughout all of the modules within AutoML-Toolkit.  

[Future Feature] In a future planned release, new accessor methods will make this setting more relevant,
    as a validated prediction data set will be returned along with run statistics.
```

#### Date Time Conversion Type

Setter: `.setDateTimeConversionType(<String>)` Map Name: `'dateTimeConversionType'`

```text
Default: "split"

Available options: "split", "unix"
```
This setting determines how to handle DateTime type fields.  
* In the "unix" setting mode, the datetime type is converted to `[Double]` type of the `[Long]` Unix timestamp in seconds.

* In the "split" setting mode, the date is transformed into its constituent parts and adding each part to seperate fields.
> By default, extraction is at maximum precision -> (year, month, day, hour, minute, second)

#### Fields to ignore in vector

Setter: `.setFieldsToIgnoreInVector(<Array[String]>)` 
Map Name `'fieldsToIgnoreInVector'`

```text
Default: Array.empty[String]

Provides a means for ignoring specific fields in the Dataframe from being included in any DataPrep feature
engineering tasks, filtering, and exlusion from the feature vector for model tuning.

This is particularly useful if there is a need to perform follow-on joins to other data sets after model
training and prediction is complete.
```

#### Scoring Metric

Setter: `.setScoringMetric(<String>)` 
Map Name `'scoringMetric'`

```text
Default is set dynamically by the prediction type (either regressor or classifier).

Available values:

Regressor -> rmse, mse, r2, mae

MultiClass Classifier -> f1, accuracy, weightedPrecision, weightedRecall
Binary Classifier -> f1, accuracy, weightedPrecision, weightedRecall, areaUnderROC, areaUnderPR
```

For Regressor modeling, the default is [rmse](https://en.wikipedia.org/wiki/Root-mean-square_deviation)

For Classifer modeling, the default is [f1](https://en.wikipedia.org/wiki/F1_score)

> NOTE: It is **highly advised** to override this value for the type of problem that is being solved.  No one scoring methodology works best for every situation.

> NOTE: although f1 score is a valid metric for binary classification problems, it is highly advised to use a Binary Classification scoring method that is more accurate for these problems (areaUnderROC, areaUnderPR)


#### Scoring Optimization Strategy

Setter: `.setScoringOptimizationStrategy(<String>)`  
Map Name `'scoringOptimizationStrategy'`

```text
The optimization strategy is a measure of the direction for which future candidates within the algorithm will be considered 'good'.  
At the conclusion of each tuning round, the evaluation of 'best as of time 'x'' is determined by sorting the results in either a maximal or minimal way,
and as such, this setting is critical to getting a good result from the tuning portion of the automl toolkit

Default: Determined by prediction type (Classifier is 'maximize', Regressor is 'minimize')
Available values: 'maximize' or 'minimize'
```
> NOTE take care when selecting which direction to optimize, particularly with r2 optimization.  The intent is typically to ***maximize*** this value, but the default
>configuration will set this to minimize.  Override this value if attempting to tune with r2 optimization.


### Switch Config
```scala
case class SwitchConfig(var naFillFlag: Boolean,
                        var varianceFilterFlag: Boolean,
                        var outlierFilterFlag: Boolean,
                        var pearsonFilterFlag: Boolean,
                        var covarianceFilterFlag: Boolean,
                        var oneHotEncodeFlag: Boolean,
                        var scalingFlag: Boolean,
                        var featureInteractionFlag: Boolean,
                        var dataPrepCachingFlag: Boolean,
                        var autoStoppingFlag: Boolean,
                        var pipelineDebugFlag: Boolean)
```


### Fill Null Values (naFillFlag)
```text
Default: ON
Turned off via setter .naFillOff()
```
> NOTE: It is **HIGHLY recommended** to leave this turned on.  If there are Null values in the feature vector, exceptions may be thrown.

This module allows for filling both numeric values and categorical & string values.

#### Available Overrides
* [Numeric Fill Stat](#numeric-fill-stat)
* [Character Fill Stat](#character-fill-stat)
* [Cardinality Check Mode](#fill-config-cardinality-check-mode)
* [Cardinality Switch](#fill-config-cardinality-switch)
* [Cardinality Type](#fill-config-cardinality-type)
* [Cardinlaity Limit](#fill-config-cardinality-limit)
* [Cardinality Precision](#fill-config-cardinality-precision)
* [Character NA Blanket Fill Value](#fill-config-character-na-blanket-fill-value)
* [Filter Precision](#fill-config-filter-precision)
* [NA Fill Mode](#fill-config-na-fill-mode)
* [Numeric Na Blanket Fill Value](#fill-config-numeric-na-blanket-fill-value)
* [Numeric NA Fill Map](#fill-config-numeric-na-fill-map)
* [Categorical NA Fill Map](#fill-config-categorical-na-fill-map)


### Filter Zero Variance Features (varianceFilterFlag)
```text
Default: ON
Turned off via setter .varianceFilterOff()

NOTE: It is HIGHLY recommended to leave this turned on.
Feature fields with zero information gain increase overall processing time and provide no real value to the model.
```
There are no options associated with module.

### Filter Outliers (outlierFilterFlag)
```text
Default: OFF
Turned on via setter .outlierFitlerOn()
```

This module allows for detecting outliers from within each field, setting filter thresholds
(either automatically or manually), and allowing for either tail reduction or two-sided reduction.
Including outliers in some families of machine learning models will result in dramatic overfitting to those values.  

> NOTE: It is recommended to only use this feature when doing basic exploratory analysis.  Filtering outliers should be
> conducted externally on a feature data set once the problem statement and analysis of data is completed.  

#### Available Overrides
* [Continuous Data Threshold](#outlier-continuous-data-threshold)
* [Fields To Ignore](#outlier-fields-to-ignore)
* [Filter Bounds](#outlier-filter-bounds)
* [Filter Precision](#outlier-filter-precision)

### Pearson Filtering (pearsonFilterFlag)
```text
Default: OFF
Turned on via setter .pearsonFilterOn()
```
This module will perform validation of each field of the data set (excluding fields that have been added to
`.setFieldsToIgnoreInVector(<Array[String]>])` and any fields that have been culled by any previous optional DataPrep
feature engineering module) to the label column that has been set (`.setLabelCol()`).

The mechanism for comparison is a ChiSquareTest that utilizes one of three currently supported modes :
- pValue
- pearsonStat
- degreesFreedom

For further reading on Pearson's chi-squared test -> 
[Spark Doc - ChiSquaredTest](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.stat.ChiSquareTest$)
[Pearson's Chi-squared test](https://en.wikipedia.org/wiki/Chi-squared_test)

#### Available Overrides
* [Pearson Filter Statistic](#pearson-filter-statistic)
* [Pearson Filter Direction](#pearson-filter-direction)
* [Pearson Filter Manual Value](#pearson-filter-manual-value)
* [Pearson Filter Mode](#pearson-filter-mode)
* [Pearson Auto Filter N Tile](#pearson-auto-filter-n-tile)

### Covariance Filtering (covarianceFilterFlag)
```text
Default: OFF
Turned on via setter .covarianceFilterOn()
```

Covariance Filtering is a Data Prep module that iterates through each element of the feature space
(fields that are intended to be part of the feature vector), calculates the pearson correlation coefficient between each
feature to every other feature, and provides for the ability to filter out highly positive or negatively correlated
features to prevent fitting errors.

Further Reading: [Pearson Correlation Coefficient](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient)

> NOTE: This algorithm, although operating within a concurrent thread pool, can be costly to execute.  
In sequential mode (parallelism = 1), it is O(n * log(n)) and should be turned on only for an initial exploratory phase of determining predictive power.  

> There are no settings for determining left / right / both sided filtering.  Instead, the cutoff values can be set to achieve this.
> > i.e. to only filter positively correlated values, apply the setting: `.setCovarianceCutoffLow(-1.0)` which would only filter
> > fields that are **exactly** negatively correlated (linear negative correlation)

#### Available Overrides
* [Cutoff-Low](#correlation-covariance-cutoff-low)
* [Cutoff-High](#correlation-covariance-cutoff-high)

##### General Algorithm example

Given a data set:

| A 	| B 	| C 	| D 	| label |
|:----:	|:----:	|:----:	|:----:	|:----: |
|   1  	|  94 	|   5  	|   10 	|   1   |
|   2  	|   1  	|   4  	|   20 	|   0   |
|   3  	|  22 	|   3  	|   30 	|   1   |
|   4  	|   5  	|   2  	|   40 	|   0   |
|   5  	|   5  	|   1  	|   50 	|   0   |

Each of the fields A:B:C:D would be compared to one another, the pearson value would be calculated, and a filter will occur.

* A->B
* A->C
* A->D
* B->C
* B->D
* C->D

There is a perfect linear negative correlation present between A->C, a perfect postitive linear correlation between
A->D, and a perfect linear negative correlation between C->D.
However, evaluation of C->D will not occur, as both C and D will be filtered out due to the correlation coefficient
threshold.
The resultant data set from this module would be, after filtering:

| A 	| B 	| label |
|:----:	|:----:	|:----: |
|   1  	|  94 	|   1   |
|   2  	|   1  	|   0   |
|   3  	|  22 	|   1   |
|   4  	|   5  	|   0   |
|   5  	|   5  	|   0   |

### OneHotEncoding (oneHotEncodingFlag)
```text
Default: OFF
Turned on via setter .oneHotEncodingOn()
```
Useful for all non-tree-based models (e.g. LinearRegression) for converting categorical features into a vector boolean
space.

Details: [OneHotEncoderEstimator](http://spark.apache.org/docs/latest/ml-features.html#onehotencoderestimator)

Implementation here follows the general recommendation: Categorical (text) fields will be StringIndexed first, then
OneHotEncoded.

> There are no options for this setting.  It either encodes the categorical features, or leaves them StringIndexed.
> NOTE: NOT RECOMMENDED for tree based models.  If a tree-based model is selected, a warning will appear.

### Scaling (scalingFlag)

The Scaling Module provides for an automated way to set scaling on the feature vector prior to modeling.  
There are a number of ML algorithms that dramatically benefit from scaling of the features to prevent overfitting.  
Although tree-based algorithms are generally resilient to this issue, if using any other family of model, it is
**highly recommended** to use some form of scaling.

The available scaling modes are:
* [minMax](http://spark.apache.org/docs/latest/ml-features.html#minmaxscaler)
   * Scales the feature vector to the specified min and max.  
   * Creates a Dense Vector.
* [standard](http://spark.apache.org/docs/latest/ml-features.html#standardscaler)
    * Scales the feature vector to the unit standard deviation of the feature (has options for centering around mean)
    * If centering around mean, creates a Dense Vector.  Otherwise, it can maintain sparsity.
* [normalize](http://spark.apache.org/docs/latest/ml-features.html#normalizer)
    * Scales the feature vector to a p-norm normalization value.
* [maxAbs](http://spark.apache.org/docs/latest/ml-features.html#maxabsscaler)
    * Scales the feature vector to a range of {-1, 1} by dividing each value by the Max Absolute Value of the feature.
    * Retains Vector type.
    
#### Available Overrides
* [Scaling Type](#scaling-type)
* [Scaling P-Norm](#scaling-p-norm)

### Feature Interaction (featureInteractionFlag)

The Feature Interaction module allows for creation of pair-wise products (Interactions) between feature fields.
The default mode (optimistic) will calculate [Information Gain](https://en.wikipedia.org/wiki/Information_gain_in_decision_trees)
from Entropy and Differential Entropy calculations done on the parents of each interaction candidate, the offspring candidate,
and make a decision to keep or discard based on the relative ratio of the interacted child to its parents.
There are 4 main modes here:
flag off - no interactions (default)
flag on - 'all' - all potential children candidates will be created with no Information Gain calculations being done
(fastest, but potentially risky as some interactions may create a poorly fit model)
flag on - 'optimistic' - potential children are interacted, but only retained in the final feature vector if the resulting
child's information gain metric is at least n% of at least one parent.  See below sections for the setters / map value to get
full explanation on what the configuration attributes are and what they do.
flag on - 'strict' - potential children are interacted, but are only retained if they are n% of BOTH PARENTS.  

DEFAULT: OFF

> NOTE strict is 'safest', but in general does not create additional fields.  It is recommended to use either 'optimisitic' for most use cases
>and 'all' for testing purposes or in aiding feature engineering iterative tasks to inform where to create more information for future experiments.

#### Available Overrides
* [Continuous Discretizer Bucket Count](#feature-interaction-continuous-discretizer-bucket-count)
* [Parallelism](#feature-interaction-parallelism)
* [Retention Mode](#feature-interaction-retention-mode)
* [Target Interaction Percentage](#feature-interaction-target-intercation-percentage)

### DataPrepCachingFlag

This setting will determine whether caching of the feature engineering stages happen prior to splitting.  It can be
useful for performance aspects of extremely large data sets or moderate size data sets with high numbers of columns in
the feature set.  

Default: OFF

### Auto Stopping Flag

If set to on, when coupled with a stopping criteria score, will stop initializing new Futures and once all async
currently running models are complete, will stop the tuning phase.

> NOTE: this feature WILL NOT TERMINATE CURRENTLY RUNNING TASKS.  Each model is run asynchronously, and once committed
>to execution, will run to completion.  This feature only prevents ADDITIONAL Futures from being executed.

#### Available Overrides
* [Auto-Stopping Score](#tuner-auto-stopping-score)

### Pipeline Debug Flag

When turned on, will report debug statements for each pipeline stage into stdout, giving insight into what settings
are being used, what the status is, and information about what is dynamically being done at runtime.

DEFAULT: OFF

> NOTE: debug reporting is VERBOSE.  Recommend to only run when needed to validate stage execution.

### Feature Engineering Config
```scala
case class FeatureEngineeringConfig(
  var dataPrepParallelism: Int,
  var numericFillStat: String,
  var characterFillStat: String,
  var modelSelectionDistinctThreshold: Int,
  var outlierFilterBounds: String,
  var outlierLowerFilterNTile: Double,
  var outlierUpperFilterNTile: Double,
  var outlierFilterPrecision: Double,
  var outlierContinuousDataThreshold: Int,
  var outlierFieldsToIgnore: Array[String],
  var pearsonFilterStatistic: String,
  var pearsonFilterDirection: String,
  var pearsonFilterManualValue: Double,
  var pearsonFilterMode: String,
  var pearsonAutoFilterNTile: Double,
  var covarianceCorrelationCutoffLow: Double,
  var covarianceCorrelationCutoffHigh: Double,
  var scalingType: String,
  var scalingMin: Double,
  var scalingMax: Double,
  var scalingStandardMeanFlag: Boolean,
  var scalingStdDevFlag: Boolean,
  var scalingPNorm: Double,
  var featureImportanceCutoffType: String,
  var featureImportanceCutoffValue: Double,
  var dataReductionFactor: Double,
  var cardinalitySwitch: Boolean,
  var cardinalityType: String,
  var cardinalityLimit: Int,
  var cardinalityPrecision: Double,
  var cardinalityCheckMode: String,
  var filterPrecision: Double,
  var categoricalNAFillMap: Map[String, String],
  var numericNAFillMap: Map[String, AnyVal],
  var characterNABlanketFillValue: String,
  var numericNABlanketFillValue: Double,
  var naFillMode: String,
  var featureInteractionRetentionMode: String,
  var featureInteractionContinuousDiscretizerBucketCount: Int,
  var featureInteractionParallelism: Int,
  var featureInteractionTargetInteractionPercentage: Double
)
```

#### Data Prep Parallelism

Setter: `.setDataPrepParallelism(<Int>)`  
Map Name: `'dataPrepParallelism'`

```text
Default: 10
```

This setting is used to set the maximum number of asynchronous Futures that are utilized for several stages within the
Data Preparation modules for feature vector creation.  Some of the checks and validations that are performed are
inherently parallelizable, and as such, the cluster can be utilized to asynchronously process these calculations.

> NOTE - WARNING - setting this value to too high of a value based on a cluster not large enough to handle the GC
> involved in multiple copies of individual field data series may introduce instability or long pauses while the Heap
> is cleared.  Perform testing on your data set to determine if  the cluster being used
> for the tuning run is sufficiently large to support overriding this value in a higher direction.


#### Numeric Fill Stat

Setter: `.setFillConfigNumericFillStat(<String>)`  
Map Name: `'fillConfigNumericFillStat'`

```text
Specifies the behavior of the naFill algorithm for numeric (continuous) fields.
Values that are generated as potential fill candidates are set according to the available statistics that are
calculated from a df.summary() method.

Default: "mean"
```
* For all numeric types (or date/time types that have been cast to numeric types)
* Allowable fill statistics:
1. <b>"min"</b> - minimum sorted value from all distinct values of the field
2. <b>"25p"</b> - 25th percentile (Q1 / Lower IQR value) of the ascending sorted data field
3. <b>"mean"</b> - the mean (average) value of the data field
4. <b>"median"</b> - median (50th percentile / Q2) value of the ascending sorted data field
5. <b>"75p"</b> - 75th percentile (Q3 / Upper IQR value) of the ascending sorted data field
6. <b>"max"</b> - maximum sorted value from all distinct values of the field

#### Character Fill Stat

Setter: `.setFillConfigCharacterFillStat(<String>)`  
Map Name: `'fillConfigCharacterFillStat'`

```text
Specifies the behavior of the naFill algorithm for character (String, Char, Boolean, Byte, etc.) fields.
Generated through a df.summary() method
Available options are:
"min" (least frequently occurring value)
or
"max" (most frequently occurring value)

Default: "max"
```

#### Model Selection Distinct Threshold

Setter: `.setFillConfigModelSelectionDistinctThreshold`  
Map Name: `'fillConfigModelSelectionDistinctThreshold'`

```text
Default: 50
```
The threshold value that is used to detect, based on the supplied labelCol, the cardinality of the label through
a .distinct().count() being issued to the label column.  Values from this cardinality determination that are
above this setter's value will be considered to be a Regression Task, those below will be considered a
Classification Task.

> NOTE: In the case of exceptions being thrown for incorrect type (detected a classifier, but intended usage is for
> a regression, lower this value.  Conversely, if a classification problem has a significant number of
> classes, above the default threshold of this setting (50), increase this value.)

#### Outlier Filter Bounds

Setter: `.setOutlierFilterBounds(<String>)`  
Map Name: `'outlierFilterBounds'`

```text
Default: "both"
```
Filtering both 'tails' is only recommended if the nature of the data set's input features are of a normal distribution.
If there is skew in the distribution of the data, a left or right-tailed filter should be employed.
The allowable modes are:
1. "lower" - useful for left-tailed distributions (rare)
2. "both" - useful for normally distributed data (common)
3. "upper" - useful for right-tailed distributions (common)

For Further reading: [Distributions](https://en.wikipedia.org/wiki/List_of_probability_distributions#With_infinite_support)

> NOTE: If you don't know what the distribution of your fields are, it is not advised to use outlier filtering.  
> This feature is also not recommended for production runs using the toolkit.  
> This feature is designed for a first-pass evaluation of completely uncleaned data for experimental purposes.

#### Lower Filter NTile

Setter: `.setOutlierLowerFilterNTile(<Double>)`  
Map Name: `'outlierLowerFilterNTile'`

```text
Default: 0.02

Restrictions are set on this configuration - the value must be between 0 and 1.
```
Filters out values (rows) that are below the specified quantile level based on a sort of the field's data in ascending order.
> Only applies to modes "both" and "lower"

#### Upper Filter NTile

Setter: `.setOutlierUpperFilterNTile(<Double>)`  
Map Name: `'outlierUpperFilterNTile'`

```text
Default: 0.98
```
Filters out values (rows) that are above the specified quantile threshold based on the ascending sort of the field's data.
> Only applies to modes "both" and "upper"

#### Outlier Filter Precision

Setter: `.setOutlierFilterPrecision(<Double>)`  
Map Name: `'outlierFilterPrecision'`

```text
Default: 0.01
```
Determines the level of precision in the calculation of the N-tile values of each field.  
Setting this number to a lower value will result in additional shuffling and computation.
The algorithm that uses the filter precision is `approx_count_distinct(columnName: String, rsd: Double)`.  
The lower that this value is set, the more accurate it is (setting it to 0.0 will be an exact count), but the more
shuffling (computationally expensive) will be required to calculate the value.
> NOTE: **Restricted Value** range: 0.0 -> 1.0

#### Outlier Continuous Data Threshold

Setter: `.setOutlierContinuousDataThreshold(<Int>)`  
Map Name: `'outlierContinuousDataThreshold'`

```text
Default: 50
```
Determines an exclusion filter of unique values that will be ignored if the unique count of the field's values is below the specified threshold.
> Example:

| Col1 	| Col2 	| Col3 	| Col4 	| Col5 	|
|:----:	|:----:	|:----:	|:----:	|:----:	|
|   1  	|  47  	|   3  	|   4  	|   1  	|
|   1  	|  54  	|   1  	|   0  	|  11  	|
|   1  	| 9999 	|   0  	|   0  	|   0  	|
|   0  	|   7  	|   3  	|   0  	|  11  	|
|   1  	|   1  	|   0  	|   0  	|   1  	|

> In this example data set, if the continuousDataThreshold value were to be set at 4, the ignored fields would be: Col1, Col3, Col4, and Col5.
> > Col2, having 5 unique entries, would be evaluated by the outlier filtering methodology and, provided that upper range filtering is being done, Row #3 (value entry 9999) would be filtered out with an UpperFilterNTile setting of 0.8 or lower.

#### Outlier Fields To Ignore

Setter: `.setOutlierFieldsToIgnore(<Array[String]>)`  
Map Name: `'outlierFieldsToIgnore'`

```text
Default: Array("") <empty array>
```
Optional configuration that allows certain fields to be exempt (ignored) by the outlier filtering processing.  
Any column names that are supplied to this setter will not be used for row filtering.

> NOTE: it is **highly advised** to populate this to control for 'blind filtering' of too much data.  
>> Typical use cases for this


#### Pearson Filter Statistic

Setter: `.pearsonFilterStatistic(<String>)`  
Map Name: `'pearsonFilterStatistic'`

```text
Default: "pearsonStat"

allowable values: "pvalue", "pearsonStat", or "degreesFreedom"
```

Correlation Detection between a feature value and the label value is capable of 3 supported modes:
* [Pearson Correlation](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient) ("pearsonStat")
> > Calculates the Pearson Correlation Coefficient in range of {-1.0, 1.0}
* [Degrees Freedom](https://en.wikipedia.org/wiki/Degrees_of_freedom_(statistics)#In_analysis_of_variance_(ANOVA)) ("degreesFreedom")

    Additional Reading:

    [Reduced Chi-squared statistic](https://en.wikipedia.org/wiki/Reduced_chi-squared_statistic)

    [Generalized Chi-squared distribution](https://en.wikipedia.org/wiki/Generalized_chi-squared_distribution)

    [Degrees Of Freedom](https://en.wikipedia.org/wiki/Degrees_of_freedom_(statistics)#Of_random_vectors)

> > Calculates the Degrees of Freedom of the underlying linear subspace, in unbounded range {0, n}
> > where n is the feature vector size.

*Before Overriding this value, ensure that a thorough understanding of this statistic is achieved.*

* p-value ("pvalue")

> > Calculates the p-value of independence from the Pearson chi-squared test in range of {0.0, 1.0}

#### Pearson Filter Direction

Setter: `.setPearsonFilterDirection(<String>)`  
Map Name: `'pearsonFilterDirection'`

```text
Default: "greater"

allowable values: "greater" or "lesser"
```

Specifies whether to filter values out that are higher or lower than the target cutoff value.

#### Pearson Filter Manual Value

Setter: `.setPearsonFilterManualValue(<Double>)`  
Map Name: `'pearsonFilterManualValue'`

```text
Default: 0.0

(placeholder value)
```

Allows for manually filtering based on a hard-defined limit.

Example: with .setPearsonFilterMode("manual") and .setPearsonFilterDirection("greater")
the removal of fields (columns) that have a pearson correlation coefficient result above this value will be dropped from the feature vector for modeling runs.

> Note: if using "manual" mode on this module, it is *imperative* to provide a valid value through this setter, as the Default placeholder value will
> provide poor results and it is intended to, as such, flag the user that a valid value be provided.

#### Pearson Filter Mode

Setter: `.setPearsonFilterMode(<String>)`  
Map Name: `'pearsonFilterMode'`

```text
Default: "auto"

allowable values: "auto" or "manual"
```
Determines whether a manual filter value is used as a threshold, or whether a quantile-based approach (automated)
based on the distribution of Chi-squared test results will be used to determine the threshold.

> The automated approach (using a specified NTile) will adapt to more general problems and is recommended for getting
measures of modeling feasibility (exploratory phase of modeling).  However, if utilizing the high-level API with a
well-understood data set, it is recommended to override the mode, setting it to manual, and utilizing a known
acceptable threshold value for the test that is deemed acceptable based on analysis of the feature fields and the predictor (label) column.

#### Pearson Auto Filter N Tile

Setter: `.setPearsonAutoFilterNTile(<Double>)`  
Map Name: `'pearsonAutoFilterNTile'`

```text
Default: 0.75

allowable range: 0.0 > x > 1.0

Provides the ntile threshold above or below which (depending on PearsonFilterDirection setting) fields will
be removed, depending on the distribution of pearson statistics from all feature columns.
```
([Q3 / Upper IQR value](https://en.wikipedia.org/wiki/Interquartile_range))


When in "auto" mode, this will reduce the feature vector by 75% of its total size, retaining only the 25% most
important predictive-power features of the vector.

#### Correlation (Covariance) Cutoff Low

Setter: `.setCovarianceCutoffLow(<Double>)`  
Map Name: `'covarianceCutoffLow'`

```text
Default: -0.8

Value must be set > -1.0
```
The setting at below which the right-hand comparison field will be filtered out of the data set, provided that the
pearson correlation coefficient between left->right fields is below this threshold.

> NOTE: Max supported value for `.setCovarianceCutoffLow` is -1.0

#### Correlation (Covariance) Cutoff High

Setter: `.setCovarianceCutoffHigh(<Double>)`  
Map Name: `'covarianceCutoffHigh'`

```text
Default: 0.8

Value must be set < 1.0
```
The upper positive correlation filter level.  Correlation Coefficients above this level setting will be removed from the data set.

> NOTE: Max supported value for `.setCovarianceCutoffHigh` is 1.0

#### Scaling Type

Setter: `.setScalingType(<String>)`  
Map Name: `'scalingType'`

```text
Default: "minMax"

allowable values: "minMax", "standard", "normalize", or "maxAbs"
```

Sets the scaling library to be employed in scaling the feature vector.

#### Scaler Min

Setter: `.setScalingMin(<Double>)`  
Map Name: `'scalingMin'`

```text
Default: 0.0

Only used in "minMax" mode
```

Used to set the scaling lower threshold for MinMax Scaler
(normalizes all features in the vector to set the minimum post-processed value specified in this setter)

#### Scaler Max

Setter: `.setScalingMax(<Double>)`  
Map Name: `'scalingMax'`

```text
Default: 1.0

Only used in "minMax" mode
```

Used to set the scaling upper threshold for MinMax Scaler
(normalizes all features in the vector to set the maximum post-processed value specified in this setter)

#### Scaling p-norm

Setter: `.setScalingPNorm(<Double>)`  
Map Name: `'scalingPNorm'`

```text
Default: 2.0
```
> NOTE: Only used in "normalize" mode.

> NOTE: value must be >=1.0 for proper functionality in a finite vector space.

Sets the level of "smoothing" for scaling the noise out of the vector.  

Further Reading: [P-Norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm), [L<sup>p</sup> Space](https://en.wikipedia.org/wiki/Lp_space)

#### Standard Scaler Mean Flag

Setter: `.setScalingStandardMeanFlag(<Boolean>)`  
Map Name: `'scalingStandardMeanFlag'`

```text
Default: false

Only used in "standard" mode
```

With this flag set to `true`, The features within the vector are centered around mean (0 adjusted) before scaling.
> Read the [docs](http://spark.apache.org/docs/latest/ml-features.html#standardscaler) before switching this on.
> > Setting to 'on' will create a dense vector, which will increase memory footprint of the data set.
>
#### Standard Scaler StdDev Flag

Setter: `.setScalingStdDevFlag(<Boolean>)`  
Map Name: `'scalingStdDevFlag'`

```text
Default: true for Linear Models, false for tree-based models (gets set AT RUNTIME unless explicitly turned off for linear models)
```
> NOTE: Only used in "standard" mode

Scales the data to the unit standard deviation. [Explanation](https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation)


#### Feature Interaction Retention Mode

Setter: `.setFeatureInteractionRetentionMode(<String>)`  
Map Name: `'featureInteractionRetentionMode'`

```text
 Setter for determining the mode of operation for inclusion of interacted features.
 Modes are:
  - all -> Includes all interactions between all features (after string indexing of categorical values)
  - optimistic -> If the Information Gain / Variance, as compared to at least ONE of the parents of the interaction
      is above the threshold set by featureInteractionTargetInteractionPercentage
      (e.g. if IG of left parent is 0.5 and right parent is 0.9, with threshold set at 10, if the interaction
      between these two parents has an IG of 0.42, it would be rejected, but if it was 0.46, it would be kept)
  - strict -> the threshold percentage must be met for BOTH parents.
      (in the above example, the IG for the interaction would have to be > 0.81 in order to be included in
      the feature vector).

Default: 'optimistic'
```

#### Feature Interaction Continuous Discretizer Bucket Count

Setter: `.setFeatureInteractionContinuousDiscretizerBucketCount(<Int>)`  
Map Name: `'featureInteractionContinuousDiscretizerBucketCount'`

```text
Setter for determining the behavior of continuous feature columns.  In order to calculate Entropy for a continuous
variable, the distribution must be converted to nominal values for estimation of per-split information gain.
This setting defines how many nominal categorical values to create out of a continuously distributed feature
in order to calculate Entropy.

Default: 10
```

> Note: must be greater than 1

#### Feature Interaction Parallelism

Setter: `.setFeatureInteractionParallelism(<Int>)`  
Map Name: `'featureInteractionParallelism'`

```text
Setter for configuring the concurrent count for scoring of feature interaction candidates.
Due to the nature of these operations, the configuration here may need to be set differently to that of
the modeling and general feature engineering phases of the toolkit.  This is highly dependent on the row
count of the data set being submitted.

Default: 12
```
> NOTE: must be greater than 0
>> It is recommended to decrease this value for larger data sets to avoid overwhelming the executor thread pools or filling the Heap too quickly.

#### Feature Interaction Target Interaction Percentage

Setter: `.setFeatureInteractionTargetInteractionPercentage(<Double>)`  
Map Name: `'featureInteractionTargetInteractionPercentage'`

```text
Establishes the minimum acceptable InformationGain or Variance allowed for an interaction
candidate based on comparison to the scores of its parents.  This value is a 'reduction from matched' in that
a value of 0.1 here would mean that children candidates that are at least 90% of the IG value of parents would be
included in the final feature vector.

Default: 10.0
```

#### Feature Importance Cutoff Type

Setter: `.setFeatureImportanceCutoffType(<String>)`  
Map Name: `'featureImportanceCutoffType'`

```text
Setting for determining where to limit the feature vector after completing a feature importances run in order to return
either the top n most important features, or the top features above a specific relevance score cutoff.

modes: 'none', 'value', or 'count'

Default: 'count'
```

#### Feature Importance Cutoff Value

Setter: `.setFeatureImportanceCutoffValue(<Double>)`  
Map Name: `'featureImportanceCutoffValue'`

```text
Restrictive filtering limit on either counts of fields (if feature importance cutoff type is in 'count' mode) ranked,
or direct value of feature importance.

WARNING: depending on the algorithm used to calculate feature importances, operating in 'value' mode is different for
XGBoost vs. RandomForest since their scoring methodologies are different.  Please see respective API docs for
XGBoost and Spark RandomForest to get an understanding of how these are calculated before attempting 'value' mode.

Default: 15.0
```

#### Data Reduction Factor

Setter: `.setDataReductionFactor(<Double>)`  
Map Name: `'dataReductionFactor'`

```text
Testing feature for validating large runs on a smaller subset of data (DEV API ONLY)
Will reduce the size of the data set by the value provided, if set.  

Default: 0.5 (will drop half of the rows)
```

> NOTE: must in range 0 to 1.
> WARNING: not recommended for use in actual training runs or in production.  This removes data from the training data
>set indiscriminantly, and as such, cannot guarantee effective preservation of underlying distributions or class balance.


#### Fill Config Cardinality Switch

Setter: `.setFillConfigCardinalitySwitch(<Boolean>)`  
Map Name: `'fillConfigCardinalitySwitch'`

```text
Toggles the checking for whether to treat a field as nominal or continuous based on the distinct counts.
This is important for nominal data that, even though numeric, should be handles as a categorical-like value.
Fields that fall below the cardinality limit will be handled in the same way as StringType fields
(utilizing max or min fill rather than mean or quantile fill)

Default: true (on)
```
> Note: it is recommended to leave this feature ON unless it is absolutely known that all numeric values in the data set
>should be handled as though they were continuous values.

#### Fill Config Cardinality Type

Setter: `.setfFillConfigCardinalityType(<String>)`  
Map Name: `'fillConfigCardinalityType'`

```text
Configuration for how cardinality is calculated, either 'approx' or 'exact'

Default: 'exact'
```
> NOTE: setting 'exact' on extremely large data sets will incur large waits as data is serialized to get counts.

#### Fill Config Cardinality Limit

Setter: `.setFillConfigCardinalityLimit(<Int>)`  
Map Name: `'fillConfigCardinalityLimit'`

```text
The cardinality threshold for use if the fill config cardinality switch is turned on - this is the value that distinct
counts below which will be considered to be 'nominal' and handled as a categorical fill value.

Default: 200
```

#### Fill Config Cardinality Precision

Setter: `.setFillConfigCardinalityPrecision(<Double>)`  
Map Name: `'fillConfigCardinalityPrecision'`

```text
Precision value for 'approx' mode on fill config cardinality type

Must be in range >0 to 1

Default: 0.05
```

#### Fill Config Cardinality Check Mode

Setter: `.setFillConfigCardinalityCheckMode(<String>)`  
Map Name: `'fillConfigCardinalityCheckMode'`

```text
Setter for the cardinality check mode to be used.  Available modes are "warn" and "silent".
- In "warn" mode, an exception will be thrown if the cardinality for a categorical column is above the threshold.
- In "silent" mode, the field will be ignored from processing and will not be included in the feature vector.

Default: 'silent"
```

#### Fill Config Filter Precision

Setter: `.setFillConfigFilterPrecision(<Double>)`  
Map Name: `'fillConfigFilterPrecision'`

```text
Setter for defining the precision for calculating the model type as per the label column

Must be in range 0 to 1
```
> NOTE: setting this value to zero (0) for a large regression problem will incur a long processing time and an expensive shuffle.

#### Fill Config Categorical NA Fill Map

Setter: `.setFillConfigCategoricalNAFillMap(<Map[String, String]>)`  
Map Name: `'fillConfigCategoricalNAFillMap'`

```text
A means of directly controlling at a column-level distinct overrides to columns for na fill of categorical data for StringType columns. The structure is of
Column Name -> fill value
This will function only on non-numeric value type columns and the data will be cast as a String, regardless of the input
data type that is applied in the Map.

Default: Map.empty[String, String] (empty Map)
```

#### Fill Config Numeric NA Fill Map

Setter: `.setFillConfigNumericNAFillMap(<Map[String, Double]>)`  
Map Name: `'fillConfigNumericNAFillMap'`

```text
A means of directly controlling at a column-level distinct overrides to columns for na fill for numeric Type columns
(all columns get cast to DoubleType throughout modeling anyway). The structure is of
Column Name -> fill value
This will function only numeric value type columns and the data will be cast as a Double, regardless of the input
data type that is applied in the Map. i.e. to fill with Int 1, simply write as a Double 1.0

Default: Map.empty[String, Double] (empty Map)
```

#### Fill Config Character NA Blanket Fill Value

Setter: `.setFillConfigCharacterNABlanketFillValue(<String>)`  
Map Name: `'fillConfigCharacterNABlanketFillValue'`

```text
Sets the ability to fill all categorical (StringType) columns in the data set to the same na fill replacement value.

Default: ""
```
> Note - only recommended for certain ML applications.  Not advised for most.

#### Fill Config Numeric NA Blanket Fill Value

Setter: `.setFillConfigNumericNABlanketFillValue(<Double>)` 
 Map Name: `'fillConfigNumericNABlanketFillValue'`

```text
Sets the ability to fill all numeric columns in the data set to the same na fill value.

Default: 0.0
```
> Note - not recommended, but included as a feature for certain older application needs from legacy migrations.

#### Fill Config NA Fill Mode

Setter: `.setFillConfigNAFillMode(<String>)`  
Map Name: `'fillConfigNAFillMode'`

```text
 Mode for na fill
  Available modes:
   - auto: Stats-based na fill for fields.  Usage of .setNumericFillStat and
     .setCharacterFillStat will inform the type of statistics that will be used to fill.
   - mapFill: Custom by-column overrides to 'blanket fill' na values on a per-column
      basis.  The categorical (string) fields are set via .setCategoricalNAFillMap while the
      numeric fields are set via .setNumericNAFillMap.<br>
   - blanketFillAll: Fills all fields based on the values specified by
      .setCharacterNABlanketFillValue and .setNumericNABlanketFillValue.  All NA's for the
      appropriate types will be filled in accordingly throughout all columns.
   - blanketFillCharOnly: Will use statistics to fill in numeric fields, but will replace
     all categorical character fields na values with a blanket fill value.
   - blanketFillNumOnly:  Will use statistics to fill in character fields, but will replace
     all numeric fields na values with a blanket value.

Default: 'auto'
```




### Tuner Config
```scala
case class TunerConfig(var tunerAutoStoppingScore: Double,
                       var tunerParallelism: Int,
                       var tunerKFold: Int,
                       var tunerTrainPortion: Double,
                       var tunerTrainSplitMethod: String,
                       var tunerKSampleSyntheticCol: String,
                       var tunerKSampleKGroups: Int,
                       var tunerKSampleKMeansMaxIter: Int,
                       var tunerKSampleKMeansTolerance: Double,
                       var tunerKSampleKMeansDistanceMeasurement: String,
                       var tunerKSampleKMeansSeed: Long,
                       var tunerKSampleKMeansPredictionCol: String,
                       var tunerKSampleLSHHashTables: Int,
                       var tunerKSampleLSHSeed: Long,
                       var tunerKSampleLSHOutputCol: String,
                       var tunerKSampleQuorumCount: Int,
                       var tunerKSampleMinimumVectorCountToMutate: Int,
                       var tunerKSampleVectorMutationMethod: String,
                       var tunerKSampleMutationMode: String,
                       var tunerKSampleMutationValue: Double,
                       var tunerKSampleLabelBalanceMode: String,
                       var tunerKSampleCardinalityThreshold: Int,
                       var tunerKSampleNumericRatio: Double,
                       var tunerKSampleNumericTarget: Int,
                       var tunerTrainSplitChronologicalColumn: String,
                       var tunerTrainSplitChronologicalRandomPercentage: Double,
                       var tunerSeed: Long,
                       var tunerFirstGenerationGenePool: Int,
                       var tunerNumberOfGenerations: Int,
                       var tunerNumberOfParentsToRetain: Int,
                       var tunerNumberOfMutationsPerGeneration: Int,
                       var tunerGeneticMixing: Double,
                       var tunerGenerationalMutationStrategy: String,
                       var tunerFixedMutationValue: Int,
                       var tunerMutationMagnitudeMode: String,
                       var tunerEvolutionStrategy: String,
                       var tunerGeneticMBORegressorType: String,
                       var tunerGeneticMBOCandidateFactor: Int,
                       var tunerContinuousEvolutionImprovementThreshold: Int,
                       var tunerContinuousEvolutionMaxIterations: Int,
                       var tunerContinuousEvolutionStoppingScore: Double,
                       var tunerContinuousEvolutionParallelism: Int,
                       var tunerContinuousEvolutionMutationAggressiveness: Int,
                       var tunerContinuousEvolutionGeneticMixing: Double,
                       var tunerContinuousEvolutionRollingImprovingCount: Int,
                       var tunerModelSeed: Map[String, Any],
                       var tunerHyperSpaceInference: Boolean,
                       var tunerHyperSpaceInferenceCount: Int,
                       var tunerHyperSpaceModelCount: Int,
                       var tunerHyperSpaceModelType: String,
                       var tunerInitialGenerationMode: String,
                       var tunerInitialGenerationPermutationCount: Int,
                       var tunerInitialGenerationIndexMixingMode: String,
                       var tunerInitialGenerationArraySeed: Long,
                       var tunerOutputDfRepartitionScaleFactor: Int,
                       var tunerDeltaCacheBackingDirectory: String,
                       var tunerDeltaCacheBackingDirectoryRemovalFlag: Boolean,
                       var splitCachingStrategy: String)
```


#### Tuner Auto Stopping Score

Setter: `.setTunerAutoStoppingScore(<Double>)`  
Map Name: `'tunerAutoStoppingScore'`

```text
Setting for specifying the early stopping value.  

Default: 0.95
```
> NOTE: Ensure that the value specified matches the optimization score set in `.setScoringMetric(<String>)`
>> i.e. if using f1 score for a classification problem, an appropriate early stopping score might be in the range of {0.92, 0.98}

[WARNING] This value is set as a placeholder for a default classification problem.  If using regression, this will ***need to be changed***

#### Tuner Parallelism

Setter: `.setTunerParallelism(<Int>)`  
Map Name: `'tunerParallelism'`

```text
Means for setting the number of asynchronous models that are executed concurrently within the generational genetic algorithm.
Feeds into the equations for determining appropriate repartitions based on cluster size and available executor CPU's to tune the run appropriately.

Default: 20
```

Sets the number of concurrent models that will be evaluated in parallel through Futures.  
This creates a new [ForkJoinPool](https://java-8-tips.readthedocs.io/en/stable/forkjoin.html), and as such, it is important to not set it too high, in order to prevent overloading
the driver JVM with too many elements in the `Array[DEqueue[task]]` ForkJoinPool.  
NOTE: There is a global limit of 32767 threads on the JVM, as well.
However, in practice, running too many models in parallel will likely OOM the workers anyway.  

> NOTE: highly recommended to override this value as the default value is simply a placeholder.
> NOTE values set above 30 will receive a WARNING stating that the number of concurrent tasks is likely beyond an efficient ratio of
>avalable cluster resources and the benefits of asynchronous tuning.
> NOTE: different models have different characteristics and behavior.  It is recommended that tree-based (non XGBoost) use a relatively low value here
> (~4-6), while linear models can run at much higher levels and benefit from the higher concurrency.

#### Tuner Kfold

Setter: `.setTunerKFold(<Int>)`  
Map Name: `'tunerKFold'`

```text
Sets the number of different splits that are happening on the pre-modeled data set for train and test, allowing for
testing of different splits of data to ensure that the hyper parameters under test are being evaluated for different mixes of the data.
This value dictates the number of copies of the data that will exist either cached, persisted, or written to temporary delta tables during the
modeling phase.

Default: 5
```

> Note a warning will appear if the kFold count is lower than recommended to prevent selecting hyper parameters that may overfit to one particular split of
>the data set into train / test.

#### Tuner Train Portion

Setter: `.setTunerTrainPortion(<Double>)`  
Map Name: `'tunerTrainPortion'`

```text
Sets the proportion of the input DataFrame to be used for Train (the value of this variable) and Test
(1 - the value of this variable)

Default: 0.8
```
> NOTE: restricted to between 0 and 1 (highly recommended to stay in the range of 0.5 to 0.9)


#### Train Split Method

Setter: `.setTunerTrainSplitMethod(<String>)`  
Map Name: `'tunerTrainSplitMethod'`

This setting allows for specifying how to split the provided data set into test/training sets for scoring validation.
Some ML use cases are highly time-dependent, even in traditional ML algorithms (i.e. predicting customer churn).  
As such, it is important to be able to predict on apriori data and synthetically 'predict the future' by doing validation
testing on a holdout data set that is more recent than the training data used to build the model.

Additional reading: [Sampling in Machine Learning](https://en.wikipedia.org/wiki/Oversampling_and_undersampling_in_data_analysis)

Setter: `.setTrainSplitMethod(<String>)`
```text
Available options: "random" or "chronological" or "stratified" or "underSample" or "overSample" or "kSample"

Default: "random"

```
Chronological Split Mode
- Splits train / test between a sortable field (date, datetime, unixtime, etc.)
> Chronological split method **does not require** a date type or datetime type field. Any sort-able / continuous distributed field will work.

Random Split Mode
- Leaving the default value of "random" will randomly shuffle the train and test data sets each k-fold iteration.

***This is only recommended for classification data sets where there is relatively balanced counts of unique classes in the label column***
> [NOTE]: attempting to run any mode other than "random" or "chronological" on a regression problem will not work.  Default behavior
will reset the trainSplitMethod to "random" if they are selected on a regression problem.

Stratified Mode

- Stratified mode will balance all of the values present in the label column of a classification algorithm so that there
is adequate coverage of all available labels in both train and test for each kfold split step.

***It is HIGHLY RECOMMENDED to use this mode if there is a large skew in your label column (class imbalance) and there is a need for
training on the full, unmanipulated data set.***

UnderSampling Mode

- Under sampling will evaluate the classes within the label column and sample all classes within each kfold / model run
to target the row count of the smallest class. (Only recommended for moderate skew cases)

***If using this mode, ensure that the smallest class has sufficient row counts to make training effective!***

OverSampling Mode [NOT RECOMMENDED FOR GENERAL USE]

- Over sampling will evaluate the class with the highest count of entries and during splitting, will replicate all
other classes' data to *generally match* the counts of the most frequent class. (**this is not exact and can vary from
run to run**)

***WARNING*** - using this mode will dramatically increase the training and test data set sizes.  
***Small count classes' data will be copied multiple times to eliminate skew***

KSampling Mode
- Uses a distributed implementation of [SMOTE](https://en.wikipedia.org/wiki/Oversampling_and_undersampling_in_data_analysis#SMOTE) applied to
the minority class(es) for the training split only (test is not augmented).  
At it's core, the algorithm is building k clusters based on the feature vector.  From these cluster centroids, 
a MinHashLSH model is built to perform distance calculations from the centroids to each of the members of the cluster
centroid.  The collection of candidate points are then sorted based on the distance metric, at which point random
numbers of vector index values are mutated between the adjacent points near the centroid and the centroid value 
itself.  These synthetic feature elements are then vectorized, labeled as 'synthetic' and used to augment the 
minority classes present in the unbalanced classification training set.  
> NOTE: The synthetic data IS NOT INCLUDED IN THE TEST SETS for scoring of models. 

> NOTE: as this is an ML-based augmentation system, there is considerable time and resources involved in creating 'intelligent'
>over-sampling of the minority class(es), which will occur prior to the modeling phase.

#### Tuner KSample Synthetic Col

Setter: `.setTunerKSampleSyntheticCol(<String>)`  
Map Name: `'tunerKSampleSyntheticCol'`

```text
Internal temporary column name denoting normal vs synthetic rows of data to ensure that a train/test split will not
include synthetic data in the test data set (which would invalidate the model's scoring)

Default: 'synthetic_ksample'
```
> NOTE: the name can be anything, provided it isn't the same as a name in the data set already, or a reserved field name (i.e. 'features' or 'label')

#### Tuner KSample K Groups

Setter: `.setTunerKSampleKGroups(<Int>)`  
Map Name: `'tunerKSampleKGroups'`

```text
Specifies the number of K clusters to generate for the synthetic data generation for minority classes

Default: 25
```
> NOTE: placeholder value derived at based on general testing.  Feel free to override as needed.

#### Tuner KSample KMeans MaxIter

Setter: `.setTunerKSampleKMeansMaxIter(<Int>)`  
Map Name: `'tunerKSampleKMeansMaxIter'`

```text
Specifies the maximum number of iterations for the KMeans model to attempt to converge

Default: 100
```
> NOTE: not recommended to override unless needed based on cardinality complexity of the features involved. (DEV API)

#### Tuner KSample KMeans Tolerance

Setter: `.setTunerKSampleKMeansTolerance(<Double>)`  
Map Name: `'tunerKSampleKMeansTolerance'`

```text
KMeans setting for determining the tolerance value for convergence.

Default: 1e-6
```

> NOTE Must be greater than 0.

See [DOC](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.clustering.KMeans) for further details.

#### Tuner KSample KMeans Distribution Measurement

Setter: `.setTunerKSampleKMeansDistanceMeasurement(<String>)`  
Map Name: `'tunerKSampleKMeansDistanceMeasurement'`

```text
Which distance measurement to use for generation of synthetic data.

Either 'euclidean' or 'cosine'

Default: 'euclidean'
```

#### Tuner KSample KMeans Seed

Setter: `.setTunerKSampleKMeansSeed(<Long>)`  
Map Name: `'tunerKSampleKMeansSeed'`

```text
The seed for KMeans to attempt to create a somewhat repeatable convergence for a particular data set.

Default: 42L
```

#### Tuner KSample KMeans Prediction Col

Setter: `.setTunerKSampleKMeansPredictionCol(<String>)`  
Map Name: `'tunerKSampleKMeansPredictionCol'`

```text
Internal use only column name for KSampling internal processes.  

Default: 'kGroups_ksample'
```
> NOTE: ensure that reserved field names are not used, nor a field name that is present in the raw data set.

#### Tuner KSample LSH Hash Tables

Setter: `.setTunerKSampleLSHHashTables(<Int>)`  
Map Name: `'tunerKSampleLSHHashTables'`

```text
Sets the number of hash tables involved in the jaccard distance calculations in MinHashLSH

Default: 10
```

For further reading, review the [docs and links](http://spark.apache.org/docs/latest/ml-features.html#minhash-for-jaccard-distance)

#### Tuner KSample LSH Seed

Setter: `.setTunerKSampleLSHSeed(<Long>)`  
Map Name: `'tunerKSampleLSHSeed'`

```text
Seed for the MinHashLSH algorithm for repeatability.

Default: 42L
```

#### Tuner KSample LSH Output Col

Setter: `.setTunerKSampleLSHOutputCol(<String>)`  
Map Name: `'tunerKSampleLSHOutputCol'`

```text
Internal use only column name for MinHashLSH processes.

Default: 'hashes_ksample'
```
> NOTE: ensure that reserved field names are not used or any name of a field in the raw data set.

#### Tuner KSample Quorum Count

Setter: `.setTunerKSampleQuorumCount(<Int>)`  
Map Name: `'tunerKSampleQuorumCount'`

```text
Setting for how many vectors to include in adjacency calculations to the centroid position of the K-cluster
for the generation of synthetic data.

Larger values will get more dynamic synthetic data, however, will incur additional runtime processing.

Default: 7
```

#### Tuner KSample Minimum Vector Count to Mutate

Setter: `.setTunerKSampleMinimumVectorCountToMutate(<Int>)`  
Map Name: `'tunerKSampleMinimumVectorCountToMutate'`

```text
Minimum threshold value for vector indeces to mutate within the feature vector during synthetic data generation.

Higher values will result in more data variation, but potentially will create more of a challenge to converge during training.

Default: 1
```

> NOTE: random selection between this number and the total number of features within the vector will be used, depending on the mutation mode.
> highly recommended to override if using 'fixed' mode on vector mutation method.

#### Tuner KSample Vector Mutation Method

Setter: `.setTunerKSampleVectorMutationMethod(<String>)`  
Map Name: `'tunerKSampleVectorMutationMethod'`

```text
One of three modes:
"fixed" - will use the value of minimumVectorCountToMutate to select random indexes of this number of indexes.
"random" - will use this number as a lower bound on a random selection of indexes between this and the vector length.
"all" - will mutate all of the vectors.

Default: 'random'
```

#### Tuner KSample Mutation Mode

Setter: `.setTunerKSampleMutationMode(<String>)`  
Map Name: `'tunerKSampleMutationMode'`

```text
Defines the method of mixing of the Vector positions selected to the centroid position.
Available modes:
"weighted" - uses weighted averaging to scale the euclidean distance between the centroid vector and mutation candidate vectors
"random" - randomly selects a position on the euclidean vector between the centroid vector and the candidate mutation vectors
"ratio" - uses a ratio between the values of the centroid vector and the mutation vector

Default: 'weighted'
```

#### Tuner KSample Mutation Value

Setter: `.setTunerKSampleMutationValue(<Double>)`  
Map Name: `'tunerKSampleMutationValue'`

```text
Specifies the magnitude of mixing in 'weighted' or 'ratio' modes of mutation mode.

Default: 0.5
```
> NOTE: must be set within a range of 0 and 1

#### Tuner KSample Label Balance Mode

Setter: `.setTunerKSampleLabelBalanceMode(<String>)`  
Map Name: `'tunerKSampleLabelBalanceMode'`

```text
Split methodology for the KSample methodology.  
Available Modes:
'match': Will match all smaller class counts to largest class count.  [WARNING] - May significantly increase memory pressure!
'percentage': Will adjust smaller classes to a percentage value of the largest class count.
'target': Will increase smaller class counts to a fixed numeric target of rows.

Default: 'match'
```

#### Tuner KSample Cardinality Threshold

Setter: `.seTunerKSampleCardinalityThreshold(<Int>)`  
Map Name: `'tunerKSampleCardinalityThreshold'`

```text
Cardinality check threshold for determining if synthetic data should be resolved on an ordinal scale (rounded / floored)
or left as a continuous data point

Default: 20
```

#### Tuner KSample

Setter: `.setTunerKSampleNumericRatio(<Double>)`  
Map Name: `'tunerKSampleNumericRatio'`

```text
For Percentage mode on setTunerKSampleLabelBalanceMode()
Defines the percentage target to match to based on the majority class.

Default: 0.2
```
> NOTE: must be between 0 and 1 (recommend setting this based on analysis of the class imbalance and the total data size)

#### Tuner KSample

Setter: `.setTunerKSampleNumericTarget(<Int>)`  
Map Name: `'tunerKSampleNumericTarget'`

```text
For Target mode on setTunerKSampleLabelBalanceMode()
Defines a target number of rows for the minority class(es) to reach (real and synthetic data together)

Default: 500
```
> NOTE: highly recommended to use if the data set is small.  If large, use ratio or match settings.


#### Train Split Chronological Column

Specify the field to be used in restricting the train / test split based on sort order and percentage of data set to conduct the split.
> As specified above, there is no requirement that this field be a date or datetime type.  However, it *is recommended*.

Setter: `.setTunerTrainSplitChronologicalColumn(<String>)`  
Map Name: `'tunerTrainSplitChronologicalColumn'`

```text
Default: "datetime"

This is a placeholder value.
Validation will occur when modeling begins (post data-prep) to ensure that this field exists in the data set.
```
> It is ***imperative*** that this field exists in the raw DataFrame being supplied to the main class. ***CASE SENSITIVE MATCH***
> > Failing to ensure this setting is correctly applied could result in an exception being thrown mid-run, wasting time and resources.

#### Train Split Chronological Random Percentage

Due to the fact that a Chronological split, when done by a sort and percentage 'take' of the DataFrame, each k-fold
generation would extract an identical train and test data set each iteration if the split were left static.  This
setting allows for a 'jitter' to the train / test boundary to ensure that k-fold validation provides more useful results.

Setter: `.setTunerTrainSplitChronologicalRandomPercentage(<Double>)` 
Map Name: `'tunerTrainSplitChronologicalRandomPercentage'`

```text

representing the percentage value (fractional * 100)
Default: 0.0

This is a placeholder value.
```
> [WARNING] Failing to override this value if using "chronological" mode on `.setTunerTrainSplitMethod()` is equivalent to setting
`.setTunerKFold(1)` for efficacy purposes, and will simply waste resources by fitting multiple copies of the same hyper
parameters on the exact same data set.

#### Tuner Seed

Setter: `.setTunerSeed(<Long>)`  
Map Name: `'tunerSeed'`

```text
Sets the seed for both random selection generation for the initial random pool of values, as well as initializing
another randomizer for random train/test splits.

Default: 42L
```

#### Tuner First Generation Gene Pool

Setter: `.setTunerFirstGenerationGenePool(<Int>)`  
Map Name: `'tunerFirstGenerationGenePool'`

```text
Determines the random search seed pool for the genetic algorithm to operate from.  
There are space constraints on numeric hyper parameters, character, and boolean that are distinct for each modeling
family and model type.
Setting this value higher increases the chances of minimizing convergence, at the expense of a longer run time.

Default: 20
```
> NOTE: Setting this value below 10 is ***not recommended***.  Values less than 6 are not permitted and will throw an assertion exception.

#### Tuner Number of Generations

Setter: `.setTunerNumberOfGenerations(<Int>)`  
Map Name: `'tunerNumberOfGenerations'`

```text
This setting, applied only to batch processing mode, sets the number of mutation generations that will occur.

The higher this number, the better the exploration of the hyper parameter space will occur, although it comes at the
expense of longer run-time.  This is a *sequential blocking* setting.  Parallelism does not effect this.

Default: 10
```
> NOTE: batch mode only!

#### Tuner Number of Parents To Retain

Setter: `.setTunerNumberOfParentsToRetain(<Int>)`  
Map Name: `tunerNumberOfParentsToRetain`

```text
This setting will restrict the number of candidate 'best' results of the previous generation of hyper parameter tuning,
using these result's configuration to mutate the next generation of attempts.

Default: 3
```

> NOTE: The higher this setting, the more 'space exploration' that will occur.  However, it may slow the possibility of
        converging to an optimal condition.


#### Tuner Number of Mutations Per Generation

Setter: `.setTunerNumberOfMutationsPerGeneration(<Int>)`  
Map Name: `tunerNumberOfMutationsPerGeneration`

```text
This setting specifies the size of each evolution batch pool per generation (other than the first seed generation).

Default: 10
```
> The higher this setting is set, the more alternative spaces are checked, however, if this value is higher than
what is set by `.setTunerParallelism()`, it will add to the run-time.

#### Tuner Genetic Mixing

Setter: `.setTunerGeneticMixing(<Double>)`  
Map Name: `'tunerGeneticMixing'`

```text
This setting defines the ratio of impact that the 'best parent' that is used to mutate with a new randomly generated
child will have upon the mixed-inheritance hyper parameter.  The higher this number, the more effect from the parent the parameter will have.

Default: 0.7

Recommended range: {0.3, 0.8}
```

> NOTE: Setting this value < 0.1 is effectively using random parameter replacement.  
Conversely, setting the value > 0.9 will not mutate the next generation strongly enough to effectively search the parameter space.


#### Tuner Generational Mutation Strategy

Setter: `.setTunerGenerationalMutationStrategy(<Double>)`  
Map Name: `'tunerGenerationalMutationStrategy'`

```text
Provides for one of two modes:
* Linear
> This mode will decrease the number of selected hyper parameters that will be mutated each generation.
It is set to utilize the fixed mutation value as a decrement reducer.

        Example:

        A model family is selected that has 10 total hyper parameters.

        In "linear" mode for the generational mutation strategy, with a fixed mutation value of 1,
        the number of available mutation parameters for the first mutation generation would be
        set to a maximum value of 9 (randomly selected in range of {1, 9}).
        At generation #2, the maximum mutation count for hyper parameters in the vector space
        will decrememt, leaving a range of randomly selected random hyper parameters of {1, 8}.
        This behavior continues until either the decrement value is 1 or the generations are exhausted.

* Fixed
> This mode sets a static mutation count for each generation.  The setting of Fixed Mutation Value
determines how many of the hyper parameters will be mutated each generation.  There is no decrementing.

Default: "linear"

Available options: "linear" or "fixed"
```
> NOTE: fixed mode may introduce very long training times in order to explore the hyper parameter space effectively.
>Only use 'fixed' mode when fine-tuning an already existing model with small amounts of new training data.

#### Tuner Fixed Mutation Value

Setter: `.setTunerFixedMutationValue(<Int>)`  
Map Name: `'tunerFixedMutationValue'`

```text
Allows for restricting the number of hyper parameters to mutate per generational epoch.

Default: 1
```
> NOTE: using this setting and keeping the value low will DRAMATICALLY increase the time to convergence.
> only recommended for fine-tuning of an already existing use-case in which a small exploration of fine tuning
> is desired.

#### Tuner Mutation Magnitude Mode

Setter: `.setTunerMutationMagnitudeMode(<String>)`  
Map Name: `'tunerMutationMagnitudeMode'`

```text

This setting determines the number of hyper parameter values that will be mutated during each mutation iteration.

There are two modes:
* "random"

> In random mode, the setting of `.setGenerationalMutationStrategy()` is used, in conjunction with
the current generation count, to provide a bounded restriction on the number of hyper parameters
per model configuration that will be mutated.  A Random number of indeces will be selected for
mutation in this range.

* "fixed"

> In fixed mode, a constant count of hyper parameters will be mutated, used in conjunction with
the setting of .`setGenerationalMutationStrategy()`.
>> i.e. With fixed mode, and a generational mutation strategy of "fixed", each mutation generation
would be static (e.g. fixedMutationValue of 3 would mean that each model of each generation would
always mutate 3 hyper parameters).  Variations of the mixing of these configurations will result in
varying degrees of mutation aggressiveness.

Default: "fixed"

Available options: "fixed" or "random"
```

#### Tuner Evolution Strategy

Setter: `.setTunerEvolutionStrategy(<String>)`  
Map Name: `'tunerEvolutionStrategy'`

```text
Determining the mode (batch vs. continuous)

In batch mode, the hyper parameter space is explored with an initial seed pool (based on Random Search with constraints).

After this initial pool is evaluated (in parallel), the best n parents from this seed generation are used to 'sire' a new generation.

This continues for as many generations are specified through the config `.setTunerNumberOfGenerations(<Int>)`,
*or until a stopping threshold is reached at the conclusion of a concurrent generation batch run.*


Continuous mode uses the concept of micro-batching of hyper parameter tuning, running *n* models
in parallel.  When each Future is returned, the evaluation of its performance is made, compared to
the current best results, and a new hyper parameter run is constructed.  Since this is effectively
a queue/dequeue process utilizing concurrency, there is a certain degree of out-of-order process
and return.  Even if an early stopping criteria is met, the thread pool will await for all committed
Futures to return their values before exiting the parallel concurrent execution context.


Default: "batch" (HIGHLY recommended)

Available options: "batch" or "continuous"
```
> Sets the mutation methodology used for optimizing the hyper parameters for the model.
>> NOTE! Continuous mode is 'experimental' and may be slightly unstable depending on the data set used in training.

#### Tuner Genetic MBO Regressor Type

Setter: `.setTunerGeneticMBORegressorType(<String>)`  
Map Name: `'tunerGeneticMBORegressorType'`

```text
The post-genetic algorithm stage consists of running MBO (Model-based optimization) on apriori hyper parameters
to score associations.  

This setting allows for a choice between XGBoost or RandomForest or LinearRegression

Default 'XGBoost'
```

#### Tuner Genetic MBO Candidate Factor

Setter: `.setTunerGeneticMBOCandidateFactor(<Int>)`  
Map Name: `'tunerGeneticMBOCandidateFactor'`

```text
Setter for defining the factor to be applied to the candidate listing of hyperparameters to generate through
mutation for each generation other than the initial and post-modeling optimization phases.  The larger this
value (default: 10), the more potential space can be searched.  There is not a large performance hit to this,
and as such, values in excess of 100 are viable.

Default: 10
```  

#### Tuner Continuous Evolution Improvement Threshold [EXPERIMENTAL]

Setter: `.setTunerContinuousEvolutionImprovementThreshold(<Int>)`  
Map Name: `'tunerContinuousEvolutionImprovementThreshold'`

```text
Setter for defining the secondary stopping criteria for continuous training mode ( number of consistently
not-improving runs to terminate the learning algorithm due to diminishing returns.
(an improvement to a priori will reset the counter and subsequent non-improvements
will decrement a mutable counter.  If the counter hits this limit specified in value, the continuous
mode algorithm will stop).

Default -10
```
> NOTE: must be a negative Integer!

#### Tuner Continuous Evolution Max Iterations [EXPERIMENTAL]

Setter: `.setTunerContinuousEvolutionMaxIterations(<Int>)`  
Map Name: `'tunerContinuousEvolutionMaxIterations'`

```text
This parameter sets the total maximum cap on the number of hyper parameter tuning models that are
created and set to run.  The higher the value, the better the chances for convergence to optimum
tuning criteria, at the expense of runtime.


Default: 200
```

#### Tuner Continuous Evolution Stopping Score [EXPERIMENTAL]  

[OVERRIDE WARNING]

Setter: `.setTunerContinuousEvolutionStoppingScore(<Double>)`  
Map Name: `'tunerContinuousEvolutionStoppingScore'`

**NOTE**: ***This value MUST be overridden in regression problems***

```text
Setting for early stopping.  When matched with the score type, this target is used to terminate the hyper parameter
tuning run so that when the threshold has been passed, no additional Futures runs will be submitted to the concurrent
queue for parallel processing.

Default: 1.0

This is a placeholder value.  Ensure it is overriden for early stopping to function in classification problems.
```
> NOTE: The asynchronous nature of this algorithm will have additional results potentially return after a stopping
criteria is met, since Futures may have been submitted before the result of a 'winning' run has returned.  
> > This is intentional by design and does not constitute a bug.

#### Tuner Continuous Evolution Parallelism [EXPERIMENTAL]

Setter: `.setTunerContinuousEvolutionParallelism(<Int>)`  
Map Name: `'tunerContinuousEvolutionParallelism'`

```text
This setting defines the number of concurrent Futures that are submitted in continuous mode.  Setting this number too
high (i.e. > 5) will minimize / remove the functionality of continuous processing mode, as it will begin to behave
more like a batch mode operation.

Default: 4
```
> TIP: **Recommended value range is {2, 5}** to see the greatest exploration benefit of the n-dimensional hyper
parameter space, with the benefit of run-time optimization by parallel async execution.


#### Tuner Continuous Evolution Mutation Aggressiveness [EXPERIMENTAL]

Setter: `.setTunerContinousEvolutionMutationAggressiveness(<Int>)`  
Map Name: `'tunerContinuousEvolutionMutationAggressiveness'`

```text
Similar to the batch mode setting `.setFixedMutationValue()`; however, there is no concept of a 'linear' vs 'fixed'
setting.  There is only a fixed mode for continuous processing.  This sets the number of hyper parameters that will
be mutated during each async model execution.

Default: 3
```
> The higher the setting of this value, the more the feature space will be explored; however, the longer it may take to
converge to a 'best' tuned parameter set.

> The recommendation is, for **exploration of a modeling task**, to set this value ***higher***.  If trying to fine-tune a model,
or to automate the **re-tuning of a production model** on a scheduled basis, setting this value ***lower*** is preferred.

#### Tuner Continuous Evolution Genetic Mixing [EXPERIMENTAL]

Setter: `.setTunerContinuousEvolutionGeneticMixing(<Double>)`  
Map Name: `'tunerContinuousEvolutionGeneticMixing'`

```text
This mirrors the batch mode genetic mixing parameter.  Refer to description above.

Default: 0.7

Restricted to range {0, 1}
```

#### Tuner Continuous Evolution Rolling Improvement Count [EXPERIMENTAL]

Setter: `.setTunerContinuousEvolutionRollingImprovementCount(<Int>)`  
Map Name: `'tunerContinuousEvolutionRollingImprovementCount'`

```text
[EXPERIMENTAL]
This is an early stopping criteria that measures the cumulative gain of the score as the job is running.  
If improvements ***stop happening***, then the continuous iteration of tuning will stop to prevent useless continuation.

Default: 20
```

#### Tuner Model Seed

Setters: `.setTunerModelSeed(<Map[String, Any]>)`  
Map Name: `'tunerModelSeed'`

```text
Allows for 'jump-starting' a model tuning run, primarily for the purpose of
fine-tuning after a previous run, or for retraining a previously trained model.

Provides a forced hyper parameter configuration for resumption of tuning, or to re-evaluate a model that has already
been tuned prior.  Useful in production use cases where a solid baseline exists, but updated training data may
make a better model.
```
> ***CAUTION*** : using a model Seed from an initial starting point on a data set (during exploratory phase) **may**
result in poor hyper parameter tuning performance.  *It is always best to not seed a new model when exploring new use cases*.

#### Tuner Hyper Space Inference Flag

Setters: `.setTunerHyperSpaceInferenceFlag(<Boolean>)`  
Map Name: `'tunerHyperSpaceInferenceFlag'`

```text
Whether or not to run a hyper space inference run at the conclusion of the genetic algorithm.

Default: ON
```
> It is HIGHLY ADVISED to leave this setting on.

#### Tuner Hyper Space Inference Count

Setters: `.setTunerHyperSpaceInferenceCount(<Int>)`  
Map Name: `'tunerHyperSpaceInferenceCount'`

```text
Count of synthetic rows of permutations to generate for the MBO-based post genetic tuning runs.
Default: 200000
```
> NOTE: Maximum limit is 1,000,000 (1 Million). Values above that do not provide noticeable results more than 500k
>and simply put more stress on the driver.

#### Tuner Hyper Space Model Count
Setters: `.setTunerHyperSpaceModelCount(<Int>)`  
Map Name: `'tunerHyperSpaceModelCount'`

```text
Number of models to generate from the predicted 'best hyper parameters' from the MBO stage.
Default: 10
```
> NOTE: a Warning will be issued if this setting is applied >50. The likelihood that a better setting is arrived at
>over this threshold is slim to none.  Most users set this in the range of 5 - 20

#### Tuner Hyper Space Model Type

Setters: `setTunerHyperSpaceModelType(<String>)`  
Map Name: `'tunerHyperSpaceModelType'`

```text
The type of Regressor to use in the MBO phase (RandomForest or XGBoost or LinearRegression)
Default: RandomForest
```

#### Tuner Initial Generation Mode

Setters: `.setTunerInitialGenerationMode(<String>)`  
Map Name: `'tunerInitialGenerationMode'`

```text
Whether to use a random search (default) or a permutations-based search space.
Options: 'random' or 'permutations'

Default: 'random'
```
> the 'permutations' mode is recommended to ensure that full search is conducted throughout the algorithm.  However,
>it generates guaranteed bad results, and as such, is not turned on by default until users become familiar with
>the performance of the toolkit.

#### Tuner Initial Generation Permutation Count

Setters: `.setTunerInitialGenerationPermutationCount(<Int>)`  
Map Name: `'tunerInitialGenerationPermutationCount'`

```text
Sets the number of hyper parameter combinations to generate and utilize when in 'permutation' mode for the initial
search space prior to the genetic algorithm being activated on the 2nd generation and onwards.
Default: 10
```

#### Tuner Initial Generation Index Mixing Mode

Setters: `.setTunerInitialGenerationIndexMixingMode(<String>)` 
Map Name: `'tunerInitialGenerationIndexMixingMode'`

```text
Sets the method in which the hyper parameter permutations are configured.
Random will shuffle the generated min to max series of individual hyperparameters, exploring the n-dimensional space
in a linear manner.
Random will mix them in random combinations to achieve a more 'random search' across the n-dimensional hyper plane.
Options: 'random' or 'linear'
Default: 'linear'
```

#### Tuner Initial Generation Array Seed

Setters: `.setTunerInitialGenerationArraySeed(<Long>)`  
Map Name: `'tunerInitialGenerationArraySeed'`

```text
Sets the seed for the generation of permutations to make the samplers for the random mode first-selection repeatable.
Default: 42L
```

#### Tuner Output Df Repartition Scale Factor

Setters: `.setTunerOutputDfRepartitionScaleFactor(<Int>)`  
Map Name: `'tunerOutputDfRepartitionScaleFactor'`

```text
Sets the degree of repartitioning factor that is done on the output Dataframes coming from the toolkit.
Default: 3
```

### Algorithm Config
```scala
case class AlgorithmConfig(var stringBoundaries: Map[String, List[String]],
                           var numericBoundaries: Map[String, (Double, Double)])
```


Setters (for all numeric boundaries): `.setNumericBoundaries(<Map[String, Tuple2[Double, Double]]>)`

Setters (for all string boundaries): `.setStringBoundaries(<Map[String, List[String]]>)`

> To override any of the features space exploration constraints, pick the correct Map configuration for the family
that is being used, define the Map values, and override with the common setters.

#### XGBoost

###### Default Numeric Boundaries
```scala
  def _xgboostDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "alpha" -> Tuple2(0.0, 1.0),
    "eta" -> Tuple2(0.1, 0.5),
    "gamma" -> Tuple2(0.0, 10.0),
    "lambda" -> Tuple2(0.1, 10.0),
    "maxDepth" -> Tuple2(3.0, 10.0),
    "subSample" -> Tuple2(0.4, 0.6),
    "minChildWeight" -> Tuple2(0.1, 10.0),
    "numRound" -> Tuple2(25.0, 250.0),
    "maxBins" -> Tuple2(25.0, 512.0),
    "trainTestRatio" -> Tuple2(0.2, 0.8)
  )
```

#### Random Forest

###### Default Numeric Boundaries
```scala
def _rfDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "numTrees" -> Tuple2(50.0, 1000.0),
    "maxBins" -> Tuple2(10.0, 100.0),
    "maxDepth" -> Tuple2(2.0, 20.0),
    "minInfoGain" -> Tuple2(0.0, 1.0),
    "subSamplingRate" -> Tuple2(0.5, 1.0)
  )
```
###### Default String Boundaries
```scala
 def _rfDefaultStringBoundaries = Map(
    "impurity" -> List("gini", "entropy"),
    "featureSubsetStrategy" -> List("auto")
  )
```
#### Gradient Boosted Trees

###### Default Numeric Boundaries
```scala
  def _gbtDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "maxBins" -> Tuple2(10.0, 100.0),
    "maxIter" -> Tuple2(10.0, 100.0),
    "maxDepth" -> Tuple2(2.0, 20.0),
    "minInfoGain" -> Tuple2(0.0, 1.0),
    "minInstancesPerNode" -> Tuple2(1.0, 50.0),
    "stepSize" -> Tuple2(1E-4, 1.0)
  )
```
###### Default String Boundaries
```scala
  def _gbtDefaultStringBoundaries: Map[String, List[String]] = Map(
    "impurity" -> List("gini", "entropy"),
    "lossType" -> List("logistic")
  )
```
#### Decision Trees

###### Default Numeric Boundaries
```scala
  def _treesDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "maxBins" -> Tuple2(10.0, 100.0),
    "maxDepth" -> Tuple2(2.0, 20.0),
    "minInfoGain" -> Tuple2(0.0, 1.0),
    "minInstancesPerNode" -> Tuple2(1.0, 50.0)
  )
```
###### Default String Boundaries
```scala
  def _treesDefaultStringBoundaries: Map[String, List[String]] = Map(
    "impurity" -> List("gini", "entropy")
  )
```
#### Linear Regression

###### Default Numeric Boundaries
```scala
  def _linearRegressionDefaultNumBoundaries: Map[String, (Double, Double)] = Map (
    "elasticNetParams" -> Tuple2(0.0, 1.0),
    "maxIter" -> Tuple2(100.0, 10000.0),
    "regParam" -> Tuple2(0.0, 1.0),
    "tolerance" -> Tuple2(1E-9, 1E-5)
  )
```
###### Default String Boundaries
```scala
  def _linearRegressionDefaultStringBoundaries: Map[String, List[String]] = Map (
    "loss" -> List("squaredError", "huber")
  )
```
#### Logistic Regression

###### Default Numeric Boundaries
```scala
  def _logisticRegressionDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "elasticNetParams" -> Tuple2(0.0, 1.0),
    "maxIter" -> Tuple2(100.0, 10000.0),
    "regParam" -> Tuple2(0.0, 1.0),
    "tolerance" -> Tuple2(1E-9, 1E-5)
  )
```
###### Default String Boundaries
```scala
 def _logisticRegressionDefaultStringBoundaries: Map[String, List[String]] = Map(
    "" -> List("")
  )
```

> NOTE: ***DO NOT OVERRIDE THIS***

#### Multilayer Perceptron Classifier

###### Default Numeric Boundaries
```scala
  def _mlpcDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "layers" -> Tuple2(1.0, 10.0),
    "maxIter" -> Tuple2(10.0, 100.0),
    "stepSize" -> Tuple2(0.01, 1.0),
    "tolerance" -> Tuple2(1E-9, 1E-5),
    "hiddenLayerSizeAdjust" -> Tuple2(0.0, 50.0)
  )
```
###### Default String Boundaries
```scala
  def _mlpcDefaultStringBoundaries: Map[String, List[String]] = Map(
    "solver" -> List("gd", "l-bfgs")
  )
```
#### Linear Support Vector Machines

###### Default Numeric Boundaries
```scala
  def _svmDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "maxIter" -> Tuple2(100.0, 10000.0),
    "regParam" -> Tuple2(0.0, 1.0),
    "tolerance" -> Tuple2(1E-9, 1E-5)
  )
```
###### Default String Boundaries
```scala
  def _svmDefaultStringBoundaries: Map[String, List[String]] = Map(
    "" -> List("")
  )
```
> NOTE: ***DO NOT OVERRIDE THIS***

#### LightGBM Families [Coming Soon when Concurrency works with LightGBM]

###### Default Numeric Boundaries
```scala
  def _lightGBMDefaultNumBoundaries: Map[String, (Double, Double)] = Map(
    "baggingFraction" -> Tuple2(0.5, 1.0),
    "baggingFreq" -> Tuple2(0.0, 1.0),
    "featureFraction" -> Tuple2(0.6, 1.0),
    "learningRate" -> Tuple2(1E-8, 1.0),
    "maxBin" -> Tuple2(50, 1000),
    "maxDepth" -> Tuple2(3.0, 20.0),
    "minSumHessianInLeaf" -> Tuple2(1e-5, 50.0),
    "numIterations" -> Tuple2(25.0, 250.0),
    "numLeaves" -> Tuple2(10.0, 50.0),
    "lambdaL1" -> Tuple2(0.0, 1.0),
    "lambdaL2" -> Tuple2(0.0, 1.0),
    "alpha" -> Tuple2(0.0, 1.0)
  )
```

###### Default String Boundaries
```scala
  def _lightGBMDefaultStringBoundaries: Map[String, List[String]] = Map(
    "boostingType" -> List("gbdt", "rf", "dart", "goss")
  )
```


### Logging Config
```scala
case class LoggingConfig(var mlFlowLoggingFlag: Boolean,
                         var mlFlowLogArtifactsFlag: Boolean,
                         var mlFlowTrackingURI: String,
                         var mlFlowExperimentName: String,
                         var mlFlowAPIToken: String,
                         var mlFlowModelSaveDirectory: String,
                         var mlFlowLoggingMode: String,
                         var mlFlowBestSuffix: String,
                         var inferenceConfigSaveLocation: String,
                         var mlFlowCustomRunTags: Map[String, String]
)
```

### MLFlow Settings

MLFlow integration in this toolkit allows for logging and tracking of not only the best model returned by a particular run,
but also a tracked history of all hyper parameters, scoring results for validation, and a location path to the actual
model artifacts that are generated for each iteration.

More information: [MLFlow](https://mlflow.org/docs/latest/index.html), [API Docs](https://mlflow.org/docs/latest/java_api/index.html)

The implementation here leverages the JavaAPI and can support both remote and Databricks-hosted MLFlow deployments.



#### MLFlow Logging Flag

Setters: `.mlFlowLoggingOn()` and `.mlFlowLoggingOff()`  
Map Name: `'mlFlowLoggingFlag'`

```text
Provides for either logging the results of the hyper parameter tuning run to MLFlow or not.

Default: on
```
> NOTE: it is HIGHLY ADVISED to leave this on.  Most algorithms produce far too much text in stdout to follow the results.

#### MLFlow Log Artifacts Flag [DEPRECATED API]

```text
Deprecated for non-pipeline runs.  
Not recommended to use the older API's.

This will be removed in future releasees.
```

#### MLFlow Tracking URI [FOR EXTERNAL MLFLOW TRACKING SERVERS ONLY]

Setter: `.setMlFlowTrackingURI(<String>)`  
Map Name: `'mlFlowTrackingURI'`

```text
If using a non-Databricks hosted MLFlow instance, this is the address to your tracking server.

If running on Databricks, this information is automatically pulled for you and used to do user-based authentication.
```

#### MLFlow Experiment Name [CRITICAL TO OVERRIDE FOR MOST USE CASES]

Setter `.setMlFlowExperimentName(<String>)`  
Map Name: `'mlFlowExperimentName'`

```text
The Workspace-resolved path within your shard that you would like the model's results to be logged to.

If this value is not specified, it will log to the same Workspace directory path that the current Notebook is running within.
```
> Highly recommended to override this value!!

#### MLFlow API Token [FOR EXTENERAL MLFLOW TRACKING SERVERS ONLY]

Setter: `.setMlFlowAPIToken(<String>)`  
Map Name: `'mlFlowAPIToken'`

```text
If using a non-Databricks hosted MLFlow instance, this is the API Token to your tracking server.

If running on Databricks, this information is automatically pulled for you and used to do user-based authentication.
```


#### MLFlow Model Save Directory [CRITICAL TO OVERRIDE FOR MOST USE CASES]

Setter: `.setMlFlowModelSaveDirectory(<String>)`  
Map Name: `'mlFlowModelSaveDirectory'`

```text
The path root to store all of the models that are generated with each hyper parameter optimization iteration.
These will be preceded and labeled with the UUID that is generated for the run (also logged in mlflow model location parameter)

this will infer a dbfs location for writing the model artifacts to.  
```
> NOTE: it is HIGHLY ADVISED to override the default!!

#### MLFlow Logging Mode

Setter: `.setMlFlowLoggingMode(<String>)`  
Map Name: `'mlFlowLoggingMode'`

```text
Sets whether to log all results (default), just the best run's results, or just the tuning results.

Options: 'full', 'bestOnly', 'tuningOnly'

Default: 'full'
```


#### MLFlow Best Suffix

Setter: `.setMlFlowBestSuffix(<String>)`  
Map Name: `'mlFlowBestSuffix'`

```text
A seperate MLFlow log entry for the best results for each of the Families tested.

Default: '_best'
```

#### Inference Config Save Location [DEPRECATED API]

Setter: `.setInferenceConfigSaveLocation(<String>)`  
Map Name: `'inferenceConfigSaveLocation'`

```text
Can be set to a location on dbfs for now, but this API is deprecated in favor of the PipelineAPI and will be
removed in a future version.
```

#### MLFlow Custom Run Tags

Setter: `.setMlFlowCustomRunTags(<Map[String, AnyVal]>`  
Map Name: `'mlFlowCustomRunTags'`

```text
Additional data that can be logged about the run in mlflow.  
```

#### Tuner Delta Cache Backing Directory

Setter: `.setTunerDeltaCacheBackingDirectory(<String>)`  
Map Name: `'tunerDeltaCacheBackingDirectory'`

```text
If using 'delta' mode on the split caching strategy, this is the dbfs root path to use to temporarily (or persistently)
write the delta train / test split tables to.
```
> NOTE: ensure you have permissions to write to this dbfs location prior to attempting to conduct an automl toolkit run.

#### Tuner Delta Cache Backing Directory Removal Flag

Setter: `.setTunerDeltaCacheBackingDirectoryRemovalFlag(<Boolean>)`  
Map Name: `'tunerDeltaCacheBackingDirectoryRemovalFlag'`

```text
Specifies whether to 'clean up' the delta dbfs location for 'delta mode' split caching strategy

Some users may want to have a persistent copy of their train/test splits for particular runs to do evaluation later on
or to keep for auditing / replayability reasons.  If this is the case, set this flag to FALSE.

Default: True, will remove files
```

#### Split Caching Strategy

Setter: `.setSplitCachingStrategy(<String>)`  
Map Name: `'splitCachingStrategy'`

```text
Mode to use to store the train test splits before model tuning begins.

Options: 'cache' or 'persist' or 'delta'

cache - memory caches the splits
persist - local fs disk persists the splits
delta - writes the splits to delta, then returns a reference reader from that location

Default: cache
```
> NOTE: take  note of the size of the modeling data set and set the appropriate mode based on the memory pressure of the cluster
>and the algorithms chosen to run.

### Instance Config
This is the main wrapper for all of the above grouped configuration modules.
```scala
case class InstanceConfig(
  var modelFamily: String,
  var predictionType: String,
  var genericConfig: GenericConfig,
  var switchConfig: SwitchConfig,
  var featureEngineeringConfig: FeatureEngineeringConfig,
  var algorithmConfig: AlgorithmConfig,
  var tunerConfig: TunerConfig,
  var loggingConfig: LoggingConfig
)
```

# Inference

### Pipeline API
To use pipeline API, check [here](PIPELINE_API_DOCS.md) for an example usage.

## Batch Inference Mode [DEPRECATED API, Pipeline API is now the official main access paradigm]

Batch Inference Mode is a feature that allows for a particular run's settings to be preserved, recalled, and used to
apply the precise feature engineering actions that occured during a model training run.  This is useful in order to
utilize the built model for batch inference, or to understand how to write real-time transformation logic in order to
create a feature vector that can be sent through a model in a model-as-a-service mode.

***This mode REQUIRES MLFlow in order to function***

Each individual model will get two forms of the configuration:
1. A compact JSON data structure that is logged as a tagged element to the mlflow run that can be copied and
pasted (or retrieved through the mlflow API) for any particular run that would be used to do batch inference
2. A specified location that a Dataframe has been saved that contains the same location.

There are two primary entry points for an inference run.  One simply requires the path of the DataFrame (preferred),
while the other requires the json string itself.  Everything needed to execute the inference prediction is
contained within this data structure (either the json or the Dataframe)


## Feature Importance

To utilize the Feature Importance functionality and the associated API, the settings are similar to what is listed
above in the main APIs.

The total config is:

```scala
case class FeatureImportanceConfig(
  labelCol: String,
  featuresCol: String,
  dataPrepParallelism: Int,
  numericBoundaries: Map[String, (Double, Double)],
  stringBoundaries: Map[String, List[String]],
  scoringMetric: String,
  trainPortion: Double,
  trainSplitMethod: String,
  trainSplitChronologicalColumn: String,
  trainSplitChronlogicalRandomPercentage: Double,
  parallelism: Int,
  kFold: Int,
  seed: Long,
  scoringOptimizationStrategy: String,
  firstGenerationGenePool: Int,
  numberOfGenerations: Int,
  numberOfMutationsPerGeneration: Int,
  numberOfParentsToRetain: Int,
  geneticMixing: Double,
  generationalMutationStrategy: String,
  mutationMagnitudeMode: String,
  fixedMutationValue: Int,
  autoStoppingScore: Double,
  autoStoppingFlag: Boolean,
  evolutionStrategy: String,
  continuousEvolutionMaxIterations: Int,
  continuousEvolutionStoppingScore: Double,
  continuousEvolutionParallelism: Int,
  continuousEvolutionMutationAggressiveness: Int,
  continuousEvolutionGeneticMixing: Double,
  continuousEvolutionRollingImprovementCount: Int,
  dataReductionFactor: Double,
  firstGenMode: String,
  firstGenPermutations: Int,
  firstGenIndexMixingMode: String,
  firstGenArraySeed: Long,
  fieldsToIgnore: Array[String],
  numericFillStat: String,
  characterFillStat: String,
  modelSelectionDistinctThreshold: Int,
  dateTimeConversionType: String,
  modelType: String,
  featureImportanceModelFamily: String,
  featureInteractionFlag: Boolean,
  featureInteractionRetentionMode: String,
  featureInteractionContinuousDiscretizerBucketCount: Int,
  featureInteractionParallelism: Int,
  featureInteractionTargetInteractionPercentage: Double,
  deltaCacheBackingDirectory: String,
  deltaCacheBackingDirectoryRemovalFlag: Boolean,
  splitCachingStrategy: String
)
```

These settings are applied through using the Configuration Generator, with identical map overrides as specified in 
the previous sections.

The main API for feature importances is used as follows:

```scala
import com.databricks.labs.automl.executor.config.ConfigurationGenerator
import com.databricks.labs.automl.exploration.FeatureImportances

val configurationOverrides = Map(
  "labelCol" -> "my_label",
  "tunerParallelism" -> 6,
  "tunerKFold" -> 3,
  "tunerTrainSplitMethod" -> "stratified",
  "scoringMetric" -> "areaUnderROC",
  "tunerNumberOfGenerations" -> 3,
  "tunerNumberOfMutationsPerGeneration" -> 8,
  "tunerInitialGenerationMode" -> "permutations",
  "tunerInitialGenerationPermutationCount" -> 18,
  "tunerFirstGenerationGenePool" -> 18
)

val config = ConfigurationGenerator.generateConfigFromMap("RandomForest", "classifier", configurationOverrides)
val featConfig = ConfigurationGenerator.generateFeatureImportanceConfig(config)

val featureImportances = FeatureImportances(df, featConfig, "count", 20).generateFeatureImportances()
```


