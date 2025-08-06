from typing import Optional
from dbldatagen.extensions.datagen_spec import ColumnDefinition, DatagenSpec, TableDefinition
from .base import AbstractSpecGenerator
from .text_templates import COLUMN_TEMPLATES


class EmployeeSpecGenerator(AbstractSpecGenerator):
    """
    A static generator for employee dataset that creates a predefined schema
    with common employee-related fields.
    """

    def __init__(self, number_of_rows: int = 1000, partitions: Optional[int] = 4):
        """
        Initialize the EmployeeSpecGenerator.

        Args:
            number_of_rows: Number of rows to generate (default: 1000)
            partitions: Number of partitions for Spark (default: 4)
        """
        self.number_of_rows = number_of_rows
        self.partitions = partitions

    def _get_employee_table_definition(self) -> TableDefinition:
        """
        Creates a TableDefinition for the employee dataset with predefined columns.
        """
        columns = [
            ColumnDefinition(name="employee_id", type="int",
                             primary=True, nullable=False),
            ColumnDefinition(name="first_name", type="string", options={
                             "template": COLUMN_TEMPLATES["first_name"]}),
            ColumnDefinition(name="last_name", type="string", options={
                             "template": COLUMN_TEMPLATES["last_name"]}),
            ColumnDefinition(name="email", type="string", options={
                             "template": COLUMN_TEMPLATES["email"]}),
            ColumnDefinition(
                name="department",
                type="string",
                options={
                    "values": [
                        "Engineering",
                        "Sales",
                        "Marketing",
                        "HR",
                        "Finance",
                        "Operations",
                    ]
                },
            ),
            ColumnDefinition(
                name="job_title",
                type="string",
                options={
                    "values": [
                        "Software Engineer",
                        "Senior Engineer",
                        "Engineering Manager",
                        "Sales Representative",
                        "Sales Manager",
                        "Marketing Specialist",
                        "Marketing Manager",
                        "HR Specialist",
                        "HR Manager",
                        "Financial Analyst",
                        "Finance Manager",
                        "Operations Specialist",
                        "Operations Manager",
                    ]
                },
            ),
            ColumnDefinition(
                name="salary",
                type="decimal",
                options={"min": 50000, "max": 200000, "precision": 2},
            ),
            ColumnDefinition(
                name="hire_date",
                type="date",
                options={
                    "begin": "2020-01-01",
                    "end": "2024-12-31",
                    "format": "yyyy-MM-dd",
                    "random": True,
                },
            ),
            # The active field did fail locally with
            # pyspark.errors.exceptions.captured.AnalysisException: [DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve "CASE WHEN (_scaled_code59 <= 0.6666666666666666) THEN true WHEN (_scaled_code59 <= 1.0) THEN false ELSE {values[-1]} END" due to data type mismatch: Input to `casewhen` should all be the same type, but it's ["BOOLEAN", "BOOLEAN", "STRING"].; line 1 pos 1;
            # 'Project [id#0L, _scaled_code59#2, cast(cast(CASE WHEN (_scaled_code59#2 <= cast(0.6666666666666666 as double)) THEN true WHEN (_scaled_code59#2 <= cast(1.0 as double)) THEN false ELSE {values[-1]} END as boolean) as boolean) AS code59#5]
            # +- Project [id#0L, cast(cast((cast((FLOOR(((id#0L % cast(6 as bigint)) + cast(6 as bigint))) % cast(6 as bigint)) as double) / cast(5.0 as double)) as double) as double) AS _scaled_code59#2]
            #    +- Range (0, 10, step=1, splits=Some(4))
            # Likely bug in dbldatagen.function_builder.ColumnGeneratorBuilder.mkExprChoicesFn {values[-1]} should be a f string with values -1 evaluated.
            # works on dbr might be hash or the seed differs
            ColumnDefinition(
                name="is_active",
                type="boolean",
                options={
                    "values": [True, False],
                    "weights": [9, 1],  # 90% active, 10% inactive
                },
            ),
            ColumnDefinition(
                name="manager_id",
                type="int",
                options={
                    "baseColumn": "employee_id",
                    "omit": 0.1,  # 10% chance of being null (no manager)
                },
            ),
        ]

        return TableDefinition(
            number_of_rows=self.number_of_rows,
            partitions=self.partitions,
            columns=columns,
        )

    def generate_spec(self) -> DatagenSpec:
        """
        Generates a DatagenSpec for the employee dataset.

        Returns:
            DatagenSpec: A specification for generating employee data
        """
        employee_table = self._get_employee_table_definition()
        config_obj = DatagenSpec(tables={"employees": employee_table})
        return config_obj
