from contextlib import nullcontext as does_not_raise
import json
import pytest
import yaml
import dbldatagen as dg
from dbldatagen.utils import parse_time_interval

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestSerialization:
    @pytest.mark.parametrize("expectation, columns", [
        (does_not_raise(), [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}
        ]),
        (does_not_raise(), [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}
        ]),
        (does_not_raise(), [
            {"colName": "col1", "colType": "date", "dataRange": {"kind": "DateRange", "begin": "2025-01-01 00:00:00",
                                                                 "end": "2025-12-31 00:00:00", "interval": "1 day"}},
            {"colName": "col2", "colType": "double", "dataRange": {"kind": "NRange", "minValue": 0.0,
                                                                   "maxValue": 10.0, "step": 0.1}}
        ]),
        (does_not_raise(), [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "random": True,
             "distribution": {"kind": "Gamma", "shape": 1.0, "scale": 2.0}},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 1.0, "random": True,
             "distribution": {"kind": "Beta", "alpha": 2, "beta": 5}},
            {"colName": "col3", "colType": "float", "minValue": 0, "maxValue": 10000, "random": True,
             "distribution": {"kind": "Exponential", "rate": 1.5}},
            {"colName": "col4", "colType": "int", "minValue": 0, "maxValue": 100, "random": True,
             "distribution": {"kind": "Normal", "mean": 50.0, "stddev": 2.0}},
        ])
    ])
    def test_column_definitions_from_dict(self, columns, expectation):
        with expectation:
            # Test the options set on the ColumnGenerationSpecs:
            gen_from_dicts = dg.DataGenerator(rows=100, partitions=1).withColumnDefinitions(columns)
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                column_dict = column_spec.toDict()
                for key in column.keys():
                    if key == "dataRange" and column[key]["kind"] == "DateRange":
                        left_interval = parse_time_interval(column_dict[key].pop("interval"))
                        right_interval = parse_time_interval(column[key].pop("interval"))
                        assert left_interval == right_interval
                        left_format = column_dict[key].pop("datetime_format")
                        if "datetime_format" in column[key]:
                            right_format = column["key"]["datetime_format"]
                            assert left_format == right_format
                    assert column_dict[key] == column[key]

            # Test the data generated after building the DataFrame:
            df_from_dicts = gen_from_dicts.build()
            assert df_from_dicts.columns == [column["colName"] for column in columns]

    @pytest.mark.parametrize("expectation, constraints", [
        (does_not_raise(), [
            {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
            {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True}
        ]),
        (does_not_raise(), [
            {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": False},
            {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"kind": "SqlExpr", "expr": "col1 > 0"},
            {"kind": "LiteralRelation", "columns": ["col2"], "relation": "<>", "value": "0"}
        ]),
        (pytest.raises(ValueError), [  # Testing an invalid "relation" value
            {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
            {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"kind": "SqlExpr", "expr": "col1 > 0"},
            {"kind": "LiteralRelation", "columns": ["col2"], "relation": "+", "value": "0"}
        ]),
        (pytest.raises(IndexError), [  # Testing an invalid "kind" value
            {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": False},
            {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"kind": "SqlExpr", "expr": "col1 > 0"},
            {"kind": "Equivalent", "columns": ["col2"], "value": "0"}
        ]),
        (does_not_raise(), [
            {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
            {"kind": "NegativeValues", "columns": ["col1", "col2"], "strict": False},
            {"kind": "ChainedRelation", "columns": ["col1", "col2"], "relation": ">"},
            {"kind": "RangedValues", "columns": ["col2"], "lowValue": 0, "highValue": 100, "strict": True},
            {"kind": "UniqueCombinations", "columns": ["col1", "col2"]}
        ]),
    ])
    def test_constraint_definitions_from_dict(self, constraints, expectation):
        with expectation:
            # Test the options set on the ColumnGenerationSpecs:
            columns = [
                {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
                {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
                {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}
            ]
            gen_from_dicts = dg.DataGenerator(rows=100, partitions=1) \
                .withColumnDefinitions(columns) \
                .withConstraintDefinitions(constraints)

            constraint_specs = [constraint.toDict() for constraint in gen_from_dicts.getConstraints()]
            for constraint in constraints:
                assert constraint in constraint_specs

    @pytest.mark.parametrize("expectation, options", [
        (does_not_raise(),
         {"name": "test_generator", "rows": 1000,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}]
          }),
        (does_not_raise(),
         {"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}]
          }),
        (does_not_raise(),
         {"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}],
          "constraints": [
              {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
              {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
              {"kind": "SqlExpr", "expr": "col1 > 0"},
              {"kind": "LiteralRelation", "columns": ["col2"], "relation": "<>", "value": "0"}]
          }),
        (does_not_raise(),  # Testing a dictionary missing a "generator" object
         {"columns": [
             {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
             {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
             {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}]
          }),
        (pytest.raises(IndexError),  # Testing an invalid "kind" value
         {"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}],
          "constraints": [
              {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
              {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
              {"kind": "SqlExpr", "expr": "col1 > 0"},
              {"kind": "Equivalent", "columns": ["col2"], "value": 0}]
          }),
    ])
    def test_generator_from_dict(self, options, expectation):
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.fromDict(options)
            generator = {k: v for k, v in options.items() if not isinstance(v, list) and not isinstance(v, dict)}
            for key in generator:
                assert gen_from_dicts.toDict()[key] == generator[key]

            # Test the options set on the ColumnGenerationSpecs:
            columns = options.get("columns", [])
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                for key in column.keys():
                    assert column_spec.toDict()[key] == column[key]

            # Test the options set on the Constraints:
            constraints = options.get("constraints", [])
            constraint_specs = [constraint.toDict() for constraint in gen_from_dicts.getConstraints()]
            for constraint in constraints:
                assert constraint in constraint_specs

            # Test the data generated after building the DataFrame:
            df_from_dicts = gen_from_dicts.build()
            assert df_from_dicts.columns == ["col1", "col2", "col3"]

    @pytest.mark.parametrize("expectation, json_options", [
        (does_not_raise(),
         '''{"name": "test_generator", "rows": 1000,
          "columns": [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}]
          }'''),
        (does_not_raise(),
         '''{"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}]
          }'''),
        (does_not_raise(),
         '''{"name": "test_generator", "rows": 10000, "randomSeed": 42,
              "columns": [
                {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
                {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
                {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}],
              "constraints": [
                {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": true},
                {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": true},
                {"kind": "SqlExpr", "expr": "col1 > 0"},
                {"kind": "LiteralRelation", "columns": ["col2"], "relation": "<>", "value": 0}]
              }'''),
        (does_not_raise(),  # Testing a JSON object missing the "generator" key
         '''{"columns": [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}]
          }'''),
        (pytest.raises(IndexError),  # Testing an invalid "kind" value
         '''{"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}],
          "constraints": [
              {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": true},
              {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": true},
              {"kind": "SqlExpr", "expr": "col1 > 0"},
              {"kind": "Equivalent", "columns": ["col2"], "value": 0}]
          }'''),
    ])
    def test_generator_from_json(self, json_options, expectation):
        options = json.loads(json_options)
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.fromJson(json_options)
            generator = {k: v for k, v in options.items() if not isinstance(v, list) and not isinstance(v, dict)}
            for key in generator:
                assert gen_from_dicts.toDict()[key] == generator[key]

            # Test the options set on the ColumnGenerationSpecs:
            columns = options.get("columns", [])
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                for key in column.keys():
                    assert column_spec.toDict()[key] == column[key]

            # Test the options set on the Constraints:
            constraints = options.get("constraints", [])
            constraint_specs = [constraint.toDict() for constraint in gen_from_dicts.getConstraints()]
            for constraint in constraints:
                assert constraint in constraint_specs

            # Test the data generated after building the DataFrame:
            df_from_dicts = gen_from_dicts.build()
            assert df_from_dicts.columns == ["col1", "col2", "col3"]

    @pytest.mark.parametrize("expectation, yaml_options", [
        (does_not_raise(),
         '''---
            name: test_generator
            rows: 10000
            randomSeed: 42
            columns:
            - colName: col1
              colType: int
              minValue: 0
              maxValue: 100
              step: 2
              random: true
            - colName: col2
              colType: float
              minValue: 0
              maxValue: 100
              step: 1.5
            - colName: col3
              colType: string
              values:
              - a
              - b
              - c
              random: true'''),
        (does_not_raise(),
         '''---
            name: test_generator
            rows: 1000
            columns:
            - colName: col1
              colType: int
              minValue: 0
              maxValue: 100
            - colName: col2
              colType: float
              minValue: 0
              maxValue: 100
            - colName: col3
              colType: string
              values:
              - a
              - b
              - c
              random: true'''),
        (does_not_raise(),
         '''---
            name: test_generator
            rows: 10000
            randomSeed: 42
            columns:
            - colName: col1
              colType: int
              minValue: 0
              maxValue: 100
              step: 2
              random: true
            - colName: col2
              colType: float
              minValue: 0
              maxValue: 100
              step: 1.5
            - colName: col3
              colType: string
              values:
              - a
              - b
              - c
              random: true
            constraints:
            - kind: LiteralRange
              columns:
              - col1
              lowValue: -1000
              highValue: 1000
              strict: true
            - kind: PositiveValues
              columns:
              - col1
              - col2
              strict: true
            - kind: SqlExpr
              expr: col1 > 0
            - kind: LiteralRelation
              columns:
              - col2
              relation: "<>"
              value: 0'''),
        (does_not_raise(),  # Testing a YAML object missing the "generator" key
         '''---
            columns:
            - colName: col1
              colType: int
              minValue: 0
              maxValue: 100
            - colName: col2
              colType: float
              minValue: 0
              maxValue: 100
            - colName: col3
              colType: string
              values:
              - a
              - b
              - c
              random: true'''),
        (pytest.raises(IndexError),  # Testing an invalid "kind" value
         '''---
            name: test_generator
            rows: 10000
            randomSeed: 42
            columns:
            - colName: col1
              colType: int
              minValue: 0
              maxValue: 100
              step: 2
              random: true
            - colName: col2
              colType: float
              minValue: 0
              maxValue: 100
              step: 1.5
            - colName: col3
              colType: string
              values:
              - a
              - b
              - c
              random: true
            constraints:
            - kind: LiteralRange
              columns:
              - col1
              lowValue: -1000
              highValue: 1000
              strict: true
            - kind: PositiveValues
              columns:
              - col1
              - col2
              strict: true
            - kind: SqlExpr
              expr: col1 > 0
            - kind: Equivalent
              columns:
              - col2
              value: 0''')
    ])
    def test_generator_from_yaml(self, yaml_options, expectation):
        options = yaml.safe_load(yaml_options)
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.fromYaml(yaml_options)
            generator = {k: v for k, v in options.items() if not isinstance(v, list) and not isinstance(v, dict)}
            for key in generator:
                assert gen_from_dicts.toDict()[key] == generator[key]

            # Test the options set on the ColumnGenerationSpecs:
            columns = options.get("columns", [])
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                for key in column.keys():
                    assert column_spec.toDict()[key] == column[key]

            # Test the options set on the Constraints:
            constraints = options.get("constraints", [])
            constraint_specs = [constraint.toDict() for constraint in gen_from_dicts.getConstraints()]
            for constraint in constraints:
                assert constraint in constraint_specs

            # Test the data generated after building the DataFrame:
            df_from_dicts = gen_from_dicts.build()
            assert df_from_dicts.columns == ["col1", "col2", "col3"]
