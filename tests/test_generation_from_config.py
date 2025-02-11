from contextlib import nullcontext as does_not_raise
import json
import pytest
import yaml
import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestGenerationFromConfig:
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
    ])
    def test_column_definitions_from_dict(self, columns, expectation):
        with expectation:
            # Test the options set on the ColumnGenerationSpecs:
            gen_from_dicts = dg.DataGenerator(rows=100, partitions=1).withColumnDefinitions(columns)
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                for key in column.keys():
                    assert column_spec.toDict()[key] == column[key]

            # Test the data generated after building the DataFrame:
            df_from_dicts = gen_from_dicts.build()
            assert df_from_dicts.columns == ["col1", "col2", "col3"]

    @pytest.mark.parametrize("expectation, constraints", [
        (does_not_raise(), [
            {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
            {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": True}
        ]),
        (does_not_raise(), [
            {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": False},
            {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"type": "SqlExpr", "expr": "col1 > 0"},
            {"type": "LiteralRelation", "columns": ["col2"], "relation": "<>", "value": "0"}
        ]),
        (pytest.raises(ValueError), [  # Testing an invalid "relation" value
            {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
            {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"type": "SqlExpr", "expr": "col1 > 0"},
            {"type": "LiteralRelation", "columns": ["col2"], "relation": "+", "value": "0"}
        ]),
        (pytest.raises(ValueError), [  # Testing an invalid "type" value
            {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": False},
            {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"type": "SqlExpr", "expr": "col1 > 0"},
            {"type": "Equivalent", "columns": ["col2"], "value": "0"}
        ]),
        (does_not_raise(), [
            {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
            {"type": "NegativeValues", "columns": ["col1", "col2"], "strict": False},
            {"type": "ChainedRelation", "columns": ["col1", "col2"], "relation": ">"},
            {"type": "RangedValues", "columns": ["col2"], "lowValue": 0, "highValue": 100, "strict": True},
            {"type": "UniqueCombinations", "columns": ["col1", "col2"]}
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
         {"generator": {"name": "test_generator", "rows": 1000},
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}]
          }),
        (does_not_raise(),
         {"generator": {"name": "test_generator", "rows": 10000, "randomSeed": 42},
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}]
          }),
        (does_not_raise(),
         {"generator": {"name": "test_generator", "rows": 10000, "randomSeed": 42},
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}],
          "constraints": [
              {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
              {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
              {"type": "SqlExpr", "expr": "col1 > 0"},
              {"type": "LiteralRelation", "columns": ["col2"], "relation": "<>", "value": "0"}]
          }),
        (pytest.raises(KeyError),  # Testing a dictionary missing a "generator" object
         {"columns": [
             {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
             {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
             {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}]
          }),
        (pytest.raises(ValueError),  # Testing an invalid "type" value
         {"generator": {"name": "test_generator", "rows": 10000, "randomSeed": 42},
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}],
          "constraints": [
              {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
              {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
              {"type": "SqlExpr", "expr": "col1 > 0"},
              {"type": "Equivalent", "columns": ["col2"], "value": 0}]
          }),
    ])
    def test_generator_from_dict(self, options, expectation):
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.fromDict(options)
            generator = options.get("generator")
            for key in generator:
                assert gen_from_dicts.toDict()["generator"][key] == generator[key]

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
         '''{"generator": {"name": "test_generator", "rows": 1000},
          "columns": [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}]
          }'''),
        (does_not_raise(),
         '''{"generator": {"name": "test_generator", "rows": 10000, "randomSeed": 42},
          "columns": [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}]
          }'''),
        (does_not_raise(),
         '''{"generator": {"name": "test_generator", "rows": 10000, "randomSeed": 42},
              "columns": [
                {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
                {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
                {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}],
              "constraints": [
                {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": true},
                {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": true},
                {"type": "SqlExpr", "expr": "col1 > 0"},
                {"type": "LiteralRelation", "columns": ["col2"], "relation": "<>", "value": 0}]
              }'''),
        (pytest.raises(KeyError),  # Testing a JSON object missing the "generator" key
         '''{"columns": [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}]
          }'''),
        (pytest.raises(ValueError),  # Testing an invalid "type" value
         '''{"generator": {"name": "test_generator", "rows": 10000, "randomSeed": 42},
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}],
          "constraints": [
              {"type": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": true},
              {"type": "PositiveValues", "columns": ["col1", "col2"], "strict": true},
              {"type": "SqlExpr", "expr": "col1 > 0"},
              {"type": "Equivalent", "columns": ["col2"], "value": 0}]
          }'''),
    ])
    def test_generator_from_json(self, json_options, expectation):
        options = json.loads(json_options)
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.fromJson(json_options)
            generator = options.get("generator")
            for key in generator:
                assert gen_from_dicts.toDict()["generator"][key] == generator[key]

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
            generator:
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
            generator:
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
            generator:
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
            - type: LiteralRange
              columns:
              - col1
              lowValue: -1000
              highValue: 1000
              strict: true
            - type: PositiveValues
              columns:
              - col1
              - col2
              strict: true
            - type: SqlExpr
              expr: col1 > 0
            - type: LiteralRelation
              columns:
              - col2
              relation: "<>"
              value: 0'''),
        (pytest.raises(KeyError),  # Testing a YAML object missing the "generator" key
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
        (pytest.raises(ValueError),  # Testing an invalid "type" value
         '''---
            generator:
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
            - type: LiteralRange
              columns:
              - col1
              lowValue: -1000
              highValue: 1000
              strict: true
            - type: PositiveValues
              columns:
              - col1
              - col2
              strict: true
            - type: SqlExpr
              expr: col1 > 0
            - type: Equivalent
              columns:
              - col2
              value: 0''')
    ])
    def test_generator_from_yaml(self, yaml_options, expectation):
        options = yaml.safe_load(yaml_options)
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.fromYaml(yaml_options)
            generator = options.get("generator")
            for key in generator:
                assert gen_from_dicts.toDict()["generator"][key] == generator[key]

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
