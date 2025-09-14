from contextlib import nullcontext as does_not_raise
import json
import pytest
import dbldatagen as dg
from dbldatagen.utils import parse_time_interval
from dbldatagen.column_generation_spec import ColumnGenerationSpec

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
            {"colName": "col1", "colType": "string", "template": r"\w.\w@\w.com|\w@\w.co.u\k"},
            {"colName": "col2", "colType": "string", "text": {"kind": "TemplateGenerator", "template": "ddd-ddd-dddd"}},
            {"colName": "col3", "colType": "string", "text": {"kind": "TemplateGenerator",
                                                              "template": r"\w \w|\w \w \w|\w \a. \w",
                                                              "escapeSpecialChars": True,
                                                              "extendedWordList": ["red", "blue", "yellow"]}},
            {"colName": "col4", "colType": "string", "text": {"kind": "ILText", "paragraphs": 2,
                                                              "sentences": 4, "words": 10}}
        ]),
        (does_not_raise(), [
            {"colName": "col1", "colType": "date", "dataRange": {"kind": "DateRange", "begin": "2025-01-01 00:00:00",
                                                                 "end": "2025-12-31 00:00:00", "interval": "days=1"}},
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
        ]),
        (pytest.raises(NotImplementedError), [  # Testing serialization error with PyfuncText
            {"colName": "col1", "colType": "string", "text": {"kind": "PyfuncText", "fn": "lambda x: x.trim()"}}
        ]),
        (pytest.raises(ValueError), [  # Testing serialization error with a bad "kind"
            {"colName": "col1", "colType": "string", "text": {"kind": "InvalidTextFactory", "property": "value"}}
        ])
    ])
    def test_column_definitions_from_dict(self, columns, expectation):
        with expectation:
            # Test the options set on the ColumnGenerationSpecs:
            gen_from_dicts = dg.DataGenerator(rows=100, partitions=1)._loadColumnsFromInitializationDicts(columns)
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                column_dict = column_spec._toInitializationDict()
                for key, value in column.items():
                    if key == "dataRange" and value["kind"] == "DateRange":
                        left_interval = parse_time_interval(column_dict[key].pop("interval", None))
                        right_interval = parse_time_interval(value.pop("interval", None))
                        assert left_interval == right_interval
                        left_format = column_dict[key].pop("datetime_format")
                        if "datetime_format" in value:
                            right_format = value.pop("datetime_format")
                            assert left_format == right_format
                    if isinstance(column[key], dict):
                        for inner_key in value:
                            assert column_dict[key][inner_key] == value[inner_key]
                        continue
                    assert column_dict[key] == value

                    # Test the data generated after building the DataFrame:
                    df_from_dicts = gen_from_dicts.build()
                    assert df_from_dicts.columns == [column["colName"] for column in columns]
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
        (pytest.raises(TypeError), [  # Testing an invalid "kind" value
            {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": False},
            {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
            {"kind": "SqlExpr", "expr": "col1 > 0"},
            {"kind": "InvalidConstraintType", "columns": ["col2"], "value": "0"}
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
                ._loadColumnsFromInitializationDicts(columns) \
                ._loadConstraintsFromInitializationDicts(constraints)

            constraint_specs = [constraint._toInitializationDict() for constraint in gen_from_dicts.constraints]
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
        (pytest.raises(TypeError),  # Testing an invalid "kind" value
         {"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": True},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}],
          "constraints": [
              {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": True},
              {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": True},
              {"kind": "SqlExpr", "expr": "col1 > 0"},
              {"kind": "InvalidConstraintType", "columns": ["col2"], "value": 0}]
          }),
    ])
    def test_generator_from_dict(self, options, expectation):
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.loadFromInitializationDict(options)
            generator = {
                k: v for k, v in options.items()
                if not isinstance(v, list)
                and not isinstance(v, dict)
            }
            for key in generator:
                assert gen_from_dicts.saveToInitializationDict()[key] == generator[key]

            # Test the options set on the ColumnGenerationSpecs:
            columns = options.get("columns", [])
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                for key in column.keys():
                    assert column_spec._toInitializationDict()[key] == column[key]

            # Test the options set on the Constraints:
            constraints = options.get("constraints", [])
            constraint_specs = [
                constraint._toInitializationDict()
                for constraint in gen_from_dicts.constraints
            ]
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
        (pytest.raises(TypeError),  # Testing an invalid "kind" value
         '''{"name": "test_generator", "rows": 10000, "randomSeed": 42,
          "columns": [
              {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100, "step": 2, "random": true},
              {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0, "step": 1.5},
              {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": true}],
          "constraints": [
              {"kind": "LiteralRange", "columns": ["col1"], "lowValue": -1000, "highValue": 1000, "strict": true},
              {"kind": "PositiveValues", "columns": ["col1", "col2"], "strict": true},
              {"kind": "SqlExpr", "expr": "col1 > 0"},
              {"kind": "InvalidConstraintType", "columns": ["col2"], "value": 0}]
          }'''),
    ])
    def test_generator_from_json(self, json_options, expectation):
        options = json.loads(json_options)
        with expectation:
            # Test the options set on the DataGenerator:
            gen_from_dicts = dg.DataGenerator.loadFromJson(json_options)
            generator = {
                k: v for k, v in options.items()
                if not isinstance(v, list)
                and not isinstance(v, dict)
            }
            for key in generator:
                assert gen_from_dicts.saveToInitializationDict()[key] == generator[key]

            # Test the options set on the ColumnGenerationSpecs:
            columns = options.get("columns", [])
            for column in columns:
                column_spec = gen_from_dicts.getColumnSpec(column["colName"])
                for key in column.keys():
                    assert column_spec._toInitializationDict()[key] == column[key]

            # Test the options set on the Constraints:
            constraints = options.get("constraints", [])
            constraint_specs = [
                constraint._toInitializationDict()
                for constraint in gen_from_dicts.constraints
            ]
            for constraint in constraints:
                assert constraint in constraint_specs

            # Test the data generated after building the DataFrame:
            df_from_dicts = gen_from_dicts.build()
            assert df_from_dicts.columns == ["col1", "col2", "col3"]

    def test_generator_to_json(self):
        gen = dg.DataGenerator(rows=100, name="test")
        assert json.loads(gen.saveToJson())["rows"] == 100
        assert json.loads(gen.saveToJson())["name"] == "test"

        gen = gen.withColumn("val", "int", expr="id % 12")
        assert json.loads(gen.saveToJson())["rows"] == 100
        assert json.loads(gen.saveToJson())["name"] == "test"
        assert len(json.loads(gen.saveToJson())["columns"]) == 2

        column = json.loads(gen.saveToJson())["columns"][1]
        assert column["colName"] == "val"
        assert column["colType"] == "int"
        assert column["expr"] == "id % 12"

    def test_from_options(self):
        options = {
            "kind": "ColumnGenerationSpec",
            "name": "col1",
            "colType": "double",
            "dataRange": {"kind": "NRange", "minValue": 0.0, "maxValue": 100.0, "step": 0.1}
        }
        column = ColumnGenerationSpec._fromInitializationDict(options)
        column_dict = column._toInitializationDict()
        options.pop("kind")
        for k, v in options.items():
            if k == "name":
                assert column_dict["colName"] == v
                continue
            if k == "dataRange":
                for inner_key in v:
                    assert column_dict[k][inner_key] == v[inner_key]
                continue
            assert column_dict[k] == v
