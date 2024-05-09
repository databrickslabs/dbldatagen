# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `SchemaParser` class
"""

import re
import pyparsing as pp
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, StructField, StructType, MapType, ArrayType


class SchemaParser(object):
    """ SchemaParser class

        Creates pyspark SQL datatype from string
    """
    _type_parser = None

    @classmethod
    def getTypeDefinitionParser(cls):
        """
        Define a pyparsing based parser for Spark SQL type definitions

        Allowable constructs for generated type parser are:
         * `string`, `varchar`, `char`, `nvarchar`,
         * `int`, `integer`,
         * `bigint`, `long`,
         * `bool`, `boolean`,
         * `smallint`, `short`
         * `binary`
         * `tinyint`, `byte`
         * `date`
         * `timestamp`, `datetime`,
         * `double`, `float`, `date`, `short`, `byte`,
         * `decimal` or `decimal(p)` or `decimal(p, s)` or `number(p, s)`
         * `map<type1, type2>` where type1 and type2 are type definitions of form accepted by parser
         * `array<type1>` where type1 is type definitions of form accepted by parser
         * `struct<a:binary, b:int, c:float>`

         Type definitions may be nested recursively - for example, the following are valid type definitions:
         * `array<array<int>>
         * `struct<a:array<int>, b:int, c:float>`
         * `map<string, struct<a:array<int>, b:int, c:float>>`

        :return: parser

        See the package pyparsing for details of how the parser mechanism works
        `https://pypi.org/project/pyparsing/`
        """
        # define grammar for parsing type definitions of the form "INT", "decimal(10,4)"
        # and "map<string, string>" etc
        if cls._type_parser is None:
            int_keyword = pp.CaselessKeyword("int") ^ pp.CaselessKeyword("integer")
            int_keyword.setParseAction(lambda s: "int")

            bigint_keyword = pp.CaselessKeyword("bigint") ^ pp.CaselessKeyword("long")
            bigint_keyword.setParseAction(lambda s: "bigint")

            binary_keyword = pp.CaselessKeyword("binary")

            boolean_keyword = pp.CaselessKeyword("boolean") ^ pp.CaselessKeyword("bool")
            boolean_keyword.setParseAction(lambda s: "boolean")

            date_keyword = pp.CaselessKeyword("date")
            double_keyword = pp.CaselessKeyword("double")

            float_keyword = pp.CaselessKeyword("float") ^ pp.CaselessKeyword("real")
            float_keyword.setParseAction(lambda s: "float")

            smallint_keyword = pp.CaselessKeyword("smallint") ^ pp.CaselessKeyword("short")
            smallint_keyword.setParseAction(lambda s: "smallint")

            timestamp_keyword = pp.CaselessKeyword("timestamp") ^ pp.CaselessKeyword("datetime")
            timestamp_keyword.setParseAction(lambda s: "timestamp")

            tinyint_keyword = pp.CaselessKeyword("tinyint") ^ pp.CaselessKeyword("byte")
            tinyint_keyword.setParseAction(lambda s: "tinyint")

            lbracket = (pp.Literal("(").suppress()).setName("lPar")
            rbracket = pp.Literal(")").suppress().setName("rPar")
            comma = pp.Literal(",").suppress().setName("comma")
            number = pp.Word(pp.nums).setName("number_literal")

            # handle string types of the form "STRING", "char(10)" etc
            string_keyword = pp.oneOf(["string", "nvarchar", "varchar", "char"], caseless=True, asKeyword=True)
            string_keyword.setParseAction(lambda s: "string")
            string_type_expr = string_keyword + pp.Optional(lbracket + number + rbracket)

            # handle decimal types of the form "decimal(10)", "real", "numeric(10,3)"
            decimal_keyword = pp.MatchFirst(
                [pp.CaselessKeyword("decimal"), pp.CaselessKeyword("dec"), pp.CaselessKeyword("number"),
                 pp.CaselessKeyword("numeric")])
            decimal_keyword.setParseAction(lambda s: "decimal")
            # first number is precision , default 10; second number is scale, default 0
            decimal_type_expr = decimal_keyword + pp.Optional(
                lbracket + number + pp.Optional(comma + number, "0") + rbracket)

            primitive_type_keyword = (int_keyword ^ bigint_keyword ^ binary_keyword ^ boolean_keyword
                                      ^ date_keyword ^ float_keyword ^ smallint_keyword ^ timestamp_keyword
                                      ^ tinyint_keyword ^ string_type_expr ^ decimal_type_expr ^ double_keyword
                                      ).setName("primitive_type_defn")

            # handle more complex type definitions such as struct, map and array

            # declare forward reference to type expression
            type_expr = pp.Forward()

            l_angle = pp.Literal("<").suppress()
            r_angle = pp.Literal(">").suppress()

            # handle arrays
            array_keyword = pp.CaselessKeyword("array")
            array_expr = array_keyword + pp.Group(l_angle + type_expr + r_angle)
            array_expr.setName("array_type_expr")

            # handle maps
            map_keyword = pp.CaselessKeyword("map")
            map_expr = map_keyword + l_angle + pp.Group(type_expr) + comma + pp.Group(type_expr) + r_angle
            map_expr.setName("map_type_expr")

            # handle SQL identifiers such as "a14" and "`a test`"
            ident = pp.Word(pp.alphas, pp.alphanums + "_") | pp.QuotedString(quoteChar="`", escQuote="``")

            colon = pp.Literal(":").suppress()

            # handle structs
            struct_keyword = pp.CaselessKeyword("struct")
            struct_expr = struct_keyword + l_angle + pp.Group(
                pp.delimitedList(pp.Group(ident + pp.Optional(colon) + pp.Group(type_expr)))) + r_angle

            # try to capture invalid type name for better error reporting
            invalid_type = pp.Word(pp.alphas, pp.alphanums + "_", asKeyword=True)

            # use left recursion to handle nesting of types
            type_expr <<= pp.MatchFirst([primitive_type_keyword, array_expr, map_expr, struct_expr, invalid_type])

            type_parser = pp.StringStart() + type_expr + pp.StringEnd()
            cls._type_parser = type_parser
        return cls._type_parser

    @classmethod
    def _parse_ast(cls, ast):
        """
        Parse Abstract Syntax Tree generated from earlier parse
        :param ast: AST generated by parsing type definition
        :return: Pyspark Type Definition - e.g StringType() etc
        """
        assert len(ast) > 0, "AST tree must contain elements"

        first_token = ast[0]

        if first_token == "string":
            retval = StringType()
        elif first_token == "int":
            retval = IntegerType()
        elif first_token == "bigint":
            retval = LongType()
        elif first_token == "boolean":
            retval = BooleanType()
        elif first_token == "binary":
            retval = BinaryType()
        elif first_token == "timestamp":
            retval = TimestampType()
        elif first_token == "decimal":
            if len(ast) == 1:
                retval = DecimalType(10, 0)
            # note if `decimal(10)` is passed, parser will expand to `decimal(10,0)`
            # so there is no need to handle case here len(ast) == 2
            elif len(ast) == 3:
                retval = DecimalType(int(ast[1]), int(ast[2]))
            else:
                raise ValueError(f"Cannot parse decimal type {list(ast)}")
        elif first_token == "double":
            retval = DoubleType()
        elif first_token == "float":
            retval = FloatType()
        elif first_token == "date":
            retval = DateType()
        elif first_token == "smallint":
            retval = ShortType()
        elif first_token == "tinyint":
            retval = ByteType()
        elif first_token == "interval":
            raise ValueError("Interval is invalid type for field definition")
        elif first_token == "array":
            try:
                assert len(ast) == 2, "array must have nested type"
                inner_type = cls._parse_ast(ast[1])
                retval = ArrayType(inner_type)
            except Exception as e:
                raise ValueError(f"Could not construct array type definition due to `{e}`") from e
        elif first_token == "map":
            try:
                assert len(ast) == 3, "map must have 2 inner types"
                inner_type1 = cls._parse_ast(ast[1])
                inner_type2 = cls._parse_ast(ast[2])
                retval = MapType(inner_type1, inner_type2)
            except Exception as e:
                raise ValueError(f"Could not construct map type definition due to `{e}`") from e
        elif first_token == "struct":
            try:
                assert len(ast) == 2, f"struct must have nested list of fields : {len(ast)}"
                field_list_defns = ast[1]
                fields = []
                assert len(field_list_defns) > 0, "field list must be non-empty"

                for field_defn in field_list_defns:
                    field_name = field_defn[0]
                    field_type = cls._parse_ast(field_defn[1])
                    fields.append(StructField(field_name, field_type))
                retval = StructType(fields)
            except Exception as e:
                raise ValueError(f"Could not construct struct type definition due to `{e}`") from e
        else:
            raise ValueError(f" Invalid or unsupported type `{first_token}` found while processing `{list(ast)}`")
        return retval

    @classmethod
    def columnTypeFromString(cls, type_string):
        """ Generate a Spark SQL data type from a string

        Allowable options for `type_string` parameter are:
         * `string`, `varchar`, `char`, `nvarchar`,
         * `int`, `integer`,
         * `bigint`, `long`,
         * `bool`, `boolean`,
         * `smallint`, `short`
         * `binary`
         * `tinyint`, `byte`
         * `date`
         * `timestamp`, `datetime`,
         * `double`, `float`, `date`, `short`, `byte`,
         * `decimal` or `decimal(p)` or `decimal(p, s)` or `number(p, s)`
         * `map<type1, type2>` where type1 and type2 are type definitions of form accepted by parser
         * `array<type1>` where type1 is type definitions of form accepted by parser
         * `struct<a:binary, b:int, c:float>`

         Type definitions may be nested recursively - for example, the following are valid type definitions:
         * `array<array<int>>
         * `struct<a:array<int>, b:int, c:float>`
         * `map<string, struct<a:array<int>, b:int, c:float>>`

        :param type_string: String representation of SQL type such as 'integer' etc.
        :returns: Spark SQL type
        """
        assert type_string is not None, "`type_string` must be specified"

        parser = cls.getTypeDefinitionParser()

        # generate abstract syntax tree (AST) from parsed definition
        try:
            ast = parser.parseString(type_string)
        except Exception as e:
            raise ValueError(f"Invalid type definition `{type_string}`", e) from e

        type_construct = cls._parse_ast(ast)

        return type_construct

    @classmethod
    def _cleanseSQL(cls, sql_string):
        """ Cleanse sql string removing string literals so that they are not considered as part of potential column
            references
        :param sql_string: String representation of SQL expression
        :returns: cleansed string

        Any strings identified are replaced with `' '`
        """
        assert sql_string is not None, "`sql_string` must be specified"

        # skip over quoted identifiers even if they contain quotes
        quoted_ident = pp.QuotedString(quoteChar="`", escQuote="``")
        quoted_ident.setParseAction(lambda s, loc, toks: f"`{toks[0]}`")

        stringForm1 = pp.Literal('r') + pp.QuotedString(quoteChar="'")
        stringForm2 = pp.Literal('r') + pp.QuotedString(quoteChar='"')
        stringForm3 = pp.QuotedString(quoteChar="'", escQuote=r"\'")
        stringForm4 = pp.QuotedString(quoteChar='"', escQuote=r'\"')
        stringForm = stringForm1 ^ stringForm2 ^ stringForm3 ^ stringForm4
        stringForm.setParseAction(lambda s, loc, toks: "' '")

        parser = quoted_ident ^ stringForm

        transformed_string = parser.transformString(sql_string)

        return transformed_string

    @classmethod
    def columnsReferencesFromSQLString(cls, sql_string, filterItems=None):
        """ Generate a list of possible column references from a SQL string

        This method finds all condidate references to SQL columnn ids in the string

        To avoid the overhead of a full SQL parser, the implementation will simply look for possible field names

        Further improvements may eliminate some common syntax but in current form, reserved words will
        also be returned as possible column references.

        So any uses of this must not assume that all possible references are valid column references

        :param sql_string: String representation of SQL expression
        :param filterItems: filter results to only results in items listed
        :returns: list of possible column references
        """
        assert sql_string is not None, "`sql_string` must be specified"
        assert filterItems is None or isinstance(filterItems, (list, set))

        cleansed_sql_string = cls._cleanseSQL(sql_string)

        ident = pp.Word(pp.alphas, pp.alphanums + "_") | pp.QuotedString(quoteChar="`", escQuote="``")
        parser = ident

        references = parser.searchString(cleansed_sql_string)

        results = set([item for sublist in references for item in sublist])

        if filterItems is not None:
            filtered_results = results.intersection(set(filterItems))
            return list(filtered_results)
        else:
            return list(results)

    @classmethod
    def parseCreateTable(cls, sparkSession, source_schema):
        """ Parse a schema from a schema string

            :param sparkSession: spark session to use
            :param source_schema: should be a table definition minus the create table statement
            :returns: Spark SQL schema instance
        """
        assert (source_schema is not None), "`source_schema` must be specified"
        assert (sparkSession is not None), "`sparkSession` must be specified"
        lines = [x.strip() for x in source_schema.split("\n") if x is not None]
        table_defn = " ".join(lines)

        # get table name from s
        table_name_match = re.search(r"^\s*create\s*(temporary\s*)?table\s*([a-zA-Z0-9_]*)\s*(\(.*)$", table_defn,
                                     flags=re.IGNORECASE)

        if table_name_match:
            table_name = table_name_match.group(2)
            table_body = table_name_match.group(3)
        else:
            raise ValueError("Table name could not be found")

        sparkSession.sql(f"create temporary table {table_name}{table_body}using csv")

        result = sparkSession.sql(f"select * from {table_name}")
        result.printSchema()
        result_schema = result.schema

        sparkSession.sql(f"drop table {table_name}")

        return result_schema
