import re
import sys
from pathlib import Path

# metadata about source files
""" Metadata controlling index entries for types in the source files

The metadata has brief description and a grouping for each file

The generation of the quick index will find the metadata for each file and use the `grouping` attribute
 to group related classes , types and functions together.
 
The brief description attribute will be used to provide a brief description of classes, types and functions
for a given module.

NOTE: when a new source code file is added, an entry should be added for that file in this metadata.

"""
SOURCE_FILES = {
    "data_generator.py": {"briefDesc": "Main generator classes",
                          "grouping": "main classes"},
    "column_generation_spec.py": {"briefDesc": "Column Generation Spec types",
                                  "grouping": "internal classes"},
    "column_spec_options.py": {"briefDesc": "Column Generation Options",
                               "grouping": "main classes"},
    "datarange.py": {"briefDesc": "Internal data range abstract types",
                     "grouping": "main classes"},
    "daterange.py": {"briefDesc": "Date and time ranges",
                     "grouping": "main classes"},
    "nrange.py": {"briefDesc": "Numeric ranges",
                  "grouping": "main classes"},
    "text_generators.py": {"briefDesc": "Text data generation",
                           "grouping": "main classes"},
    "text_generator_plugins.py": {"briefDesc": "Text data generation",
                                  "grouping": "main classes"},
    "data_analyzer.py": {"briefDesc": "Analysis of existing data",
                         "grouping": "main classes"},
    "function_builder.py": {"briefDesc": "Internal utilities to create functions related to weights",
                            "grouping": "internal classes"},
    "schema_parser.py": {"briefDesc": "Internal utilities to parse Spark SQL schema information",
                         "grouping": "internal classes"},
    "spark_singleton.py": {"briefDesc": "Spark singleton for test purposes",
                           "grouping": "internal classes"},
    "utils.py": {"briefDesc": "",
                 "grouping": "internal classes"},
    "html_utils.py": {"briefDesc": "",
                 "grouping": "internal classes"},

    "beta.py": {"briefDesc": "Beta distribution related code",
                "grouping": "data distribution"},
    "data_distribution.py": {"briefDesc": "Data distribution related code",
                             "grouping": "data distribution"},
    "normal_distribution.py": {"briefDesc": "Normal data distribution related code",
                               "grouping": "data distribution"},
    "gamma.py": {"briefDesc": "Gamma data distribution related code",
                 "grouping": "data distribution"},
    "exponential_distribution.py": {"briefDesc": "Exponential data distribution related code",
                                    "grouping": "data distribution"},

    "constraint.py": {"briefDesc": "Constraint related code",
                "grouping": "data generation constraints"},
    "chained_relation.py": {"briefDesc": "ChainedInequality constraint related code",
                             "grouping": "data generation constraints"},
    "value_multiple_constraint.py": {"briefDesc": "FixedIncrement constraint related code",
                               "grouping": "data generation constraints"},
    "negative_values.py": {"briefDesc": "Negative constraint related code",
                 "grouping": "data generation constraints"},
    "positive_values.py": {"briefDesc": "Positive constraint related code",
                                    "grouping": "data generation constraints"},
    "literal_relation_constraint.py": {"briefDesc": "Scalar inequality constraint related code",
                               "grouping": "data generation constraints"},
    "literal_range_constraint.py": {"briefDesc": "ScalarRange constraint related code",
                                        "grouping": "data generation constraints"},
    "sql_expr.py": {"briefDesc": "SQL expression constraint related code",
                                        "grouping": "data generation constraints"},

}

# grouping metadata information
# note that the GROUPING_INFO will be output in the order that they appear here
GROUPING_INFO = {
    "main classes": {
        "heading": "Main user facing classes, functions and types"
    },
    "internal classes": {
        "heading": "Internal classes, functions and types"
    },
    "data distribution": {
        "heading": "Data distribution related classes, functions and types"
    },
    "data generation constraints": {
        "heading": "Data generation constraints related classes, functions and types"
    }
}

PACKAGE_NAME = "dbldatagen"
PROJECT_PATH = f"../{PACKAGE_NAME}"


def writeUnderlined(outputFile, text, underline="="):
    """ write underlined text in RST markup format

    :param outputFile: output file to write to
    :param text:  text to write
    :param underline: character for underline
    :return: nothing
    """
    assert outputFile is not None
    assert text is not None and len(text) > 0

    lenText = len(text)
    outputFile.write(text)
    outputFile.write("\n")
    outputFile.write(underline * lenText)
    outputFile.write("\n\n")


class FileMeta:
    UNGROUPED = "ungrouped"
    GROUPING = "grouping"
    BRIEF_DESCRIPTION = "briefDesc"

    def __init__(self, moduleName: str, metadata, classes, functions, types, subpackage: str = None):
        """ Constructor for File Meta

        :param moduleName:
        :param metadata:
        :param classes:
        :param functions:
        :param types:
        :param subpackage:
        """
        self.moduleName, self.metadata, self.subPackage = moduleName, metadata, subpackage
        self.classes, self.functions, self.types = classes, functions, types
        print("creating file metadata for module: ", moduleName, ", with metadata: ", metadata)
        assert self.metadata[self.GROUPING] is not None
        if metadata is not None and type(metadata) is dict:
            self.description = self.metadata[self.BRIEF_DESCRIPTION]
            if self.GROUPING in metadata:
                self.grouping = self.metadata[self.GROUPING]
            else:
                self.grouping = self.UNGROUPED

    @property
    def isPopulated(self):
        """ Check if instance has any classes, functions or types"""
        return ((self.classes is not None and len(self.classes) > 0)
                or (self.functions is not None and len(self.functions) > 0)
                or (self.types is not None and len(self.types) > 0))


def findMembers(sourceFile, fileMetadata, fileSubpackage):
    """  Find classes, types and functions in file

    :param fileMetadata: metadata for file
    :param fileSubpackage: subpackage for file
    :param sourceFile: file to search
    :return: tuple of classes, types and functions
    """
    class_pattern = re.compile(r"^class\s+([\w_]+)")
    function_pattern = re.compile(r"^def\s+([\w_]+)")
    types_pattern = re.compile(r"^([\w_]+)\s*=")

    classes = []
    functions = []
    types = []

    with open(sourceFile, 'r') as fp:
        fname = fp.name
        print("scanning module for members:", Path(fp.name).stem)
        try:
            for line in fp:
                classNames = class_pattern.findall(line)
                classes.extend(classNames)
                functionNames = function_pattern.findall(line)

                filteredFunctionNames = [fn for fn in functionNames if not fn.startswith("_")]
                functions.extend(filteredFunctionNames)

                typeNames = types_pattern.findall(line)
                filteredTypeNames = [typ for typ in typeNames if not typ.startswith("_")]
                types.extend(filteredTypeNames)

        except Exception as e:
            print(f"*** failed to process file: {fname}")

    return FileMeta(moduleName=Path(sourceFile.name).stem,
                    metadata=fileMetadata,
                    subpackage=fileSubpackage,
                    classes=sorted(classes),
                    functions=sorted(functions),
                    types=sorted(types))


def includeTemplate(outputFile):
    """
    Include template in output

    :param outputFile:
    :return: nothing
    """
    with open('utils/template_quick_index.rst', 'r') as templateFile:
        outputFile.write(templateFile.read())
        outputFile.write("\n\n")


def processItemList(outputFile, items, sectionTitle, subpackage=None):
    """ process list of items

    :param outputFile: output file instance
    :param items: list of items. each item is a tuple of ( "moduleName.typename", "type description")
    :param sectionTitle: title of section
    :param subpackage: optional subpackage name
    :return: nothing
    """
    if items is not None and len(items) > 0:
        if sectionTitle is not None and len(sectionTitle) > 0:
            outputFile.write(f"{sectionTitle}\n\n")

        for item in items:
            desc = ""
            if item[1] is not None and len(item[1]) > 0:
                desc = f" - {item[1]}"
            if subpackage is not None:
                outputFile.write(f"* :data:`~{PACKAGE_NAME}.{subpackage}.{item[0]}`{desc}\n")
            else:
                outputFile.write(f"* :data:`~{PACKAGE_NAME}.{item[0]}`{desc}\n")

        outputFile.write("\n")


def processDirectory(outputFile, pathToProcess, subpackage=None):
    """ process directory for package or subpackage

    :param outputFile: output file instance
    :param pathToProcess: path to process
    :param subpackage: subpackage to process
    :return: nothing
    """
    projectDirectory = Path(PROJECT_PATH)
    fileGroupings = {}  # map of lists of file metainfo keyed by grouping

    if pathToProcess.exists():
        filesToProcess = pathToProcess.glob("*.py")  # get list of python files in path

        # ... and process them - adding each file's members to the metadata for that file
        for fp in filesToProcess:
            relativeFile = fp.relative_to(projectDirectory)  # compute relative file name
            print(f"processing file: [{fp}], relative_name: [{relativeFile}]")
            if relativeFile.name in SOURCE_FILES:
                title = SOURCE_FILES[relativeFile.name]["briefDesc"]

                # get the classes, functions and types for the file
                fileMetaInfo = findMembers(fp,
                                           fileMetadata=SOURCE_FILES[relativeFile.name],
                                           fileSubpackage=subpackage)

                assert fileMetaInfo is not None

                # for now, just store the metadata about the file
                if fileMetaInfo.isPopulated:
                    if fileMetaInfo.grouping in fileGroupings:
                        assert type(fileMetaInfo.grouping) is str
                        assert type(fileGroupings[fileMetaInfo.grouping]) is list
                        fileGroupings[fileMetaInfo.grouping].append(fileMetaInfo)
                    else:
                        newEntry = [fileMetaInfo, ]
                        assert type(newEntry) is list
                        fileGroupings[fileMetaInfo.grouping] = newEntry
            elif not relativeFile.name.startswith("_"):
                # generate warning if relative file name does not begin with `_`
                print(f"*** warning entry not found for : [{relativeFile}]")

        # now processing the grouping information to generate the index entries
        for grp in GROUPING_INFO:
            if grp in fileGroupings:
                writeUnderlined(outputFile, GROUPING_INFO[grp]["heading"], underline="~")
                fileMetaInfoList = fileGroupings[grp]

                # get the list of classes for the package
                classList = [(f"{fileMetaInfo.moduleName}.{cls}", fileMetaInfo.description)
                             for fileMetaInfo in fileMetaInfoList for cls in fileMetaInfo.classes
                             if fileMetaInfo.isPopulated]

                # get the list of functions for the package
                functionList = [(f"{fileMetaInfo.moduleName}.{fn}", fileMetaInfo.description)
                                for fileMetaInfo in fileMetaInfoList for fn in fileMetaInfo.functions
                                if fileMetaInfo.isPopulated]

                # get the list of types for the package
                typeList = [(f"{fileMetaInfo.moduleName}.{typ}", "")
                            for fileMetaInfo in fileMetaInfoList for typ in fileMetaInfo.types
                            if fileMetaInfo.isPopulated]

                # emit each of the sections in the index
                processItemList(outputFile, classList,
                                sectionTitle="Classes",
                                subpackage=subpackage)
                processItemList(outputFile, functionList,
                                sectionTitle="Functions",
                                subpackage=subpackage)
                processItemList(outputFile, typeList,
                                sectionTitle="Types",
                                subpackage=subpackage)


def main(dirToSearch, outputPath):
    dirToSearch = sys.argv[1]
    outputFile = sys.argv[2]
    print(f"scanning dir: {dirToSearch}")
    print(f"writing to output file: {outputPath}")

    with open(outputPath, 'w') as outputFile:
        includeTemplate(outputFile)

        writeUnderlined(outputFile, f"The ``{PACKAGE_NAME}`` package", underline="_")

        processDirectory(outputFile, Path(f"{PROJECT_PATH}"))

        # Add entries here for subpackages
        writeUnderlined(outputFile, f"The ``{PACKAGE_NAME}.constraints`` package", underline="_")
        processDirectory(outputFile, Path(f"{PROJECT_PATH}/constraints"), subpackage="constraints")

        writeUnderlined(outputFile, f"The ``{PACKAGE_NAME}.distributions`` package", underline="_")
        processDirectory(outputFile, Path(f"{PROJECT_PATH}/distributions"), subpackage="distributions")



if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
