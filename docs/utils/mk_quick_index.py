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
sourceFiles = {
    "data_generator.py": {"briefDesc": "Main generator classes",
                          "grouping": "main classes"},
    "column_generation_spec.py": {"briefDesc": "Column Generation Spec types",
                                  "grouping": "main classes"},
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

    "beta.py": {"briefDesc": "Beta distribution related code",
                "grouping": "data distribution"},
    "data_distribution.py": {"briefDesc": "Data distribution related code",
                             "grouping": "data distribution"},
    "normal_distribution.py": {"briefDesc": "Normal data distribution related code",
                               "grouping": "data distribution"},
    "gamma.py": {"briefDesc": "Gamma data distribution related code",
                 "grouping": "data distribution"},
    "exponential_distribution.py": {"briefDesc": "Exponential data distribution related code",
                                    "grouping": "data distribution"}
}

# grouping metadata information
# note that the groupings will be output in the order that they appear here
groupings = {
    "main classes": {
        "heading": "Main user facing classes, functions and types"
    },
    "internal classes": {
        "heading": "Internal classes, functions and types"
    },
    "data distribution": {
        "heading": "Data distribution related classes, functions and types"
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
    def __init__(self, moduleName: str, metadata, classes, functions, types,
                 subpackage: str = None):
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
        print("module", moduleName, "metadata", metadata, self.metadata)
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
                or (self.types is not None and  len(self.types) > 0))


def find_members(sourceFile, fileMetadata, fileSubpackage):
    """
    Find classes, types and functions in file
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
        print("module :", Path(fp.name).stem)
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
                    classes=sorted(classes), functions=sorted(functions), types=sorted(types))


def include_template(outputFile):
    '''
    Include template in output

    :param outputFile:
    :return: nothing
    '''
    with open('utils/template_quick_index.rst', 'r') as templateFile:
        outputFile.write(templateFile.read())
        outputFile.write("\n\n")


def processItemList(outputFile, items, sectionTitle, subpackage=None):
    """ process list of items

    :param outputFile:
    :param items: list of items. each item is a tuple of ( "moduleName.typename", "type description")
    :param sectionTitle:
    :param subpackage:
    :return:
    """
    if items is not None and len(items) > 0 and sectionTitle is not None and len(sectionTitle) > 0:
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

    :param outputFile:
    :param pathToProcess:
    :param subpackage:
    :return:
    """
    projectDirectory = Path(PROJECT_PATH)
    fileGroupings = {} # map of lists of file metainfo keyed by grouping

    print("directory: ", pathToProcess)
    if pathToProcess.exists():
        filesToProcess = pathToProcess.glob("*.py")
        for fp in filesToProcess:
            relativeFile = fp.relative_to(projectDirectory)
            print("processing file:", relativeFile)
            if relativeFile.name in sourceFiles:
                print("dict", sourceFiles[relativeFile.name])
                title = sourceFiles[relativeFile.name]["briefDesc"]
                print(relativeFile, title)

                fileMetaInfo = find_members(fp,
                                            fileMetadata=sourceFiles[relativeFile.name],
                                            fileSubpackage=subpackage)

                assert fileMetaInfo is not None
                if fileMetaInfo.isPopulated:
                    if fileMetaInfo.grouping in fileGroupings:
                        print(f"extending entry for `{fileMetaInfo.grouping}`")
                        assert type(fileMetaInfo.grouping) is str
                        assert type(fileGroupings[fileMetaInfo.grouping]) is list
                        fileGroupings[fileMetaInfo.grouping].append(fileMetaInfo)
                    else:
                        print(f"adding new entry for `{fileMetaInfo.grouping}`")
                        newEntry = [fileMetaInfo, ]
                        assert type(newEntry) is list
                        fileGroupings[fileMetaInfo.grouping] = newEntry

        for grp in fileGroupings:
            writeUnderlined(outputFile, groupings[grp]["heading"], underline="~")
            fileMetaInfoList = fileGroupings[grp]

            classList = [(f"{fileMetaInfo.moduleName}.{cls}", fileMetaInfo.description)
                       for fileMetaInfo in fileMetaInfoList for cls in fileMetaInfo.classes
                       if fileMetaInfo.isPopulated]

            functionList = [(f"{fileMetaInfo.moduleName}.{fn}", fileMetaInfo.description)
                       for fileMetaInfo in fileMetaInfoList for fn in fileMetaInfo.functions
                       if fileMetaInfo.isPopulated]

            typeList = [(f"{fileMetaInfo.moduleName}.{typ}", "")
                       for fileMetaInfo in fileMetaInfoList for typ in fileMetaInfo.types
                       if fileMetaInfo.isPopulated]


            print(classList)
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
    print(f"scanning dir {dirToSearch}")
    print(f"writing to output file {outputPath}")

    with open(outputPath, 'w') as outputFile:
        include_template(outputFile)

        writeUnderlined(outputFile, f"The ``{PACKAGE_NAME}`` package",
                        underline="_")

        processDirectory(outputFile, Path(f"{PROJECT_PATH}"))

        writeUnderlined(outputFile, f"The ``{PACKAGE_NAME}.distributions`` package",
                        underline="_")

        processDirectory(outputFile, Path(f"{PROJECT_PATH}/distributions"), subpackage="distributions")


main(sys.argv[1], sys.argv[2])
