import os
import re
from pathlib import Path
import sys

sourceFiles = {
    "data_generator.py": "Main generator classes",
    "column_generation_spec.py" : "Column Generation Spec types",
    "column_spec_options.py": "Column Generation Options",
    "datarange.py": "Internal data range abstract types",
    "daterange.py": "Date and time ranges",
    "nrange.py": "Numeric ranges",
    "text_generators.py": "Text data generation",
    "data_analyzer.py": "Analysis of existing data",
    "function_builder.py": "Internal utilities to create functions related to weights",
    "schema_parser.py": "Internal utilities to parse Spark SQL schema information",
    "spark_singleton.py": "Spark singleton for test purposes",
    "utils.py": "Internal general purpose utils",

    "beta.py": "Beta distribution related code",
    "data_distribution.py": "Data distribution related code",
    "normal_distribution.py": "Normal data distribution related code",
    "gamma.py": "Gamma data distribution related code",
    "exponential_distribution.py": "Exponential data distribution related code"
}

def writeUnderlined(outputFile, text, underline="="):
    assert outputFile is not None
    assert text is not None and len(text) > 0

    lenText = len(text)
    outputFile.write(text)
    outputFile.write("\n")
    outputFile.write(underline * lenText)
    outputFile.write("\n\n")

def find_members(sourceFile):
    '''
    Find classes, types and functions in file
    :param sourceFile: file to search
    :return: tuple of classes, types and functions
    '''
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
                functions.extend(functionNames)
                typeNames = types_pattern.findall(line)
                types.extend(typeNames)

        except Exception as e:
            print(f"*** failed to process file: {fname}")

    return sorted(classes), sorted(types), sorted(functions)



def include_template(outputFile):
    '''
    Include template in output

    :param outputFile:
    :return: nothing
    '''
    with open('utils/template_quick_index.rst', 'r') as templateFile:
        outputFile.write(templateFile.read())
        outputFile.write("\n\n")

PROJECT_PATH="../databricks_datagen"

def processSection(outputFile, items, sectionTitle, module):
    if items is not None and len(items) > 0:
        outputFile.write(f"{sectionTitle}\n\n")

    for item in items:
        outputFile.write(f"* :data:`~databricks_datagen.{module}.{item}`\n")

    outputFile.write("\n")

def processDirectory(outputFile, pathToProcess):
    projectDirectory = Path(PROJECT_PATH)
    print("directory: ", pathToProcess)
    if pathToProcess.exists():
        filesToProcess = pathToProcess.glob("*.py")
        for fp in filesToProcess:
            relativeFile = fp.relative_to(projectDirectory)
            print("processing file:", relativeFile)
            if relativeFile.name in sourceFiles:
                title = sourceFiles[relativeFile.name]
                print(relativeFile, title)
                moduleName = Path(fp.name).stem

                classList, typeList, functionList = find_members(fp)

                # print title
                if len(classList) > 0 or len(functionList) > 0 or len(typeList) > 0:
                    outputFile.write("\n")
                    writeUnderlined(outputFile, title, underline="~")
                    processSection(outputFile, classList, sectionTitle="Classes", module=moduleName)
                    processSection(outputFile, functionList, sectionTitle="Functions", module=moduleName)
                    processSection(outputFile, typeList, sectionTitle="Types", module=moduleName)


def main(dirToSearch, outputPath):
    dirToSearch = sys.argv[1]
    outputFile = sys.argv[2]
    print(f"scanning dir {dirToSearch}")
    print(f"writing to output file {outputPath}")

    with open(outputPath, 'w') as outputFile:
        include_template(outputFile)

        writeUnderlined(outputFile, f"The ``TestDataGenerator`` package",
                        underline="_")

        processDirectory(outputFile, Path(f"{PROJECT_PATH}"))

        writeUnderlined(outputFile, f"The ``TestDataGenerator.distributions`` package",
                        underline="_")

        processDirectory(outputFile, Path(f"{PROJECT_PATH}/distributions"))


main(sys.argv[1], sys.argv[2])

