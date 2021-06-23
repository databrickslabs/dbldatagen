import sys

print("""Building Requirements markdown document""")
print("args", str(sys.argv))

with open(sys.argv[1]) as reader:
    lines = reader.readlines()

    with open(sys.argv[2], "w") as writer:
        writer.write("# Dependencies for the Test Data Generator framework\n\n")
        writer.write("Building the framework will install a number of packages in the virtual environment used for building the framework.\n")
        writer.write("Some of these are required at runtime, while a number of packages are used for building the documentation only.\n\n")
        for line in lines:
            line =line.strip()
            if line.startswith("#"):
                line = line[1:]
            elif not len(line) == 0:
                line = "* " + line
            writer.write(line + "\n")
