# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
DOC_DIR     = .
BUILDDIR      = `pwd`/build
MODULEDIR      = ../dbldatagen
PROJECT_DIR      = ..

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help apidoc Makefile clean quick_index

clean: Makefile
	@echo "$(SPHINXBUILD) -M $@ '$(SOURCEDIR)' '$(BUILDDIR)' $(SPHINXOPTS) $(O)"
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@- rm "$(SOURCEDIR)/reference/api"/*.rst
	@- rm "$(SOURCEDIR)/relnotes"/*.*
	@- rm "htmldocs"/*.*


relnotes: Makefile
	@echo "Copying release notes etc"
	@mkdir -p source/relnotes
	@cp "$(PROJECT_DIR)"/CHANGELOG.md "$(SOURCEDIR)"/relnotes/
	@cp "$(PROJECT_DIR)"/CONTRIBUTING.md "$(SOURCEDIR)"/relnotes/

markdown: Makefile
	@echo "Making md5 files"
	@$(SPHINXBUILD) -M markdown "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

apidoc: Makefile
	@echo "Making APIDOC"
	@sphinx-apidoc -e -o "$(SOURCEDIR)/reference/api" "$(MODULEDIR)"
	#@sphinx-autogen -o "$(SOURCEDIR)/generated" `pwd`/"$(SOURCEDIR)/reference/api/*.rst"

requirements:
	@echo "Making requirements"
	@python3 ./utils/mk_requirements.py "$(PROJECT_DIR)/python/require.txt" "$(SOURCEDIR)/relnotes"/requirements.md

quick_index:
	@echo "Making quick index"
	python3 ./utils/mk_quick_index.py "$(MODULEDIR)" "$(SOURCEDIR)/relnotes"/quickindex.rst
	@echo "done"


license:
	@echo "Making license"
	@cp -f "$(PROJECT_DIR)/LICENSE" "$(SOURCEDIR)/relnotes"/LICENSE.txt

dependent_files: relnotes requirements license quick_index
	@echo "Making dependent files"
	@-mkdir source/relnotes
	@cp -f "$(DOC_DIR)/APIDOCS.md" "$(SOURCEDIR)"/relnotes/
	@cp -f "$(DOC_DIR)/DATARANGES.md" "$(SOURCEDIR)"/relnotes/
	@cp -f "$(DOC_DIR)/DISTRIBUTIONS.md" "$(SOURCEDIR)"/relnotes/


html: Makefile
	@echo "Making HTML"
	@echo "$(SPHINXBUILD) -M $@ '$(SOURCEDIR)' '$(BUILDDIR)' $(SPHINXOPTS) $(O)"
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

docs: clean dependent_files Makefile apidoc html
	@echo "Making full docs"
	@echo "Full path to HTML docs is [$(BUILDDIR)/html]"
	@echo "The starting point for the documentation is '[$(BUILDDIR)/html/index.html]'"

public-docs: docs
	@echo "Making github pages docs from $(BUILDDIR)/html"
	@echo "cp -R $(BUILDDIR)/html/* ./public_docs/"
	@cp -R $(BUILDDIR)/html/* ./public_docs/


# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@echo "$(SPHINXBUILD) -M $@ '$(SOURCEDIR)' '$(BUILDDIR)' $(SPHINXOPTS) $(O)"
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
