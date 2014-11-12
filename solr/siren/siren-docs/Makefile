# Makefile for the SIREn documentation
#

PROJECTNAME      = siren-manual

BUILDDIR         = $(CURDIR)/target

SRCDIR           = $(CURDIR)/src/main/asciidoc
SRCFILE          = $(SRCDIR)/index.asciidoc

RSCDIR           = $(CURDIR)/src/main/resources/
IMGDIR           = $(RSCDIR)/images
CSSDIR           = $(RSCDIR)/css
JSDIR            = $(RSCDIR)/js
CONFDIR          = $(RSCDIR)/conf

SCRIPTDIR        = $(CURDIR)/src/main/sh

DOCBOOKFILE      = $(BUILDDIR)/$(PROJECTNAME).xml
CHUNKEDTARGET    = $(BUILDDIR)/$(PROJECTNAME).chunked
CHUNKEDHTMLDIR   = $(BUILDDIR)/chunked
CHUNKEDWPDIR     = $(BUILDDIR)/wp_chunked

ASCIIDOC         = asciidoc
A2X              = a2x

ifdef VERBOSE
	V = -v
	VA = VERBOSE=1
endif

ifdef KEEP
	K = -k
	KA = KEEP=1
endif

ASCIIDOC_FLAGS = $(V)

A2X_FLAGS = $(K) $(ASCIIDOC_FLAGS)

.PHONY: all dist docbook help clean html cleanup

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean       to clean the build directory"
	@echo "  html        to make standalone HTML files"
	@echo "For verbose output, use 'VERBOSE=1'".
	@echo "To keep temporary files, use 'KEEP=1'".

dist: html cleanup

clean:
	-rm -rf "$(BUILDDIR)/"*

cleanup:
	#
	#
	# Cleaning up.
	#
	#
ifndef KEEP
	rm -f "$(DOCBOOKFILE)"
	rm -f "$(BUILDDIR)/"*.xml
endif

copyimages:
	#
	#
	# Copying images from source projects.
	#
	#
	"$(SCRIPTDIR)/copy-images.sh" "$(SRCDIR)" "$(BUILDDIR)/images"

docbook: copyimages
	#
	#
	# Building docbook output.
	#
	#
	"$(ASCIIDOC)" $(ASCIIDOC_FLAGS) --backend docbook --doctype book --conf-file="$(CONFDIR)/asciidoc.conf" --out-file "$(DOCBOOKFILE)" "$(SRCFILE)" 2>&1

html: copyimages docbook
	#
	#
	# Building html output.
	#
	#
	"$(A2X)" $(V) -L -f chunked -D "$(BUILDDIR)" --xsl-file="$(CONFDIR)/chunked.xsl" -r "$(BUILDDIR)/images" -r "$(IMGDIR)" -r "$(CSSDIR)" --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" --xsltproc-opts "--stringparam toc.section.depth 1" "$(DOCBOOKFILE)" 2>&1
	rm -rf "$(CHUNKEDHTMLDIR)"
	mv "$(CHUNKEDTARGET)" "$(CHUNKEDHTMLDIR)"
	cp -fr "$(JSDIR)" "$(CHUNKEDHTMLDIR)/js"
	cp -fr "$(CSSDIR)/"* "$(CHUNKEDHTMLDIR)/css"


wordpress: copyimages docbook
	#
	#
	# Building wordpress output.
	#
	#
	"$(A2X)" $(V) -v -L -f chunked -D "$(BUILDDIR)" --xsl-file="$(CONFDIR)/wp_chunked.xsl" -r "$(BUILDDIR)/images" -r "$(IMGDIR)" -r "$(CSSDIR)" --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" --xsltproc-opts "--stringparam toc.section.depth 1" "$(DOCBOOKFILE)" 2>&1
	rm -rf "$(CHUNKEDWPDIR)"
	mv "$(CHUNKEDTARGET)" "$(CHUNKEDWPDIR)"
	cp -fr "$(JSDIR)" "$(CHUNKEDWPDIR)/js"
	cp -fr "$(CSSDIR)/"* "$(CHUNKEDWPDIR)/css"


docbook-pdf: copyimages
	#
	#
	# Building docbook output.
	#
	#
	"$(ASCIIDOC)" $(ASCIIDOC_FLAGS) --backend docbook --doctype book --attribute nonhtmloutput=1 --conf-file="$(CONFDIR)/asciidoc.conf" --out-file "$(DOCBOOKFILE)" "$(SRCFILE)" 2>&1

pdf: copyimages docbook
	#
	#
	# Building pdf output.
	#
	#
	"$(A2X)" $(V) -d book --xsl-file="$(CONFDIR)/fo.xsl" -f pdf --fop --fop-opts='-c $(CONFDIR)/fop.xml' "$(DOCBOOKFILE)" 2>&1
