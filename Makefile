PYTHON=`which python`
DESTDIR=/
PROJECT=tron
BUILDIR=$(CURDIR)/debian/$PROJECT
VERSION=`$(PYTHON) setup.py --version`
DOT=dot -Tpng

SPHINXBUILD=sphinx-build
DOCS_DIR=docs
DOCS_BUILDDIR=docs/_build
DOCS_STATICSDIR=$(DOCS_DIR)/images
ALLSPHINXOPTS=-d $(DOCS_BUILDDIR)/doctrees $(SPHINXOPTS)

PYFLAKES=pyflakes
PEP8=pep8
PEP8IGNORE=E22,E23,E24,E302,E401
PEP8MAXLINE=100

.PHONY : all source install clean tests docs

all:
		@echo "make source - Create source package"
		@echo "make build - Build from source"
		@echo "make install - Install on local system"
		@echo "make rpm - Generate a rpm package"
		@echo "make deb - Generate a deb package"
		@echo "make clean - Get rid of scratch and byte files"

source:
		$(PYTHON) setup.py sdist $(COMPILE)

build:
		$(PYTHON) setup.py build $(COMPILE)

install:
		$(PYTHON) setup.py install --root $(DESTDIR) $(COMPILE)

rpm:
		$(PYTHON) setup.py bdist_rpm --post-install=rpm/postinstall --pre-uninstall=rpm/preuninstall

deb: man
		# build the source package in the parent directory
		# then rename it to project_version.orig.tar.gz
		$(PYTHON) setup.py sdist $(COMPILE) --dist-dir=../
		rename -f 's/$(PROJECT)-(.*)\.tar\.gz/$(PROJECT)_$$1\.orig\.tar\.gz/' ../*
		# build the package
		dpkg-buildpackage -i -I -rfakeroot -uc -us

clean:
		$(PYTHON) setup.py clean
		rm -rf build/ MANIFEST
		find . -name '*.pyc' -delete
		find . -name "._*" -delete
		rm -rf $(DOCS_BUILDDIR)/*
		rm -rf $(DOCS_STATICSDIR)/*
		fakeroot $(MAKE) -f $(CURDIR)/debian/rules clean

coffee:
		mkdir -p tronweb/js/cs
		coffee -o tronweb/js/cs/ -c tronweb/coffee/

# TODO: add less target, and web target

docs:
	PYTHONPATH=. $(PYTHON) tools/state_diagram.py
	mkdir -p $(DOCS_STATICSDIR)
	$(DOT) -o$(DOCS_STATICSDIR)/action.png action.dot
	$(DOT) -o$(DOCS_STATICSDIR)/service_instance.png service_instance.dot
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(DOCS_BUILDDIR)/html."

doc: docs

man:
	$(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_DIR)/man
	@echo
	@echo "Build finished. The manual pages are in $(DOCS_BUILDDIR)/man."

style:
	@echo "PyFlakes check:"
	-$(PYFLAKES) .
	@echo "\nPEP8 check:"
	-$(PEP8) --ignore=$(PEP8IGNORE) --max-line-length=$(PEP8MAXLINE) .

tests:
	PYTHONPATH=.:bin testify -x sandbox -x mongodb -x integration tests

test: tests

test_all:
	PYTHONPATH=.:bin testify tests --summary
