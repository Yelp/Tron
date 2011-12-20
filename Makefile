PYTHON=`which python`
DESTDIR=/
PROJECT=tron
BUILDIR=$(CURDIR)/debian/$PROJECT
VERSION=`$(PYTHON) setup.py --version`

SPHINXBUILD=sphinx-build
DOCS_DIR=docs
DOCS_BUILDDIR=docs/_build
ALLSPHINXOPTS=-d $(DOCS_BUILDDIR)/doctrees $(SPHINXOPTS)

.PHONY : all source install clean

all:
		@echo "make source - Create source package"
		@echo "make install - Install on local system"
		@echo "make rpm - Generate a rpm package"
		@echo "make deb - Generate a deb package"
		@echo "make clean - Get rid of scratch and byte files"

source:
		$(PYTHON) setup.py sdist $(COMPILE)

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
		fakeroot $(MAKE) -f $(CURDIR)/debian/rules clean

html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(DOCS_BUILDDIR)/html."

man: 
	$(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_DIR)/man
	@echo
	@echo "Build finished. The manual pages are in $(DOCS_BUILDDIR)/man."
