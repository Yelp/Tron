PYTHON=`which python`
DESTDIR=/
PROJECT=tron
BUILDIR=$(CURDIR)/debian/$PROJECT
VERSION=`$(PYTHON) setup.py --version`

.PHONY : all source install clean devinstall

all:
		@echo "make source - Create source package"
		@echo "make install - Install on local system"
		@echo "make buildrpm - Generate a rpm package"
		@echo "make builddeb - Generate a deb package"
		@echo "make clean - Get rid of scratch and byte files"

source:
		$(PYTHON) setup.py sdist $(COMPILE)

install:
		$(PYTHON) setup.py install --root $(DESTDIR) $(COMPILE)

buildrpm:
		$(PYTHON) setup.py bdist_rpm --post-install=rpm/postinstall --pre-uninstall=rpm/preuninstall

builddeb:
		# build the source package in the parent directory
		# then rename it to project_version.orig.tar.gz
		$(PYTHON) setup.py sdist $(COMPILE) --dist-dir=../ --prune
		rename -f 's/$(PROJECT)-(.*)\.tar\.gz/$(PROJECT)_$$1\.orig\.tar\.gz/' ../*
		# build the package
		dpkg-buildpackage -i -I -rfakeroot -uc -us

clean:
		$(PYTHON) setup.py clean
		fakeroot $(MAKE) -f $(CURDIR)/debian/rules clean
		rm -rf build/ MANIFEST
		find . -name '*.pyc' -delete
		find . -name "._*" -delete

devclean:
		rm -rf env

env:
		mkdir -f env

env/virtualenv.install: env
		virtualenv env
		touch $@

devinstall: env env/lib/tron.install

env/lib/tron.install :
		pip -E env install -e .
		mkdir -p env/var/tron
		touch $@