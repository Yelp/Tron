VERSION=$(shell python setup.py --version)
DOCKER_RUN=docker run -t -v $(CURDIR)/:/work:rw tron-deb-builder

.PHONY : all source install clean tests docs

all:
	@echo "make source - Create source package"
	@echo "make build - Build from source"
	@echo "make install - Install on local system"
	@echo "make rpm - Generate a rpm package"
	@echo "make deb - Generate a deb package"
	@echo "make clean - Get rid of scratch and byte files"
	@echo "make publish - publish to pypi.python.org"

build_%_docker:
	[ -d dist ] || mkdir dist
	cd ./yelp_package/$*/ && docker build -t tron-deb-builder .

itest_trusty: package_trusty_deb

package_%_deb: clean  build_%_docker tronweb/js/cs
	$(DOCKER_RUN) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"

publish:
	python setup.py sdist bdist_wheel
	twine upload dist/*

clean:
	rm -rf tronweb/js/cs
	find . -name '*.pyc' -delete

COFFEE := $(shell which coffee 2 > /dev/null)
tronweb/js/cs:
ifdef COFFEE
	$(error coffee is missing. please install coffeescript)
else
	$(DOCKER_RUN) mkdir -p tronweb/js/cs
	$(DOCKER_RUN) coffee -o tronweb/js/cs/ -c tronweb/coffee/
endif

docs:
	tox -e docs

man:
	which $(SPHINXBUILD) >/dev/null && $(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_DIR)/man || true
	@echo
	@echo "Build finished. The manual pages are in $(DOCS_BUILDDIR)/man."

tests:
	tox

test: tests

test_all:
	PYTHONPATH=.:bin testify tests --summary

LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B | sed -e 's/\x27/"/g')
release: build_trusty_docker docs
	$(DOCKER_RUN) dch -v $(VERSION) --distribution trusty --changelog ./debian/changelog $$'$(VERSION) tagged with \'make release\'\rCommit: $(LAST_COMMIT_MSG)'
	@git diff
	@echo "Now Run:"
	@echo 'git commit -a -m "Released $(VERSION) via make release"'
	@echo 'git tag --force v$(VERSION)'
	@echo 'git push --tags origin master'
