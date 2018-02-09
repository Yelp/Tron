VERSION=$(shell python setup.py --version)
DOCKER_RUN = docker run -t -v $(CURDIR):/work:rw tron-deb-builder
UID:=$(shell id -u)
GID:=$(shell id -g)

.PHONY : all clean tests docs

-usage:
	@echo "make test - Run tests"
	@echo "make package_trusty_deb - Generate trusty package"
	@echo "make release - Prepare debian info for new release"
	@echo "make clean - Get rid of scratch and byte files"

build_trusty_docker:
	[ -d dist ] || mkdir -p dist
	cd ./yelp_package/trusty && docker build -t tron-deb-builder .

package_trusty_deb: clean build_trusty_docker coffee
	$(DOCKER_RUN) /bin/bash -c '                \
		dpkg-buildpackage -d &&                   \
		mv ../*.deb dist/ &&                      \
		chown -R $(UID):$(GID) dist debian        \
	'

coffee:
	$(DOCKER_RUN) /bin/bash -c '                     \
		mkdir -p tronweb/js/cs &&                      \
		coffee -o tronweb/js/cs/ -c tronweb/coffee/ && \
		chown -R $(UID):$(GID) tronweb/js/cs/          \
	'

test:
	tox

_itest:
	$(DOCKER_RUN) /work/itest.sh

itest_trusty: test package_trusty_deb _itest

# Release

LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B | sed -e 's/\x27/"/g')
release: build_trusty_docker docs
	$(DOCKER_RUN) /bin/bash -c " \
		dch -v $(VERSION) --distribution trusty --changelog debian/changelog \
			$$'$(VERSION) tagged with \'make release\'\rCommit: $(LAST_COMMIT_MSG)' && \
		chown $(UID):$(GID) debian/changelog \
"
	@git diff
	@echo "Now Run:"
	@echo 'git commit -a -m "Released $(VERSION) via make release"'
	@echo 'git tag --force v$(VERSION)'
	@echo 'git push --tags origin master'

# Docs

docs:
	tox -e docs

man:
	which $(SPHINXBUILD) >/dev/null && $(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_DIR)/man || true
	@echo
	@echo "Build finished. The manual pages are in $(DOCS_BUILDDIR)/man."

clean:
	rm -rf tronweb/js/cs
	find . -name '*.pyc' -delete
