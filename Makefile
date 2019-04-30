VERSION=$(shell python setup.py --version)
DOCKER_RUN = docker run -t -v $(CURDIR):/work:rw -v $(CURDIR)/.tox-indocker:/work/.tox:rw
UID:=$(shell id -u)
GID:=$(shell id -g)

.PHONY : all clean tests docs dev cluster_itests

-usage:
	@echo "make test - Run tests"
	@echo "make deb_xenial - Generate xenial deb package"
	@echo "make itest_xenial - Run tests and integration checks"
	@echo "make _itest_xenial - Run only integration checks"
	@echo "make release - Prepare debian info for new release"
	@echo "make clean - Get rid of scratch and byte files"
	@echo "make dev - Get a local copy of trond running in debug mode in the foreground"

docker_%:
	@echo "Building docker image for $*"
	[ -d dist ] || mkdir -p dist
	cd ./yelp_package/$* && docker build -t tron-builder-$* .

deb_%: clean docker_% coffee_%
	@echo "Building deb for $*"
	$(DOCKER_RUN) tron-builder-$* /bin/bash -c ' \
		dpkg-buildpackage -d &&                  \
		mv ../*.deb dist/ &&                     \
		rm -rf debian/tron &&                    \
		chown -R $(UID):$(GID) dist debian       \
	'

coffee_%: docker_%
	@echo "Building tronweb"
	$(DOCKER_RUN) tron-builder-$* /bin/bash -c '       \
		rm -rf tronweb/js/cs &&                        \
		mkdir -p tronweb/js/cs &&                      \
		coffee -o tronweb/js/cs/ -c tronweb/coffee/ && \
		chown -R $(UID):$(GID) tronweb/js/cs/          \
	'

test:
	tox -e py36

test_in_docker_%: docker_%
	$(DOCKER_RUN) tron-builder-$* python3.6 -m tox -vv -e py36

tox_%:
	tox -e $*

_itest_%:
	$(DOCKER_RUN) ubuntu:$* /work/itest.sh

debitest_%: deb_% _itest_%
	@echo "Package for $* looks good"

itest_%: debitest_%
	@echo "itest $* OK"

cluster_itests:
	tox -e cluster_itests

dev:
	SSH_AUTH_SOCK=$(SSH_AUTH_SOCK) .tox/py36/bin/trond --debug --working-dir=dev -l logging.conf --host=$(shell hostname -f)

example_cluster:
	tox -e example-cluster

LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B | sed -e 's/[\x27\x22]/\\\x27/g')
release: docker_xenial docs
	$(DOCKER_RUN) tron-builder-xenial /bin/bash -c " \
		dch -v $(VERSION) --distribution xenial --changelog debian/changelog \
			$$'$(VERSION) tagged with \'make release\'\rCommit: $(LAST_COMMIT_MSG)' && \
		chown $(UID):$(GID) debian/changelog \
	"
	@git diff
	@echo "Now Run:"
	@echo 'git commit -a -m "Released $(VERSION) via make release"'
	@echo 'git tag --force v$(VERSION)'
	@echo 'git push --tags origin master'

docs:
	tox -r -e docs

man:
	which $(SPHINXBUILD) >/dev/null && $(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_DIR)/man || true
	@echo
	@echo "Build finished. The manual pages are in $(DOCS_BUILDDIR)/man."

clean:
	rm -rf tronweb/js/cs
	find . -name '*.pyc' -delete
