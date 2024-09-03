# Edit this release and run "make release"
RELEASE=2.4.1

SHELL=/bin/bash

DOCKER_RUN = docker run -t -v $(CURDIR):/work:rw -v $(CURDIR)/.tox-indocker:/work/.tox:rw
UID:=$(shell id -u)
GID:=$(shell id -g)

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	PAASTA_ENV ?= YELP
else
	PAASTA_ENV ?= $(shell hostname --fqdn)
endif

NOOP = true
ifeq ($(PAASTA_ENV),YELP)
	export PIP_INDEX_URL ?= http://169.254.255.254:20641/$*/simple/
	export NPM_CONFIG_REGISTRY ?= https://npm.yelpcorp.com/
	ADD_MISSING_DEPS_MAYBE:=-diff --unchanged-line-format= --old-line-format= --new-line-format='%L' ./requirements.txt ./yelp_package/extra_requirements_yelp.txt >> ./requirements.txt
else
	export PIP_INDEX_URL ?= https://pypi.python.org/simple
	export NPM_CONFIG_REGISTRY ?= https://registry.npmjs.org
	ADD_MISSING_DEPS_MAYBE:=$(NOOP)
endif

.PHONY : all clean tests docs dev

-usage:
	@echo "make test - Run tests"
	@echo "make deb_bionic - Generate bionic deb package"
	@echo "make itest_bionic - Run tests and integration checks"
	@echo "make _itest_bionic - Run only integration checks"
	@echo "make deb_jammy - Generate bionic deb package"
	@echo "make itest_jammy - Run tests and integration checks"
	@echo "make _itest_jammy - Run only integration checks"
	@echo "make release - Prepare debian info for new release"
	@echo "make clean - Get rid of scratch and byte files"
	@echo "make dev - Get a local copy of trond running in debug mode in the foreground"

docker_%:
	@echo "Building docker image for $*"
	[ -d dist ] || mkdir -p dist
	cd ./yelp_package/$* && docker build --build-arg PIP_INDEX_URL=${PIP_INDEX_URL} --build-arg NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY} -t tron-builder-$* .

deb_%: clean docker_% coffee_%
	@echo "Building deb for $*"
	# backup these files so we can temp modify them
	cp requirements.txt requirements.txt.old
	$(ADD_MISSING_DEPS_MAYBE)
	$(DOCKER_RUN) -e PIP_INDEX_URL=${PIP_INDEX_URL} tron-builder-$* /bin/bash -c ' \
		dpkg-buildpackage -d &&                  \
		mv ../*.deb dist/ &&                     \
		rm -rf debian/tron                    \
	'
	# restore the backed up files
	mv requirements.txt.old requirements.txt

coffee_%: docker_%
	@echo "Building tronweb"
	$(DOCKER_RUN) tron-builder-$* /bin/bash -c '       \
		rm -rf tronweb/js/cs &&                        \
		mkdir -p tronweb/js/cs &&                      \
		coffee -o tronweb/js/cs/ -c tronweb/coffee/ \
	'

test:
	tox -e py38

test_in_docker_%: docker_%
	$(DOCKER_RUN) tron-builder-$* python3.8 -m tox -vv -e py38

tox_%:
	tox -e $*

_itest_%:
	$(DOCKER_RUN) -e NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY} ubuntu:$* /work/itest.sh

debitest_%: deb_% _itest_%
	@echo "Package for $* looks good"

itest_%: debitest_%
	@echo "itest $* OK"

dev:
	SSH_AUTH_SOCK=$(SSH_AUTH_SOCK) .tox/py38/bin/trond --debug --working-dir=dev -l logging.conf --host=0.0.0.0

example_cluster:
	tox -e example-cluster

yelpy:
	.tox/py38/bin/pip install -r yelp_package/extra_requirements_yelp.txt


# 1. Bump version at the top of this file
# 2. `make release`

VERSION = $(firstword $(subst -, ,$(RELEASE) ))
LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B | sed -e 's/\x27/"/g')
release:
	@if [[ "$$(git status --porcelain --untracked-files=no :^/Makefile)" != '' ]]; then echo "Error: Working directory is not clean; only changes to Makefile are allowed when cutting a release."; exit 1; fi
	$(eval untracked_files_tmpfile=$(shell mktemp))
	git status --porcelain --untracked-files=all :^./Makefile > $(untracked_files_tmpfile)
	@if [[ "$$(git status --porcelain --untracked-files=normal :/docs/source/generated)" != '' ]]; then echo "Error: Untracked files found in docs/source/generated."; exit 1; fi
	@if existing_sha=$$(git rev-parse --verify --quiet v$(VERSION)); then echo "Error: tag v$(VERSION) exists and points at $$existing_sha"; exit 1; fi
	@read upstream_master junk <<<"$$(git ls-remote -h origin master)" && if ! git merge-base --is-ancestor $$upstream_master HEAD; then echo "Error: HEAD is missing commits from origin/master ($$upstream_master)."; exit 1; fi
	dch -v $(RELEASE) --distribution jammy --changelog ./debian/changelog $$'$(VERSION) tagged with \'make release\'\rCommit: $(LAST_COMMIT_MSG)'
	sed -i -e "s/__version__ = .*/__version__ = \"$(VERSION)\"/" ./tron/__init__.py
	make docs || true
	git add ./Makefile ./debian/changelog ./tron/__init__.py ./docs/source/generated/
	git commit -m "Released $(RELEASE) via make release"
	if [[ "$$(git status --porcelain --untracked-files=all)" != "$$(<$(untracked_files_tmpfile))" ]]; then echo "Error: automatic git commit left some files uncommitted. Fix the git commit command in ./Makefile to include any automatically generated files that it is currently missing."; exit 1; fi
	git tag v$(VERSION)
	git push --atomic origin master v$(VERSION)

docs:
	tox -r -e docs

man:
	which $(SPHINXBUILD) >/dev/null && $(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(DOCS_DIR) $(DOCS_DIR)/source/man || true
	@echo
	@echo "Build finished. The manual pages are in $(DOCS_BUILDDIR)/source/man."

clean:
	rm -rf tronweb/js/cs
	find . -name '*.pyc' -delete
