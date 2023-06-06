RELEASE := 'v23.06'
RELEASESEM := 'v1.4.1'

all: format lint

test:
	sbt test

format:
	sbt scalafmt

lint:
	sbt compile
	sbt scalastyle

build:
	sbt package

tag-version:
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all test lint format build tag-version