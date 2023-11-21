RELEASE := 'v23.09'
RELEASESEM := 'v1.6.0'

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
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all test lint format build tag-version