.PHONY: test deps

GLIDEPATH := $(shell command -v glide 2> /dev/null)

test: vendor
	go test

deps: vendor

vendor: glide.lock
ifndef GLIDEPATH
	$(info Please install glide.)
	$(info Install it using your package manager or)
	$(info by running: curl https://glide.sh/get | sh.)
	$(info )
	$(error glide is required to continue)
endif
	echo "Installing vendor directory"
	glide install -v

glide.lock: glide.yaml
	echo "Glide.yaml has changed, updating glide.lock"
	glide update -v	

