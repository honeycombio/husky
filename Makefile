.PHONY: test
test:
ifeq (, $(shell which gotestsum))
	@echo " ***"
	@echo "Runing with standard go test because gotestsum was not found on PATH. Consider installing gotestsum for friendlier test output!"
	@echo " ***"
	go test ./...
else
	gotestsum --junitfile unit-tests.xml --format testname
endif
