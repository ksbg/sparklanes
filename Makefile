# Parse arguments
ifeq (submit,$(firstword $(MAKECMDGOALS)))
  PIPELINE := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  $(eval $(PIPELINE):;@:)
else
  ifeq (build submit,$(wordlist 1, 2, $(MAKECMDGOALS)))
    PIPELINE := $(wordlist 3,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
    $(eval $(PIPELINE):;@:)
  endif
endif

.PHONY: build submit clean help

help:
	@echo "clean - remove all build artifacts"
	@echo "build - package the application"
	@echo "submit {Pipeline YAML file path} - submit a pipeline file"

clean:
	@rm -rf ./dist

build: clean
	@mkdir -p ./dist
	@mkdir -p ./dist/libs

	@echo "Installing requirements"
	@pip install -r requirements.txt -t ./dist/libs

	@echo "Packaging application"
	@cd ./dist/libs && zip -r ../../dist/libs.zip .
	@cp ./pyspark_etl/main.py ./dist
	@cp -R ./spark-config ./dist
	@cd ./pyspark_etl && zip -x main.py -r ../dist/pkg.zip .
	@cd ./dist && rm -rf libs

submit:
	@if [ ! -f ./dist/pkg.zip ] || [ ! -f ./dist/libs.zip ]; then \
		echo "Please run \"make build\" first to package the application"; exit 1;\
	else \
 		cd dist; spark-submit \
					--properties-file ./spark-config/spark.conf \
					--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=./spark-config/log4j-spark.properties"  \
					--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=./spark-config/log4j-spark.properties" \
					--py-files libs.zip,pkg.zip main.py \
					\
					--pipeline $(realpath $(lastword $(PIPELINE))); \
	fi
