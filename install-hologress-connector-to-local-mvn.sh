#!/usr/bin/env bash

mvn install:install-file -Dfile=hologres-flink-connector-1.10-jar-with-dependencies.jar -DgroupId=org.apache.flink -DartifactId=hologres-flink-connector -Dversion=1.10 -Dpackaging=jar -DgeneratePom=true
