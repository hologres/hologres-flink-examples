#!/usr/bin/env bash

mvn install:install-file -Dfile=library/hologres-flink-connector-1.10-jar-with-dependencies.jar -DgroupId=io.hologres -DartifactId=hologress-flink-connector -Dversion=1.10 -Dpackaging=jar -DgeneratePom=true
