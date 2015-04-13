#!/bin/sh

mvn -Dresume=false release:clean release:prepare release:perform

mvn -Dresume=false release:clean release:prepare release:perform -P scala-2.11
