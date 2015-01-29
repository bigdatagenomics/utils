#!/bin/sh

mvn -Dresume=false release:clean release:prepare release:perform

mvn -Dresume=false -P scala-2.11 release:clean release:prepare release:perform
