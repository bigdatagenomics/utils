#!/bin/sh

# what is the current revision?
REVISION=git rev-parse HEAD

# do scala 2.10 release
mvn -Dresume=false release:clean release:prepare release:perform

# do scala 2.11 release
git checkout $REVISION
./scripts/move_to_scala_2.11.sh
git commit -a -m "Modifying pom.xml files for 2.11 release."
mvn -Dresume=false release:clean release:prepare release:perform
