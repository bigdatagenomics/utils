#!/bin/bash

set +x

find . -name "pom.xml" -exec sed -e "s/2.10.6/2.11.8/g" -e "s/2.10/2.11/g" -i .2.11.bak '{}' \;
# keep maven-javadoc-plugin at version 2.10.4
find . -name "pom.xml" -exec sed -e "s/2.11.4/2.10.4/g" -i.2.11.2.bak '{}' \;
find . -name "*.2.11.*bak" -exec rm {} \;
