#!/bin/bash

set +x

find . -name "pom.xml" -exec sed -e "s/2.10.6/2.11.8/g" -e "s/2.10/2.11/g" -i .2.11.bak '{}' \;
find . -name "*.2.11.bak" -exec rm {} \;
