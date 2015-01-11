#!/bin/sh

mvn release:rollback

mvn -P scala-2.11 release:rollback
