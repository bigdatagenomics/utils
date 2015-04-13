#!/bin/sh

mvn release:rollback

mvn release:rollback -P scala-2.11