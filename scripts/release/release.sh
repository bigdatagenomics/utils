#!/bin/sh

# do we have enough arguments?
if [[ $# < 3 ]]; then
    echo "Usage:"
    echo
    echo "./release.sh <release version> <development version> <milestone id>"
    exit 1
fi

# pick arguments
release=$1
devel=$2
milestone=$3

# get current branch
branch=$(git status -bs | awk '{ print $2 }' | awk -F'.' '{ print $1 }' | head -n 1)

# update changelog per Github milestone
mvn com.github.heuermh.maven.plugin.changes:github-changes-maven-plugin:1.2:github-changes -DmilestoneId=${milestone}
git commit -a -m "Modifying changelog."

commit=$(git log --pretty=format:"%H" | head -n 1)
echo "releasing from ${commit} on branch ${branch}"

git push origin ${branch}

# do spark 3, scala 2.12 release
git checkout -b maint_spark3_2.12-${release} ${branch}
git commit -a -m "Modifying pom.xml files for Spark 3, Scala 2.12 release."
mvn --batch-mode \
  -Dresume=false \
  -Dtag=utils-parent-spark3_2.12-${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=utils-spark3_2.12-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Spark 3, Scala 2.12 version failed."
  exit 1
fi

if [ $branch = "master" ]; then
  # if original branch was master, update versions on original branch
  git checkout ${branch}
  mvn versions:set -DnewVersion=${devel} \
    -DgenerateBackupPoms=false
  git commit -a -m "Modifying pom.xml files for new development after ${release} release."
  git push origin ${branch}
fi
