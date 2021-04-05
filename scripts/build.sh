#!/bin/bash
set -e

export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xss2M" 
sbt assembly
docker build . -t samtecspg/mason-spark:$(cat VERSION)
