#!/bin/bash

MAIN_CLASS="com.nopqzip.App"

if [ -n $1 ]; then
  [ $1 = "watcher" ] && MAIN_CLASS="com.nopqzip.WatcherApp"
  [ $1 = "watchee" ] && MAIN_CLASS="com.nopqzip.WatcheeApp"
  [ $1 = "idiot" ] && MAIN_CLASS="com.nopqzip.IdiotApp"
fi

echo "Running $MAIN_CLASS"
mvn exec:java -Dexec.mainClass=$MAIN_CLASS -Dexec.args=$2

