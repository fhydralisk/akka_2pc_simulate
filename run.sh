#!/bin/bash

MAIN_CLASS="cn.edu.tsinghua.ee.fi.odl.sim.apps.LeaderApp"

if [ -n $1 ]; then
  [ $1 = "frontend" ] && MAIN_CLASS="cn.edu.tsinghua.ee.fi.odl.sim.apps.FrontendApp"
  [ $1 = "backend" ] && MAIN_CLASS="cn.edu.tsinghua.ee.fi.odl.sim.apps.BackendApp"
  [ $1 = "leader" ] && MAIN_CLASS="cn.edu.tsinghua.ee.fi.odl.sim.apps.LeaderApp"
  [ $1 = "operator" ] && MAIN_CLASS="cn.edu.tsinghua.ee.fi.odl.sim.apps.OperatorApp"
fi

echo "Running $MAIN_CLASS"
mvn exec:java -Dexec.mainClass=$MAIN_CLASS -Dexec.args=$2

