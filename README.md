# simple-spark
Smoke test for spark stand alone instance

SETUP SPARK
===========

Copy the oracle database driver to /opt/spark/lib/

cp ~/.ivy2/cache/com.oracle/ojdbc6/jars/ojdbc6-11.2.0.3.0-1.jar /opt/spark/lib/.

add to the /usr/lib/spark/conf/spark-env.sh:

export SPARK_CLASSPATH=/opt/spark/lib/ojdbc6-11.2.0.3.0-1.jar

COMPILE AND RUN THE APP
=======================

package the jar file for the remote instance of spark

$> activator package

run the app

$> activator run
