# pentaho-generate-rows
To smoke test GenerateRowStreamer.scala enter something like this:

${SPARK_HOME}/bin/spark-submit --class "com.github.baudekin.generate_row.GenerateRowStreamer" --master local[2] ${LOCAL_REPO_HOME}/pentaho-generate-rows/target/pentaho-generate-rows-1.0-SNAPSHOT.jar

To smoke test the wrapper class GenerateRwoWrapper.java submit somthing like this: 

${SPARK_HOME}/bin/spark-submit --class "com.github.baudekin.generate_row.GenerateRowWrapper" --master local[2] ${LOCAL_REPO_HOME}/pentaho-generate-rows/target/pentaho-generate-rows-1.0-SNAPSHOT.jar

To run for an hour on yarn the wrapper class GenerateRwoWrapper.java submit somthing like this: 
../ael/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
          --class "com.github.baudekin.generate_row.GenerateRowWrapper" 
          --master yarn \
          --deploy-mode cluster \
          --conf spark.yarn.maxAppAttempts=2 \
    	    --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
    	    --conf spark.yarn.max.executor.failures=16 \
          --conf spark.yarn.executor.failuresValidityInterval=1h \
          --conf spark.task.maxFailures=8 \
          --conf yarn.log-aggregation-enable=true \
          --queue realtime_queue  \
          --jars ./pentaho-generate-rows-1.0-SNAPSHOT.jar 
          3600 
