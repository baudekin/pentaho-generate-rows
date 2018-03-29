# pentaho-generate-rows
To smoke test GenerateRowStreamer.scala enter something like this:

${SPARK_HOME}/bin/spark-submit --class "com.github.baudekin.generate_row.GenerateRowStreamer" --master local[2] ${LOCAL_REPO_HOME}/pentaho-generate-rows/target/pentaho-generate-rows-1.0-SNAPSHOT.jar

To smoke test the wrapper class GenerateRwoWrapper.java submit somthing like this: 

${SPARK_HOME}/bin/spark-submit --class "com.github.baudekin.generate_row.GenerateRowWrapper" --master local[2] ${LOCAL_REPO_HOME}/pentaho-generate-rows/target/pentaho-generate-rows-1.0-SNAPSHOT.jar
