/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2002 - 2018, Hitachi Vantara
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */
package com.github.baudekin.generate_row;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.tools.nsc.backend.icode.analysis.TypeFlowAnalysis;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import java.time.LocalDateTime;

 /** GenerateRowWrapper structure provides an interface to the scala class {@link GenerateRowStreamer#GenerateRowStreamer(String, List, List, List)}
 * This class provides a Java Interface for the GenerateRowStreamer scala class. GenerateRowWrapper provides
 * ability to generate a continuous stream of rows for the rdd returned by
 * {@link #setupContinuousStreaming(String[], String[], String[], String, String, long)}. In addition
 * it provides for limit number of rows to be returned by {@link #createLimitedData(String[], String[], String[], int)}
 *
 * @author Michael A Bodkin  emailTo:michael.bodkin@hitachivantara.com>
 * @version 8.1.0.0.0-snapshot
 */
public class GenerateRowWrapper {

  /**
   * The scala object that implements the memory map for generating a stream.
   */
  private com.github.baudekin.generate_row.GenerateRowStreamer grs;

  /**
   * The stop and start streaming flag for generating continuous stream of RDD rows.
   * Uses Atomic objects to provide thread safeness. @see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/atomic/AtomicBoolean.html
   */
  private AtomicBoolean streamRows = new AtomicBoolean( false );

  /**
   * Unique Identifier for providing the ability to spin up multiple streams at one. It is assumed the user will pass in the PDI UI Step Name
   * Uses Atomic object to provide thread safeness. @see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/atomic/AtomicReference.html
   */
  private AtomicReference<String> stepId = new AtomicReference<>( "null" );

  /**  constructor to set the unique stepId for this instance. It remains the same for the life of the object.
   *
   * @param stepName = Unique string that is assumed to be the PDI UI Step name
   */
  public GenerateRowWrapper( String stepName ) {
    //TODO spaces are the only character we need to convert to make the stepName  view name compliant.
    this.stepId.set( stepName.replace( " ", "_" ) );
  }

   /** Helper method for converting primitive String array to scala list. Scala immutable lists are main data transfer
    * device for the scala GenerateRowsStreamer. {@link GenerateRowStreamer#GenerateRowStreamer(String, List, List, List)} and
    * {@link GenerateRowStreamer#addRow(List)}
    *
    * @param primitiveStringArr - Array of strings
    * @return - scala List of strings with the same value and order as the input array.
    */
  private scala.collection.immutable.List<String> toScalaList( String[] primitiveStringArr ) {
    ArrayList<String> arrList = new ArrayList<>( primitiveStringArr.length );
    for ( String name: primitiveStringArr ) {
      arrList.add( name );
    }
    return JavaConverters.asScalaBufferConverter( arrList ).asScala().toList();
  }

   /** Generates a RDD with all the rows specified by the limit parameter. Each row will be the same and
    * defined by the input lists.
    *
    * @param nameArr - Name of the row's columns
    * @param typeArr - Type data for the spark column
    * @param valueArr - Value of for each row
    * @param limit - Number of rows to limit the generation to.
    * @return - JavaRDD of type row with the generated values. A spark action such as collect has not been
    * performed on this data set.
    */
  public Dataset<Row> createLimitedData( String[] nameArr, String[] typeArr, String[] valueArr, int limit) {
    // TODO: Verify the PDI UI is supposed to verify the arrays are equal and have values.
    List<String> names = toScalaList( nameArr );
    // TODO Determine if we can use the PDI UI types directly. If not convert them to the spark types.
    List<String> types = toScalaList( typeArr );
    List<String> values = toScalaList( valueArr );

    this.grs = new com.github.baudekin.generate_row.GenerateRowStreamer( stepId.get(), names, types, values );

    // Create Large set of data
    // TODO: Paralellise it ??? The data is the same and the order does not matter
    IntStream.range( 1, limit + 1 ).forEach( index -> grs.addRow() );

    Dataset<Row> dsRows = grs.getRddStream();

    // Process all the rows we just added
    // TODO on large scale should we run processAllPendingAdditons ever so many rows???
    grs.processAllPendingAdditions();

    // Free up streaming resources the RDD will remain around
    grs.stopStream();

    return dsRows;
  }

   /** Thread safe copy of the time delay in milli seconds between row generates when continuous streaming is desired.
    * -1 means this value has not be set. It is only set in the {@link #setupContinuousStreaming(String[], String[], String[], String, String, long)}
    * and only used int {@link #startContinuousStreaming()}
    */
  private AtomicLong delay = new AtomicLong( -1 );

  /** Contains the last row that was generated used to create the current row {@link #startContinuousStreaming()} It is
   * only updated in {@link #startContinuousStreaming}
   *
   */
  private AtomicReference<ArrayList<String>> lastRow = new AtomicReference<>( null );

   /** This method primes the GenerationRowStream for generating continuous data.
    *
    * @param nameArr - Array of Columns Names for the RDD
    * @param typeArr - Array of Type to use for the RDD
    * @param valueArr - Array of values to use for the RDD row
    * @param curTimeFieldName - Name of the column that contains the current time field. This is updated for every row.
    * @param prevTimeFieldName - Name of the column that contains the last current time value. Equals "null" on row zero
    *                          This is update for every row.
    * @param delay - The time period in milli seconds(ms) to wait between row generations.
    * @return - JavaRDD<Row>  that will updated every delay ms with new row values.
    */
  public Dataset<Row> setupContinuousStreaming( String[] nameArr,
                                                String[] typeArr,
                                                String[] valueArr,
                                                String curTimeFieldName,
                                                String prevTimeFieldName,
                                                long delay ) {
    this.delay.set( delay );
    java.util.List<String> nameList =  new ArrayList<>( nameArr.length + 2 );
    // TODO this assumes PDI UI does not put these fields into the arrays already. Verify how PDI UI does this.
    // TODO After the above is answered determine if the array to list conversions below should be do with helper method
    nameList.add( curTimeFieldName );
    nameList.add( prevTimeFieldName );
    for ( String name : nameArr ) {
      nameList.add( name );
    }

    java.util.List<String> typeList =  new ArrayList<>( typeArr.length + 2 );
    typeList.add( "String" );
    typeList.add( "String" );
    for ( String typeValue: typeArr ) {
      typeList.add( typeValue );
    }

    ArrayList<String> valueList = new ArrayList<>( valueArr.length + 2 );
    valueList.add( "null" );
    valueList.add( "null" );
    for ( String value: valueArr ) {
     valueList.add( value );
    }

    this.lastRow.set( valueList );

    // Convert the arrays to scala immutable lists
    List<String> names = JavaConverters.asScalaBufferConverter( nameList ).asScala().toList();
    List<String> types = JavaConverters.asScalaBufferConverter( typeList ).asScala().toList();
    List<String> values = JavaConverters.asScalaBufferConverter( valueList ).asScala().toList();

    this.grs = new GenerateRowStreamer( stepId.get(), names, types, values );

    Dataset<Row> dsRows = grs.getRddStream();
    return dsRows;
  }

   // TODO: Verify this is true in AEL and if it is remove the thread.
   /** Data generation Thread  the thread that creates all the rows
    *
    */
  private AtomicReference<Thread> genDataThread = new AtomicReference<>( null );

   /** Continuously generates rows until {@link #stopContinousStreaming()} is invoked. Note this method is restartable. This
    * supports the pause feature of PDI UI. The calling application can use the {@link #waitOnDataGenerationToStop()} to wait
    * for the streaming to stop.
    */
   //TODO Verify pause means something in AEL
  public void startContinuousStreaming() {
    streamRows.set( true );
    // Create a thread to generate data. This might not be needed because Pentaho steps run on separate threads.
    // TODO: Verify this is true in AEL and if it is remove the thread.
    Thread t = new Thread( () -> {
      // Generate data while streamRows is true
      while ( streamRows.get() ) {

        // Adjust the value list to reflect the new current and previous times get a copy
        // of the current list.
        ArrayList<String> valueList = (ArrayList<String>)lastRow.get().clone();
        // Get the last current time and make the previous time
        valueList.set( valueList.size() - 2, valueList.get( valueList.size() - 1 ) );
        // Set the current time to current local timestamp.
        valueList.set( valueList.size() - 1,
          LocalDateTime.now().format( DateTimeFormatter.ofPattern( "yyyy/MM/dd HH:mm:ss.S" ) ) );
        this.lastRow.set( valueList );

        // Place the new values on stream and emit the new row
        List<String> values = JavaConverters.asScalaBufferConverter( valueList ).asScala().toList();
        grs.addRow( values );
        grs.processAllPendingAdditions();
        try {
          Thread.sleep( this.delay.get() );
        } catch ( java.lang.InterruptedException ex ) {
          //TODO Verify we don't need TimeoutException|java.util.concurrent.TimeoutException ex) {
          // ignore
        }
      } // End of Loop
    });
    // Give the Thread meaningful name
    t.setName( stepId.get() + ":GenerateRow" );
    // Save it so we can have the calling process wait on it.
    this.genDataThread.set( t );
    t.start();
  }

   /** Wait for the generate data thread to exit. {@link #startContinuousStreaming()}
    *
    */
  public void waitOnDataGenerationToStop() {
    try {
      this.genDataThread.get().wait();
    } catch ( InterruptedException | NullPointerException ex ) {
      // ignore
    }
  }

   /** Cause the generation of row data to stop. {@link #startContinuousStreaming()}
    *
    */
  public void stopContinousStreaming() {
    // Causes thread to exit correctly
    this.streamRows.set( false );
  }

   /** main for running integration test of GenerateRowWrapper as a spark application.
    *
    * @param args
    */
  public static void main(String[] args) {
    // Smoke test limit
    GenerateRowWrapper grw1 = new GenerateRowWrapper( "Step One" );
    GenerateRowWrapper grw2 = new GenerateRowWrapper( "Step Two" );
    GenerateRowWrapper grw3 = new GenerateRowWrapper( "Step Three" );

    String[] names = { "ColumnOne", "ColumnTwo", "ColumnThree" };
    String[] types = { "String", "Int", "Double" };
    String[] values = { "Value", "200", "303.33" };
    Dataset<Row> rdd1 = grw1.createLimitedData( names, types, values, 100);
    rdd1.javaRDD().collect().forEach( System.out::println );
    System.out.println( "Count=" + rdd1.count() );

    // Smoke test runing two streams a the same time.
    Dataset<Row> rdd2 = grw2.setupContinuousStreaming( names,
      types, values, "Now", "TwoSecondsAgo", 2000 );
    Dataset<Row> rdd3 = grw3.setupContinuousStreaming( names,
      types, values, "Now", "FourSecondsAgo", 4000 );
    // Start two streams
    grw2.startContinuousStreaming();
    grw3.startContinuousStreaming();

    // Let catpture the outputs of  streams for one minute
    for ( int i=0; i<60; i++ ) {
      // "clone" the rdd
      // TODO should I create a helper select that only returns one row?
      // TODO should I create a call back function that works of the timer instead of polling?
      Dataset<Row> rdd2c = rdd2.select("Now", "TwoSecondsAgo");
      Dataset<Row> rdd3c = rdd3.select("Now", "FourSecondsAgo");
      System.out.println("rdd2c count:" + rdd2c.count());
      System.out.println("rdd3c count:" + rdd3c.count());
      try {
        Thread.sleep(1000);
      } catch (java.lang.InterruptedException ex ) {
        // ignore
      }
    }
    grw2.stopContinousStreaming();
    grw3.stopContinousStreaming();

  }
}
