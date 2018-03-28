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

import java.util.ArrayList;
import java.util.stream.IntStream;


public class GenerateRowWrapper {

  private com.github.baudekin.generate_row.GenerateRowStreamer grs;

  public GenerateRowWrapper() {
  }

  private scala.collection.immutable.List<String> toScalaList(String[] primativeStringArr) {
    ArrayList<String> arrList = new ArrayList<String>( primativeStringArr.length );
    for ( String name: primativeStringArr ) arrList.add( name );
    return JavaConverters.asScalaBufferConverter( arrList ).asScala().toList();
  }

  public JavaRDD<Row> createLimitedData( String stepId, String[] nameArr, String[] typeArr, String[] valueArr, int limit) {
    List<String> names = toScalaList( nameArr );
    List<String> types = toScalaList( typeArr );
    List<String> values = toScalaList( valueArr );

    this.grs = new com.github.baudekin.generate_row.GenerateRowStreamer( stepId, names, types, values );

    // Create Large set of data
    // TODO: Paralellize it ???
    IntStream.range(1, limit).forEach(
      index -> {
        grs.addRow();
      }
    );

    Dataset<Row> dsRows = grs.getRddStream();
    grs.processAllPendingAdditions();
    grs.stopStream();

    return dsRows.javaRDD();
  }

  public JavaRDD<Row> setupContinousStreaming( String stepId, String[] nameArr, String[] typeArr, String[] valueArr, String nowName, String prevName) {
    String[] nameArrPlus = new String[nameArr.length + 2];
    nameArrPlus[nameArr.length + 1] = nowName;
    nameArrPlus[nameArr.length + 2] = prevName;
    String[] typeArrPlus = new String[nameArr.length + 2];
    typeArrPlus[typeArrPlus.length + 1] = "Long";
    typeArrPlus[typeArrPlus.length + 2] = "Long";
    String[] valueArrPlus = new String[nameArr.length + 2];
    valueArrPlus[valueArrPlus.length + 1] = "0";
    valueArrPlus[valueArrPlus.length + 2] = "0";

    List<String> names = toScalaList( nameArrPlus );
    List<String> types = toScalaList( typeArrPlus );
    List<String> values = toScalaList( valueArrPlus );

    this.grs = new GenerateRowStreamer( stepId, names, types, values );

    Dataset<Row> dsRows = grs.getRddStream();

    return dsRows.javaRDD();
  }

  public void startContinousStreaming() {

  }

  public void stopContinousStreaming() {

  }
}
