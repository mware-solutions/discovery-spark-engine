/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.metatron.discovery.prep.spark.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(CsvUtil.class);

  // public for tests
  public static OutputStreamWriter getWriter(OutputStream os) {
    OutputStreamWriter writer = null;
    String charset = "UTF-8";

    try {
      writer = new OutputStreamWriter(os, charset);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    return writer;
  }

  /**
   * @param strUri URI as String (to be java.net.URI)
   * @param conf Hadoop configuration which is mandatory when the url's protocol is hdfs
   *
   * header will be false for table-type snapshots.
   */
  public static CSVPrinter getPrinter(String strUri, Configuration conf) throws IOException, URISyntaxException {
    Writer writer;
    URI uri;

    LOGGER.debug("getPrinter(): strUri={} conf={}", strUri, conf);

    try {
      uri = new URI(strUri);
    } catch (URISyntaxException e) {
      LOGGER.error("getPrinter(): URISyntaxException: strUri={}", strUri);
      throw e;
    }

    switch (uri.getScheme()) {
      case "alluxio":
      case "hdfs":
        if (conf == null) {
          LOGGER.error("getPrinter(): Required property missing: check polaris.dataprep.hadoopConfDir: strUri={}",
                  strUri);
          throw new IOException("getPrinter(): Required property missing: check polaris.dataprep.hadoopConfDir");
        }
        Path path = new Path(uri);

        FileSystem hdfsFs;

        try {
          hdfsFs = FileSystem.get(conf);
        } catch (IOException e) {
          LOGGER.error("getPrinter(): Cannot get file system: check polaris.dataprep.hadoopConfDir: strUri={}", strUri);
          throw e;
        }

        FSDataOutputStream hos;
        try {
          hos = hdfsFs.create(path);
        } catch (IOException e) {
          LOGGER.error("getPrinter(): Cannot create a file: polaris.dataprep.hadoopConfDir: strUri={}", strUri);
          throw e;
        }

        writer = getWriter(hos);
        break;

      case "file":
        File file = new File(uri);
        File dirParent = file.getParentFile();
        assert dirParent != null : uri;

        if (!dirParent.exists()) {
          if (!dirParent.mkdirs()) {
            String errmsg = "getPrinter(): Cannot create a directory: " + strUri;
            LOGGER.error(errmsg);
            throw new IOException(errmsg);
          }
        }

        FileOutputStream fos;
        try {
          fos = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
          LOGGER.error("getPrinter(): FileNotFoundException: Check the permission of snapshot directory: strUri={}",
                  strUri);
          throw e;
        }

        writer = getWriter(fos);
        break;

      default:
        String errmsg = "getPrinter(): Unsupported URI scheme: " + strUri;
        LOGGER.error(errmsg);
        throw new IOException(errmsg);
    }

    CSVPrinter printer;
    try {
      printer = new CSVPrinter(writer, CSVFormat.RFC4180.withQuoteMode(QuoteMode.MINIMAL));
    } catch (IOException e) {
      LOGGER.error("getPrinter(): Failed to get CSV printer: strUri={}", strUri);
      throw e;
    }

    return printer;
  }

  public static long writeCsv(Dataset<Row> df, String strUri, Configuration conf, int limitRows)
          throws IOException, URISyntaxException {
    CSVPrinter printer = getPrinter(strUri, conf);
    long totalLines = 0L;

    String[] colNames = df.columns();

    // Column names
    for (String colName : colNames) {
      printer.print(colName);
    }
    printer.println();

    // Column values
    Iterator iter = df.toLocalIterator();
    while (iter.hasNext()) {
      Row row = (Row) iter.next();
      for (int i = 0; i < row.size(); i++) {
        printer.print(row.get(i));
      }
      printer.println();
      if (++totalLines >= limitRows) {
        break;
      }
    }

    printer.close(true);
    return totalLines;
  }
}
