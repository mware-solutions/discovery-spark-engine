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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

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

  public static long writeCsv(Dataset<Row> df, String strUri, Configuration conf, int limitRows)
          throws IOException, URISyntaxException {
    df.write().format("csv")
            .mode(SaveMode.Overwrite)
            .option("header", true)
            .save(strUri);

    FileSystem fs = FileSystem.get(new URI(strUri), conf);

    String dstUri = strUri + "_snapshot";
    FileUtil.copyMerge(fs, new Path(strUri), fs, new Path(dstUri), true, conf, null);
    fs.rename(new Path(dstUri), new Path(strUri));

    long totalRows = df.count();
    return totalRows;
  }
}
