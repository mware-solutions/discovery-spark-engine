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

import app.metatron.discovery.prep.spark.udf.ArrayToJsonEx;
import app.metatron.discovery.prep.spark.udf.CountPatternEx;
import app.metatron.discovery.prep.spark.udf.RegexpExtractEx;
import app.metatron.discovery.prep.spark.udf.SplitEx;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SparkUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(SparkUtil.class);

  private static SparkSession session;

  private static String appName;
  private static String masterUri;
  private static String metastoreUris;
  private static String warehouseDir;

  public static SparkSession createSession(Map<String, Object> datasetInfo) {
    LOGGER.info("creating session:");
    LOGGER.info("appName={} masterUri={} spark.sql.warehouse.dir={}", appName, masterUri, warehouseDir);
    LOGGER.info("spark.sql.catalogImplementation={} hive.metastore.uris={}", "hive", metastoreUris);
    LOGGER.info("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation={}", "true");

    SparkConf conf = new SparkConf();
    conf.set("spark.executor.memory", "1g");
    conf.set("spark.driver.memory", "1g");

    String storedUri = (String) datasetInfo.get("storedUri");
    if (storedUri.startsWith("s3a://")) {
      conf.set("spark.hadoop.fs.s3a.endpoint", (String) datasetInfo.get("fs.s3a.endpoint"));
      conf.set("spark.hadoop.fs.s3a.access.key", (String) datasetInfo.get("fs.s3a.access.key"));
      conf.set("spark.hadoop.fs.s3a.secret.key", (String) datasetInfo.get("fs.s3a.secret.key"));
      conf.set("spark.hadoop.fs.s3a.fast.upload", "true");
      conf.set("spark.hadoop.fs.s3a.path.style.access", "true");
      conf.set("spark.hadoop.fs.s3a.path.style.access", "true");
      conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    }

    Builder builder = SparkSession.builder()
            .config(conf)
            .appName(appName)
            .master(masterUri);

    if (metastoreUris != null && warehouseDir != null && warehouseDir.startsWith("hdfs")) {
      builder = builder
              .config("hive.metastore.uris", metastoreUris)
              .config("spark.sql.warehouse.dir", warehouseDir)
              .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
              .enableHiveSupport();
    } else {
      builder = builder
              .config("spark.sql.catalogImplementation", "in-memory")
              .config("spark.driver.maxResultSize", "2g");
    }

    session = builder.getOrCreate();

    session.udf().register("split_ex", split_ex, DataTypes.StringType);
    session.udf().register("regexp_extract_ex", regexp_extract_ex, DataTypes.StringType);
    session.udf().register("count_pattern_ex", count_pattern_ex, DataTypes.IntegerType);
    session.udf().register("array_to_json_ex", array_to_json_ex, DataTypes.StringType);
    return session;
  }

  public static SparkSession getSession() {
    if (session == null)
      throw new IllegalStateException("Session not created yet. Please call createSession() first");

    return session;
  }

  private static SplitEx split_ex = new SplitEx();
  private static RegexpExtractEx regexp_extract_ex = new RegexpExtractEx();
  private static CountPatternEx count_pattern_ex = new CountPatternEx();
  private static ArrayToJsonEx array_to_json_ex = new ArrayToJsonEx();

  public static void stopSession() {
    if (session != null) {
//      session.stop();
      session = null;
    }
  }

  public static void createTempView(Dataset<Row> df, String tempViewName) {
    df.createOrReplaceTempView(tempViewName);
  }

  public static void prepareCreateTable(Dataset<Row> df, String dbName, String tblName) throws AnalysisException {
    createTempView(df, "temp");
    getSession().sql(String.format("DROP TABLE %s PURGE", dbName + "." + tblName));
  }

  public static void createTable(Dataset<Row> df, String dbName, String tblName, int limitRows) {
    try {
      prepareCreateTable(df, dbName, tblName);
    } catch (AnalysisException e) {
      // Suppress "table not found"
    }
    String fullName = dbName + "." + tblName;
    getSession().sql(String.format("CREATE TABLE %s AS SELECT * FROM %s LIMIT %d", fullName, "temp", limitRows));
  }

  public static Dataset<Row> selectTableAll(String dbName, String tblName, int limitRows) {
    String fullName = dbName + "." + tblName;
    return getSession().sql(String.format("SELECT * FROM %s LIMIT %d", fullName, limitRows));
  }

  public static void setAppName(String appName) {
    SparkUtil.appName = appName;
  }

  public static void setMasterUri(String masterUri) {
    SparkUtil.masterUri = masterUri;
  }

  public static void setMetastoreUris(String metastoreUris) {
    SparkUtil.metastoreUris = metastoreUris;
  }

  public static String getMetastoreUris() {
    return metastoreUris;
  }

  public static void setWarehouseDir(String warehouseDir) {
    SparkUtil.warehouseDir = warehouseDir;
  }

  public static String getWarehouseDir() {
    return warehouseDir;
  }
}
