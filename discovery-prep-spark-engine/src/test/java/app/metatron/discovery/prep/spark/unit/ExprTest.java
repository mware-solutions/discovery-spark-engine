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

package app.metatron.discovery.prep.spark.unit;

import static org.junit.Assert.assertEquals;

import app.metatron.discovery.prep.spark.PrepTransformer;
import app.metatron.discovery.prep.spark.rest.TestUtil;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExprTest {

  @BeforeClass
  public static void setup() {
//    SparkUtil.setAppName("DiscoverySparkEngine");
//    SparkUtil.setMasterUri("local");
//    SparkUtil.setWarehouseDir("hdfs://localhost:9000/user/hive/warehouse");
//    SparkUtil.setMetastoreUris("thrift://localhost:9083");
  }

  @Test
  public void testDerive() throws IOException, AnalysisException {
    Dataset<Row> df = TestUtil.readAsCsvFile(new String[][]{
            {"a", "b?", "c"},
            {"3", "1", "2"},
            {"1", "1", "3"},
            {"3", "1", "4"},
            {"NULL", "NULL", "NULL"}
    });

    PrepTransformer transformer = new PrepTransformer();
    df = transformer.applyRule(df, "header rownum: 1", null);
    df = transformer.applyRule(df, "settype col: a, b_ type: long", null);
    df = transformer.applyRule(df, "set col: a, b, c value: if($col == 'NULL', null, $col)", null);
    df = transformer.applyRule(df, "derive value: `c` as: 'c_1'", null);
    df.show();

    assertEquals(df.columns()[3], "c_1");

    List<Row> rows = df.collectAsList();
    TestUtil.assertRow(rows.get(0), new Object[]{3L, 1L, "2", "2"});
    TestUtil.assertRow(rows.get(1), new Object[]{1L, 1L, "3", "3"});
    TestUtil.assertRow(rows.get(2), new Object[]{3L, 1L, "4", "4"});
    TestUtil.assertRow(rows.get(3), new Object[]{null, null, null, null});
  }
}
