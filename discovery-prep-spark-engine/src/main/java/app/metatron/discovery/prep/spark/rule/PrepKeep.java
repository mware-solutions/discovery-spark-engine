package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Keep;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepKeep extends PrepRule {

  public static Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Keep keep = (Keep) rule;
    Expression row = keep.getRow();

    SparkUtil.createView(df, "temp");

    // FIXME: replace() will be deleted after ensuring UI doesn't use (and == will be unparsable)
    String sql = "SELECT * FROM temp WHERE " + row.toString().replace("==", "=");
    return SparkUtil.getSession().sql(sql);
  }
}
