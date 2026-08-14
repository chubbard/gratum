package gratum.sink

import gratum.etl.Pipeline
import groovy.sql.Sql
import groovy.transform.CompileStatic

/**
 * Inserts a Pipeline's output into a JDBC table using JDBC batching for performance.  Keep in mind
 * that you need to handle the data types for the columns in your pipeline.  JdbcSink will not
 * convert column types for you.
 *
 * Usage is:
 * <pre>
 *     .save( JdbcSink.batch( sql, "My_Table", ["column_1", "column_2", "column_3"], 1000) )
 * </pre>
 */
@CompileStatic
class JdbcSink implements Sink {

    JdbcBatchHelper batchHelper
    int loaded = 0

    static JdbcSink batch(Sql sql, String tableName, List<String> columns, int batchSize = 1000) {
        return new JdbcSink( batchHelper: new JdbcBatchHelper(sql, tableName, columns, batchSize))
    }

    @Override
    String getName() {
        return "JDBC(${batchHelper.getTableName()}"
    }

    @Override
    void attach(Pipeline pipeline) {
        pipeline.addStep("jdbcSink(${batchHelper.getTableName()})") { row ->
            batchHelper.addBatch( row )
            loaded++
            row
        }
        .after {
            batchHelper.executeBatch()
            return
        }
    }

    @Override
    Map<String, Object> getResult() {
        return [
                tableName: batchHelper.getTableName(),
                loaded: loaded
        ] as Map<String,Object>
    }

    @Override
    void close() throws IOException {
        batchHelper.close()
    }
}
