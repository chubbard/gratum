package gratum.sink

import groovy.sql.Sql
import java.sql.Connection
import java.sql.PreparedStatement

class JdbcBatchHelper implements Closeable, AutoCloseable {

    String tableName
    List<String> columns
    int batchSize

    String sql
    Sql db
    Connection connection
    PreparedStatement statement
    int batchCount = 0

    JdbcBatchHelper(Sql db, String tableName, List<String> columns, int batchSize = 1000) {
        this.db = db
        this.tableName = tableName
        this.columns = columns
        this.batchSize = batchSize
        initStatement()
    }

    private void initStatement() {
        this.sql = generateSql()
        this.connection = db.createConnection()
        this.connection.setAutoCommit(false)
        statement = this.connection.prepareStatement( sql )
    }

    private String generateSql() {
        return "insert into ${tableName} (${columns.join(',')}) values (${ columns.collect {String x -> '?'}.join(",")})"
    }

    Map<String,Object> addBatch(Map<String,Object> params ) {
        for( int i = 0; i < columns.size(); i++ ) {
            String col = columns[i]
            statement.setObject( i+1, params[col] )
        }
        statement.addBatch()
        batchCount++
        if( batchCount  == batchSize ) {
            statement.executeBatch()
            batchCount = 0
        }
        return params
    }

    boolean hasQueuedBatches() {
        return batchCount > 0
    }

    int[] executeBatch() {
        return statement.executeBatch()
    }

    void close() {
        db.closeResources( connection, statement )
    }
}
