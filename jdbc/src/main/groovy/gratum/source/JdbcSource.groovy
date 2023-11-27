package gratum.source

import gratum.etl.Pipeline
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic

import java.sql.ResultSetMetaData

/**
 * A source that uses a database query for the source of the rows it feeds through the pipeline.
 * This source can actually be re-used for different database queries.  For example:
 *
 * <pre>
 *     database( sql )
 *      .query("select * from People where age >= ${age}")
 *      .into()
 *      .go()
 * </pre>
 */
@CompileStatic
class JdbcSource extends AbstractSource {

    Sql db
    GString query

    JdbcSource(Sql db) {
        super("jdbc")
        this.db = db
    }

    JdbcSource(String url, String username, String password) {
        super(url)
        db = Sql.newInstance(url, username, password)
    }

    static JdbcSource database( Sql sql ) {
        return new JdbcSource(sql)
    }

    static JdbcSource database(String url, String username, String password) {
        return new JdbcSource(url, username, password)
    }

    JdbcSource query( GString query ) {
        this.query = query
        return this
    }

    @Override
    void doStart(Pipeline pipeline) {
        List<String> columns = []
        int line = 1
        db.eachRow( query, { ResultSetMetaData md ->
            for( int i = 1; i <= md.columnCount; i++ ) {
                columns << md.getColumnName(i)
            }
        } ) { GroovyResultSet row ->
            Map result = [:]
            columns.eachWithIndex { String col, int index ->
                result[col] = row[index]
            }
            pipeline.process( result, line )
        }
    }
}
