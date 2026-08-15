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
 *
 * Each row will include a reference the the ResultSetMetaData under the default column name
 * _metadata.
 */
@CompileStatic
class JdbcSource extends AbstractSource {

    Sql db
    GString query
    String plainQuery
    Map<String,Object> params

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

    JdbcSource query(String query) {
        this.plainQuery = query
        return this
    }

    JdbcSource query(String query, Map<String,Object> params) {
        this.plainQuery = query
        this.params = params
        return this
    }

    @Override
    void doStart(Pipeline pipeline) {
        List<String> columns = []
        ResultSetMetaData metadata
        int line = 1
        if( query ) {
            db.eachRow( query, { md ->
                for( int i = 1; i <= md.columnCount; i++ ) {
                    columns << md.getColumnName(i)
                }
                metadata = md
            } ) {row ->
                Map<String,Object> result = [:]
                columns.eachWithIndex { String col, int index ->
                    result[col] = row[index]
                }
                result["_metadata"] = metadata
                pipeline.process( result, line++ )
            }
        } else if( plainQuery && params ) {
            db.eachRow(plainQuery, params ?: [:], { md ->
                for( int i = 1; i <= md.columnCount; i++ ) {
                    columns << md.getColumnName(i)
                }
                metadata = md
            }) { row ->
                Map<String,Object> result = [:]
                columns.eachWithIndex { String col, int index ->
                    result[col] = row[index]
                }
                result["_metadata"] = metadata
                pipeline.process( result, line++ )
            }
        }
    }
}
