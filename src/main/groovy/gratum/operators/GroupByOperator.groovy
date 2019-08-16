package gratum.operators

import gratum.etl.Pipeline
import gratum.source.Source

class GroupByOperator implements Operator<Map,Map> {

    private String[] columns

    /**
     * Returns a Pipeline where the row is grouped by the given columns.  The resulting Pipeline will only
     * return a single row where the keys of that row will be the first column passed to the groupBy() method.
     * All other columns given will occur under the respective keys.  This yields a tree like structure where
     * the height of the tree is equal to the columns.length.  In the leaves of the tree are the rows that
     * matched all of their parents.
     *
     * @param columns The columns to group each row by.
     * @return A Operator that yields a single row that represents the tree grouped by the given columns.
     */
    public static Operator groupBy( String... columns ) {
        return new GroupByOperator( columns )
    }

    GroupByOperator(String... columns) {
        this.columns = columns
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        Map cache = [:]
        source.addStep("groupBy(${columns.join(',')})") { Map row ->
            Map current = cache
            columns.eachWithIndex { String col, int i ->
                if( !current.containsKey(row[col]) ) {
                    if( i + 1 < columns.size() ) {
                        current[row[col]] = [:]
                        current = (Map)current[row[col]]
                    } else {
                        current[row[col]] = []
                    }
                } else if( i + 1 < columns.size() ) {
                    current = (Map)current[row[col]]
                }
            }

            current[ row[columns.last()] ] << row
            return row
        }

        Pipeline parent = source
        Pipeline other = new Pipeline( source.name )
        other.src = new Source() {
            @Override
            void start(Closure closure) {
                parent.start() // first start our parent pipeline
                closure( cache )
            }
        }
        other.copyStatistics( source )
        return other
    }
}
