package gratum.operators

import gratum.etl.Pipeline
import gratum.source.Source

class GroupByOperator implements Operator<Map,Map> {

    private String[] columns

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
