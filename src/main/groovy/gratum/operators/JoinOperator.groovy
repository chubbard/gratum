package gratum.operators

import gratum.etl.Pipeline
import gratum.etl.RejectionCategory

import static gratum.etl.Utils.*

class JoinOperator implements Operator<Map<String,Object>,Map<String,Object>> {

    private Pipeline<Map> other
    private def columns
    private boolean left

    public JoinOperator(Pipeline<Map> other, def columns, boolean left) {
        this.other = other
        this.columns = columns
        this.left = left
    }

    @Override
    public Pipeline<Map<String,Object>> attach(Pipeline<Map<String,Object>> source) {
        Map<String,List<Map<String,Object>>> cache =[:]

        other.addStep("join(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }

        return source >> Operators.inject("join(${source.name}, ${columns})") { Map row ->
            if( !other.complete ) {
                other.go()
            }
            String key = keyOf( row, leftColumn(columns) )

            if( left ) {
                if( cache.containsKey(key) ) {
                    return cache[key].collect { Map k ->
                        Map j = (Map)k.clone()
                        j.putAll(row)
                        return j
                    }
                } else {
                    // make sure we add columns even if they are null so sources write out columns we expect.
                    if( !cache.isEmpty() ) {
                        String c = cache.keySet().first()
                        cache[c].first().each { String i, Object v ->
                            if( !row.containsKey(i) ) row[i] = null
                        }
                    }
                    return [row]
                }
            } else if( cache.containsKey(key) ) {
                return cache[key].collect { Map k ->
                    Map j = (Map)k.clone()
                    j.putAll(row)
                    return j
                }
            } else {
                source.reject("Could not join on ${columns}", RejectionCategory.IGNORE_ROW )
                return null
            }
        }
    }
}
