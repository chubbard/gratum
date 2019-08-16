package gratum.operators

import gratum.etl.Pipeline

class IntersectOperator implements Operator<Map,Map> {

    def columns
    Pipeline<Map> other

    public static Operator<Map,Map> intersect( Pipeline<Map> pipeline, def columns ) {
        return new IntersectOperator( pipeline, columns )
    }

    public IntersectOperator(Pipeline<Map> other, def columns) {
        this.columns = columns
        this.other = other
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        Map <String,List<Map>> cache = [:]

        other.addStep("intersect(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }.start()

        source.addStep("intersect(${source.name}, ${columns})") { Map row ->
            String key = keyOf( row, leftColumn(columns) )
            row.included = cache.containsKey(key)
            return row
        }

        return source
    }

    private List<String> leftColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).keySet().asList()
        } else {
            return [columns.toString()]
        }
    }

    private List<String> rightColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).values().asList()
        } else {
            return [columns.toString()]
        }
    }

    private String keyOf( Map row, List<String> columns ) {
        return columns.collect { key -> row[key] }.join(":")
    }

}
