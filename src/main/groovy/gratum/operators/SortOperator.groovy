package gratum.operators

import gratum.etl.Pipeline
import gratum.source.ChainedSource

class SortOperator implements Operator<Map,Map> {

    String[] columns

    public static Operator<Map,Map> sort( String... columns ) {
        return new SortOperator( columns )
    }

    public SortOperator(String[] columns) {
        this.columns = columns
    }

    @Override
    public Pipeline<Map> attach(Pipeline<Map> source) {
        // todo sort externally

        Comparator<Map> comparator = new Comparator<Map>() {
            @Override
            int compare(Map o1, Map o2) {
                for( String key : columns ) {
                    int value = o1[key] <=> o2[key]
                    if( value != 0 ) return value;
                }
                return 0
            }
        }

        List<Map> ordered = []
        source.addStep("sort(${columns})") { Map row ->
            ordered << row
            return row
        }

        Pipeline next = new Pipeline(source.statistic.name)
        next.src = new ChainedSource( source )
        source.after {
            next.statistic.rejectionsByCategory = source.statistic.rejectionsByCategory
            next.statistic.start = source.statistic.start
            ordered.sort( comparator )
            ((ChainedSource)next.src).process( ordered )
            null
        }

        return next
    }
}
