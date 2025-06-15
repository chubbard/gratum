package gratum.etl

import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

@CompileStatic
class SortConfig {
    int pageSize = 0
    Closure after = null
    boolean downstream = true
    Comparator<Map<String,Object>> comparator

    void after(@ClosureParams(value = SimpleType, options = "java.io.File") Closure callback) {
        after = callback
    }

    void orderBy(String... columns) {
        comparator = new Comparator<Map<String, Object>>() {
            @Override
            int compare(Map<String, Object> o1, Map<String, Object> o2) {
                for( String key : columns ) {
                    int value = (Comparable)o1[key] <=> (Comparable)o2[key]
                    if( value != 0 ) return value
                }
                return 0
            }
        }
    }

    void orderBy(Tuple2<String,SortOrder>... ordering) {
        comparator = new Comparator<Map<String, Object>>() {
            @Override
            int compare(Map<String, Object> o1, Map<String, Object> o2) {
                for( Tuple2<String,SortOrder> key : ordering ) {
                    int value = (Comparable)o1[key.first] <=> (Comparable)o2[key.first]
                    switch( key.second ){
                        case SortOrder.ASC:
                            break
                        case SortOrder.DESC:
                            value = Math.negateExact(value)
                            break
                    }
                    if( value != 0 ) return value
                }
                return 0
            }
        }
    }
}
