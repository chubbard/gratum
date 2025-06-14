package gratum.etl

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

class SortConfig {
    int pageSize = 0
    Closure after = null
    boolean downstream = true
    Comparator<Map<String,Object>> comparator

    void after(@ClosureParams(value = SimpleType, options = "java.io.File") Closure callback) {
        after = callback
    }
}
