package gratum.etl

import groovy.transform.CompileStatic

import java.util.regex.Pattern

@CompileStatic
class Condition {

    List<Closure<Boolean>> comparators

    Condition(Map<String,Object> filterColumns) {
        comparators = new ArrayList<>(filterColumns.size())

        for( String col : filterColumns.keySet() ) {
            Object comp = filterColumns[col]
            if( comp instanceof Collection ) {
                comparators.add(createInCollectionCallback(col, comp))
            } else if( comp instanceof Pattern ) {
                comparators.add( createPatternCallback( col, comp) )
            } else if( comp instanceof Closure ) {
                comparators.add( createClosureCallback( col, comp ) )
            } else {
                comparators.add( createEqualsCallback( col, comp ) )
            }
        }
    }

    private Closure<Boolean> createInCollectionCallback(String col, Collection<?> comp) {
        return { Map row -> comp.contains(row[col]) }
    }

    boolean matches(Map row ) {
        for( Closure c : comparators ) {
            if( !c(row) ) return false
        }
        return true
    }

    Closure<Boolean> createClosureCallback(String col, Closure closure) {
        return { Map row -> (Boolean)closure(row[col]) }
    }

    Closure<Boolean> createEqualsCallback(String col, Object comp) {
        return { Map row -> row[col] == comp }
    }

    Closure<Boolean> createPatternCallback(String col, Pattern pattern) {
        return { Map row -> (row[col] =~ pattern).find() }
    }
}
