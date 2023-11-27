package gratum.etl

import groovy.transform.CompileStatic

import java.util.regex.Pattern

@CompileStatic
class Condition {

    String name
    List<Closure<Boolean>> comparators

    Condition(Map<String,Object> filterColumns) {
        name = nameOf( filterColumns )
        comparators = new ArrayList<>(filterColumns.size())

        for( String col : filterColumns.keySet() ) {
            Object comp = filterColumns[col]
            if( col == "*" ) {
                if( comp instanceof Closure ) {
                    comparators.add( comp )
                }
            } else if( comp instanceof Collection ) {
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

    boolean matches(Map row ) {
        for( Closure c : comparators ) {
            if( !c(row) ) return false
        }
        return true
    }

    private static String nameOf(Map columns) {
        return columns.keySet().collect() { key -> "${key} -> ${columns[key] instanceof Closure ? "{}" : columns[key]}" }.join(',')
    }

    private Closure<Boolean> createInCollectionCallback(String col, Collection<?> comp) {
        return { Map row -> comp.contains(row[col]) }
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

    public String toString() {
        return name
    }
}
