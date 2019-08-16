package gratum.etl

class Utils {

    public static List<String> leftColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).keySet().asList()
        } else {
            return [columns.toString()]
        }
    }

    public static List<String> rightColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).values().asList()
        } else {
            return [columns.toString()]
        }
    }

    public static String keyOf( Map row, List<String> columns ) {
        return columns.collect { key -> row[key] }.join(":")
    }

}
