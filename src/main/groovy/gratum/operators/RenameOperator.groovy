package gratum.operators

import gratum.etl.Pipeline

class RenameOperator implements Operator<Map,Map> {

    Map<String,String> fieldNames

    public static Operator<Map,Map> rename( Map<String,String> fieldNames ) {
        return new RenameOperator( fieldNames );
    }

    RenameOperator(Map<String, String> fieldNames) {
        this.fieldNames = fieldNames
    }

    @Override
    Pipeline<Map> attach(Pipeline<Map> source) {
        source.addStep("renameFields(${fieldNames}") { Map row ->
            for( String src : fieldNames.keySet() ) {
                String dest = fieldNames.get( src )
                row[dest] = row.remove( src )
            }
            return row
        }
        return source
    }
}
