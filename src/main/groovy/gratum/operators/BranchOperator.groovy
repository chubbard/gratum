package gratum.operators

import gratum.etl.Pipeline

class BranchOperator<T> implements Operator<T,T> {

    String name
    Map<String,Object> condition
    Closure<Void> split

    public static <T> Operator<T,T> branch( String name, Closure<Void> split ) {
        return new BranchOperator<T>(name, null, split)
    }

    public static Operator<Map,Map> branch( String name, Map<String,Object> condition, Closure<Void> split) {
        return new BranchOperator<Map>( null, condition, split )
    }

    BranchOperator(String name, Map<String,Object> condition, Closure<Void> split) {
        this.name = name
        this.condition = condition
        this.split = split
    }

    @Override
    Pipeline<T> attach(Pipeline<T> source) {
        Pipeline<T> branch = new Pipeline<T>( name )

        split( branch )

        if( condition ) source >> FilterFieldsOperator.filerFields( condition )

        source.addStep( "branch()" ) { T row ->
            branch.process( row )
            return row
        }

        return source
    }
}
