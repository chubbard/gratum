package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic

/**
 * A source that uses a Collection&lt;Map&gt; as its source for rows.  For example,
 *
 * <pre>
 * from([
 *  [id: 1, name: 'Bill Rhodes', age: 53, gender: 'male'],
 *  [id: 2, name: 'Cheryl Lipscome', age: 43, gender: 'female'],
 *  [id: 3, name: 'Diana Rogers', age: 34, gender: 'female'],
 *  [id: 4, name: 'Jack Lowland', age: 25, gender: 'male'],
 *  [id: 5, name: 'Ginger Rogers', age: 83, gender: 'female']
 * ])
 * .filter({ Map row -&gt; row.age > 40 }
 * .go
 * </pre>
 */
@CompileStatic
class CollectionSource extends AbstractSource {

    Iterable<Map> source

    CollectionSource(Iterable<Map> source) {
        super( "Collection" )
        this.source = source
    }

    @Override
    void doStart(Pipeline pipeline) {
        int line = 1;
        for( Map r : source ) {
            pipeline.process( r, line++ )
        }
    }

    public static CollectionSource of( Map... src ) {
        return new CollectionSource( src.toList() )
    }

    public static CollectionSource of( Iterable<Map> src ) {
        return new CollectionSource( src )
    }

    public static Pipeline from( Map... src) {
        Pipeline pipeline = new Pipeline("Array(${src.size()})")
        pipeline.src = new CollectionSource( src.toList() )
        return pipeline
    }

    public static Pipeline from(Iterable<Map> src ) {
        return new Pipeline("Collection(${src.size()})").source( new CollectionSource( src ) )
    }
}
