package gratum.source

import gratum.etl.Pipeline

class ConcatSource extends AbstractSource {

    List<Source> sources = []

    public static ConcatSource concat(Source... src) {
        ConcatSource concat = new ConcatSource()
        if( src.length > 0 ) {
            concat.addAll( Arrays.asList( src ) )
        }
        return concat
    }

    public ConcatSource add( Source source ) {
        sources << source
        return this
    }

    public ConcatSource addAll( Collection<Source> srcs ) {
        sources.addAll( srcs )
        return this
    }

    @Override
    void doStart(Pipeline pipeline) {
        sources.each { Source src ->
            src.start( pipeline )
        }
    }
}
