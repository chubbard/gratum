package gratum.operators

import gratum.etl.Pipeline
import gratum.source.ChainedSource

class ExchangeOperator<Src,Dest> implements Operator<Src,Dest> {

    String name
    Closure<Pipeline<Dest>> closure

    public ExchangeOperator(String name, Closure<Pipeline<Dest>> closure) {
        this.name = name
        this.closure = closure
    }

    @Override
    public Pipeline attach(Pipeline source) {
        Pipeline<Dest> next = new Pipeline<Dest>( name )
        next.src = new ChainedSource<Dest>(source)
        source.addStep("exchange()") { Src row ->
            Pipeline<Dest> pipeline = closure( row )
            pipeline.start { Dest current ->
                next.process( current )
                return current
            }
            return row
        }
        next.copyStatistics( source )
        return next
    }
}
