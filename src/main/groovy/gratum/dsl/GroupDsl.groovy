package gratum.dsl

import gratum.etl.LoadStatistic
import gratum.etl.Pipeline

import java.util.stream.Collectors

class GroupDsl {

    RunStrategy runStrategy = RunStrategy.SINGLE
    List<Pipeline> pipelines = []
    Map<String,LoadStatistic> results

    GroupDsl() {
    }

    void setRunStrategy(RunStrategy runStrategy) {
        this.runStrategy = runStrategy
    }

    void pipeline(@DelegatesTo(value = PipelineDsl) Closure closure) {
        PipelineDsl dsl = new PipelineDsl()
        closure.resolveStrategy = Closure.DELEGATE_FIRST
        closure.delegate = dsl
        closure()
        pipelines << dsl.pipeline
    }

    void execute() {
        switch( runStrategy ) {
            case RunStrategy.SINGLE:
                results = pipelines.stream().map { pipeline ->
                    pipeline.go()
                }.collect(Collectors.toMap({LoadStatistic stat -> stat.name}, {LoadStatistic stat -> stat}))
                break
            case RunStrategy.PARALLEL:
                results = pipelines.parallelStream().map { pipeline ->
                    pipeline.go()
                }.collect(Collectors.toMap({ LoadStatistic stat -> stat.name }, { LoadStatistic stat -> stat }))
                break
        }
    }

    Map<String, LoadStatistic> getResults() {
        return results
    }
}

