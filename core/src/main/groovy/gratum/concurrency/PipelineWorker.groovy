package gratum.concurrency

import gratum.etl.LoadStatistic

import java.util.concurrent.CompletableFuture

class PipelineWorker implements Runnable {

    CompletableFuture<LoadStatistic> stat = new CompletableFuture<>()
    Closure<LoadStatistic> logic
    Thread thread
    String name

    PipelineWorker(String name, Closure<LoadStatistic> logic) {
        this.name = name
        this.logic = logic
    }

    @Override
    void run() {
        try {
            stat.complete(logic.call())
        } catch( Throwable t ) {
            stat.completeExceptionally(t)
        }
    }

    public void start() {
        thread = new Thread(this, name)
        thread.start()
    }
}
