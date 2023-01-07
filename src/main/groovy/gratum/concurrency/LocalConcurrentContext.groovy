package gratum.concurrency

import gratum.etl.LoadStatistic
import gratum.etl.Pipeline
import gratum.etl.Rejection
import gratum.etl.RejectionCategory
import gratum.source.ChainedSource
import gratum.source.ClosureSource
import groovy.transform.CompileStatic

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@CompileStatic
public class LocalConcurrentContext implements ConcurrentContext {

    final int workerSize
    Closure<Pipeline> workerClosure
    Closure<Pipeline> resultProcessorClosure

    final ArrayBlockingQueue<Map<String,Object>> eventQueue
    final ArrayBlockingQueue<Map<String,Object>> resultQueue
    final CountDownLatch latch

    List<PipelineWorker> workers = []
    PipelineWorker resultProcessor

    public LocalConcurrentContext(int workers = 8, int queueSize = 200) {
        workerSize = workers
        eventQueue = new ArrayBlockingQueue<>(queueSize)
        resultQueue = new ArrayBlockingQueue<>(queueSize)
        latch = new CountDownLatch(workers+1)
    }

    public LocalConcurrentContext spread(@DelegatesTo(LocalConcurrentContext) Closure<Pipeline> workerClosure ) {
        this.workerClosure = workerClosure
        this.workerClosure.delegate = this
        return this
    }

    public LocalConcurrentContext collect(@DelegatesTo(LocalConcurrentContext) Closure<Pipeline> resultsClosure ) {
        this.resultProcessorClosure = resultsClosure
        this.resultProcessorClosure.delegate = this
        return this
    }

    public Closure<Pipeline> connect() {
        return { Pipeline pipeline ->
            createWorkers()
            createResultProcessor()
            pipeline.addStep("Queue to Workers") { Map<String,Object> row ->
                eventQueue.put( row )
                return row
            }
            .after {
                eventQueue.put([_done_:true] as Map<String,Object>)
                latch.await()
            }

            Pipeline next = new Pipeline( pipeline.name, pipeline ) {
                @Override
                LoadStatistic toLoadStatistic(long start, long end) {
                    LoadStatistic stat = super.toLoadStatistic(start, end)
                    LoadStatistic workerStats = workers.inject(new LoadStatistic()) {acc, worker ->
                        acc.merge( worker.stat.get() )
                        return acc
                    }
                    workerStats.stepTimings = workerStats.stepTimings.collectEntries { step, time ->
                        return [ step, (time / workerStats.stepTimings.size()).toLong() ]
                    } as Map<String,Long>

                    stat.merge( workerStats )
                    stat.merge( resultProcessor.stat.get() )
                    stat.loaded = resultProcessor.stat.get().loaded
                    return stat
                }
            }
            next.src = new ChainedSource( pipeline )
            return next
        } as Closure<Pipeline>
    }

    private void createWorkers() {
        for( int i = 0; i < workerSize; i++ ) {
            workers << new PipelineWorker("Worker-${i+1}", {
                try {
                    Pipeline pipeline = ClosureSource.of({ Pipeline pipeline ->
                        boolean done = false
                        while (!done && !Thread.interrupted()) {
                            Map<String, Object> row = eventQueue.poll()
                            if (row?._done_) {
                                eventQueue.put(row)
                                done = true
                            } else if (row) {
                                pipeline.process(row)
                            }
                        }
                    }).name("Worker").into()
                            .onRejection { Pipeline rej ->
                                rej.addStep { Map<String, Object> row ->
                                    // so when we play this down the rejections pipeline it'll expect a REJECT_KEY to be there so we recreate it
                                    // because at this point the REJECTED_KEY property has been removed so we re-add it.  Not great.
                                    Rejection reject = new Rejection(row.rejectionReason as String, row.rejectionCategory as RejectionCategory, row.rejectionStep as String)
                                    row[Pipeline.REJECTED_KEY] = reject
                                    resultQueue.put(row)
                                    return row
                                }
                                return
                            }

                    LoadStatistic stat = ((Pipeline) workerClosure(pipeline))
                            .addStep("Queue to Results") { Map row ->
                                resultQueue.put(row)
                                return row
                            }
                            .go()
                    return stat
                } finally {
                    latch.countDown()
                }
            })
            workers.last().start()
        }
    }

    void createResultProcessor() {
        resultProcessor = new PipelineWorker( "Results Processor", {
            try {
                Pipeline pipeline = ClosureSource.of({ Pipeline pipeline ->
                    boolean done = false
                    while (!done && !Thread.interrupted()) {
                        Map<String, Object> row = resultQueue.poll(10, TimeUnit.SECONDS)
                        if (row) {
                            if (row[Pipeline.REJECTED_KEY]) {
                                pipeline.reject(row, -1)
                            } else {
                                pipeline.process(row)
                            }
                        } else if (latch.count == 1) {
                            done = true
                        }
                    }
                }).name("Result Processor").into()
                LoadStatistic stats = resultProcessorClosure.call(pipeline)
                        .go()
                return stats
            } finally {
                latch.countDown()
            }
        })
        resultProcessor.start()
    }
}
