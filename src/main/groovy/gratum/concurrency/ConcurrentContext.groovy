package gratum.concurrency

import gratum.etl.Pipeline

interface ConcurrentContext {
    LocalConcurrentContext spread(Closure<Pipeline> workerClosure );
    LocalConcurrentContext collect(Closure<Pipeline> resultsClosure );
    Closure<Pipeline> connect();
}