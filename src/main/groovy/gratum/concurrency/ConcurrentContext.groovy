package gratum.concurrency

import gratum.etl.Pipeline

interface ConcurrentContext {
    LocalConcurrentContext worker( Closure<Pipeline> workerClosure );
    LocalConcurrentContext results( Closure<Pipeline> resultsClosure );
    Closure<Pipeline> connect();
}