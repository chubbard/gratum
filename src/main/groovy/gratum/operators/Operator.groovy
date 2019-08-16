package gratum.operators

import gratum.etl.Pipeline

interface Operator<S,T> {
    Pipeline<T> attach(Pipeline<S> source )
}