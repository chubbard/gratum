package gratum.source

import gratum.etl.Pipeline

/**
 * Created by charliehubbard on 7/11/18.
 */
interface Source {

    void start(Pipeline pipeline );

    Pipeline into()
}
