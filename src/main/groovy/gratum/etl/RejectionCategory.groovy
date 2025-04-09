package gratum.etl

import groovy.transform.CompileStatic

/**
 * Created by charliehubbard on 7/11/18.
 */
@CompileStatic
enum RejectionCategory {
    INVALID_FORMAT,
    MISSING_DATA,
    DUPLICATE,
    REJECTION,
    SCRIPT_ERROR,
    RUNTIME_ERROR,
    IGNORE_ROW
}