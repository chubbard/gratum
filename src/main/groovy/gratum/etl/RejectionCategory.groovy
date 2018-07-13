package gratum.etl

/**
 * Created by charliehubbard on 7/11/18.
 */
enum RejectionCategory {
    INVALID_FORMAT,
    MISSING_DATA,
    DUPLICATE,
    REJECTION,
    SCRIPT_ERROR,
    IGNORE_ROW
}