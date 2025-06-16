package gratum.etl

import groovy.transform.CompileStatic

/**
 * Created by charliehubbard on 7/11/18.
 */
@CompileStatic
class Rejection {

    RejectionCategory category
    String reason
    CharSequence step
    Throwable throwable

    Rejection(String reason, RejectionCategory category = RejectionCategory.REJECTION, CharSequence step = null, Throwable t = null) {
        this.category = category
        this.reason = reason
        this.step = step
        this.throwable = t
    }
}
