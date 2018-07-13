package gratum.etl

/**
 * Created by charliehubbard on 7/11/18.
 */
class Rejection {

    RejectionCategory category
    String reason
    String step

    Rejection(String reason, RejectionCategory category = RejectionCategory.REJECTION, String step = null) {
        this.category = category
        this.reason = reason
        this.step = step;
    }
}
