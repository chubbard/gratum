package gratum.etl

class HaltPipelineException extends RuntimeException {
    HaltPipelineException(GString message) {
        super(message)
    }
}
