package gratum.util;

public class PipelineAbortException extends RuntimeException {

    public PipelineAbortException(String message) {
        super(message);
    }

    public PipelineAbortException(String message, Throwable cause) {
        super(message, cause);
    }
}
