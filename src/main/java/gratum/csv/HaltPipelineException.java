package gratum.csv;

public class HaltPipelineException extends RuntimeException {
    HaltPipelineException(String message) {
        super(message);
    }
}
