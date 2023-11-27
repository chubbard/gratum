package gratum.csv;

public class HaltPipelineException extends RuntimeException {
    public HaltPipelineException(String message) {
        super(message);
    }
}
