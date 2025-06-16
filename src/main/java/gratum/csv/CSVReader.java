package gratum.csv;

/**
 * Created by charliehubbard on 7/11/18.
 */
import java.util.List;

public interface CSVReader {

    public void processHeaders( List<String> header ) throws Exception;

    /* Return true to stop parsing csv */
    public boolean processRow( List<String> header, List<String> row ) throws Exception;

    public default void afterProcessing() {};
}
