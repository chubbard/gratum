package gratum.etl

import groovy.transform.CompileStatic

import static java.util.concurrent.TimeUnit.DAYS
import static java.util.concurrent.TimeUnit.HOURS
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.MINUTES
import static java.util.concurrent.TimeUnit.SECONDS

/**
 * Created by charliehubbard on 7/11/18.
 */
@CompileStatic
public class Duration {
    private long duration;

    public Duration(long duration) {
        this.duration = duration;
    }

    public Duration add( Duration d ) {
        return new Duration( duration + d.duration );
    }

    public Duration add( long duration ) {
        return new Duration( this.duration + duration );
    }

    public Duration sub( Duration d ) {
        return new Duration( duration - d.duration );
    }

    public Duration sub( long duration ) {
        return new Duration( this.duration - duration );
    }

    public Duration addTo( Duration d ) {
        this.duration = this.duration + d.duration;
        return this;
    }

    public Duration addTo( long d ) {
        this.duration = this.duration + d;
        return this;
    }

    public Duration subFrom( Duration d ) {
        this.duration = this.duration - d.duration;
        return this;
    }

    public Duration subFrom( long d ) {
        this.duration = this.duration - d;
        return this;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        long days = getDays();
        long hours = getHours();
        long minutes = getMinutes();
        long seconds = getSeconds();
        long millis = getMillis();

        if( days > 0 ) {
            builder.append( String.format( "%dd %dh %dm", days, hours, minutes ) );
        } else if( hours > 0 ) {
            builder.append( String.format( "%dh %dm %ds", hours, minutes, seconds ) );
        } else if( minutes > 0 ) {
            builder.append( String.format( "%dm %ds", minutes, seconds ) );
        } else {
            builder.append( String.format( "%ds %dms", seconds, millis ) );
        }
        return builder.toString();
    }

    private long getDays() {
        return DAYS.convert( duration, MILLISECONDS );
    }

    private long getMillis() {
        return MILLISECONDS.convert(duration % MILLISECONDS.convert(1, SECONDS), MILLISECONDS);
    }

    private long getSeconds() {
        return SECONDS.convert(duration % MILLISECONDS.convert(1, MINUTES), MILLISECONDS);
    }

    private long getMinutes() {
        return MINUTES.convert(duration % MILLISECONDS.convert(1, HOURS), MILLISECONDS);
    }

    public long getHours() {
        return HOURS.convert( duration % MILLISECONDS.convert(1,DAYS), MILLISECONDS);
    }
}
