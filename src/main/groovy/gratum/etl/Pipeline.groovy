package gratum.etl

import gratum.csv.CSVFile
import gratum.source.ChainedSource
import gratum.source.Source

import java.text.ParseException
import java.text.SimpleDateFormat

class Step {
    public String name
    public Closure step

    Step(String name, Closure step) {
        this.name = name
        this.step = step
    }
}


/**
 * A Pipeline represents a series of steps that will be performed on 1 or more rows.  Rows are Map objects
 * that are passed from the Source through each step individually.  A row can be modified, transformed into 
 * something else, or rejected by a step.  If a row is rejected in any step all successive steps are not 
 * processed and the row is passed to the rejections Pipeline (if one exists).  Any rows that pass through 
 * the entire Pipeline are said to have been loaded.  Any row that does not pass through all steps is said 
 * to be rejected.
 * 
 * Rows orginate from the Pipeline's {@link gratum.source.Source}.  The {@link gratum.source.Source} sends 
 * rows into the Pipeline until it finishes.  The Pipeline keeps a set of statistics about how many rows
 * were loaded, rejected, the types of rejections, timing, etc.  This is kept in the {@link gratum.etl.LoadStatistic}
 * instance and returned from the {@link Pipeline#go(Closure)} method.
 *
 * Example:
 *
 * <pre>
 *      LoadStatistic stats = http("http://api.open-notify.org/astros.json").inject { Map json ->
 *          json.people
 *      }.printRow()
 *      .go()
 * </pre>
 *
 * In the above example you can see it using an {@link gratum.source.HttpSource} to fetch JSON data.  
 * That data is returned as a Map object which has other nested objects within it.  In this case it's
 * pulling out the "people" column which is a Collection of people objects.  Then it injects those members
 * into the down stream steps which uses printRow to print it to the console.  The output would look like
 * the following:
 * 
 * <pre>
 
 * </pre>
 */
public class Pipeline implements Source {

    LoadStatistic statistic
    Source src
    List<Step> processChain = []
    List<Closure> doneChain = []
    Pipeline rejections
    boolean complete = false

    Pipeline(String name) {
        this.statistic = new LoadStatistic([name: name])
    }

    /**
     * Creates a pipeline where startClosure is the source.  The startClosure is passed another closure that it can use
     * to pass an indiviual row to the Pipeline.
     *
     * @param name name of the pipeline to create
     * @param startClosure A closure that is called with a closure.  The startClosure can call the closure argument it's 
     * passed to send a row into the pipeline.
     * @return The pipeline attached to results of the startClosure.
     */
    public static Pipeline create( String name, Closure startClosure ) {
        Pipeline pipeline = new Pipeline(name)
        pipeline.src = new Source() {
            @Override
            void start(Closure pipelineClosure) {
                startClosure( pipelineClosure )
            }
        }
        return pipeline
    }

    /**
     * The name of the pipeline.  This is used by {@link LoadStatistic} to identify what Pipeline it came from.
     *
     * @return The name of the Pipeline
     */
    public getName() {
        return this.statistic.name
    }

    /**
     * Adds a step to the pipeline.  It's passed an optional name to identify the step by, and a closure that represents
     * the individual step.  It returns the Map to be processed by the next step in the pipeline, typically it simply returns the same
     * row it was passed.  If it returns null or {@link Rejection} then it will reject this row, stop processing additional
     * steps, and pass the current row to the rejections pipline.
     *
     * @param name The step name
     * @param step The code used to process each row processed by the Pipeline.
     * @return this Pipeline.
     */
    public Pipeline addStep( String name = null, Closure<Map> step ) {
        step.delegate = this
        processChain << new Step( name, step )
        return this
    }

    /**
     * Adds a closure to the end of the Pipeline.  This is called after all rows are processed.  This closure is
     * invoked without any arguments.
     *
     * @param step the Closure that is invoked after all rows have been processed.
     * @return this Pipeline.
     */
    public Pipeline after( Closure<Void> step ) {
        doneChain << step
        return this
    }

    /**
     * Takes a closure that is passed the rejection Pipeline.  The closure can register steps on the rejection
     * pipeline.
     *
     * @param branch Closure that's passed the rejection the pipeline
     * @return this Pipeline
     */
    public Pipeline onRejection( Closure<Void> branch ) {
        if( !rejections ) rejections = new Pipeline("Rejections(${name})")
        branch( rejections )
        after {
            rejections.doneChain.each { Closure c ->
                c()
            }
            return
        }
        return this
    }

    /**
     * Concatentates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
     * rows from this pipeline and the src pipeline.
     *
     * @param src The pipeline
     * @return Returns a new pipeline that combines all of the rows from this pipeline and the src pipeline.
     */
    public Pipeline concat( Pipeline src ) {
        Pipeline original = this
        this.after {
            int line = 0
            src.addStep("concat(${src.name})") { Map row ->
                line++
                original.process( row, line )
                return row
            }.start()
        }
        return this
    }

    /**
     * This adds a step to the Pipeline that passes all rows where the given closure returns true to the next step
     * on the pipeline.  All rows where the closure returns false are rejected.
     *
     * @param callback A callback that is passed a row, and returns a boolean.  All rows that return a false are rejected.
     * @return A Pipeline that contains only the rows that matched the filter.
     */
    public Pipeline filter(Closure callback) {
        addStep( "filter()" ) { Map row ->
            return callback(row) ? row : reject("Row did not match the filter closure.", RejectionCategory.IGNORE_ROW )
        }
        return this
    }

    /**
     * This adds a step tot he Pipeline that passes all rows where the values of the columns on the given Map are equal
     * to the columns in the row.  This is a boolean AND between columns.  For example:
     *
     * .filter( [ hair: 'Brown', eyeColor: 'Blue' ] )
     *
     * In this example all rows where hair = Brown AND eyeColor = Blue are passed through the filter.
     *
     * @param columns a Map that contains the columns, and their values that are passed through
     * @return
     */
    public Pipeline filter( Map columns ) {
        addStep( "filter ${ nameOf(columns) }" ) { Map row ->
            if(matches(columns, row)) {
                return row
            } else {
                return reject("Row did not match the filter ${columns}", RejectionCategory.IGNORE_ROW )
            }
        }
        return this
    }

    private boolean matches(Map columns, Map row) {
        return columns.keySet().inject(true) { match, key ->
            if( columns[key] instanceof Collection ) {
                match && ((Collection)columns[key]).contains( row[key] )
            } else {
                match && row[key] == columns[key]
            }
        }
    }

    /**
     * Returns a Pipeline where all white space is removed from all columns contained within the rows.
     *
     * @return Pipeline where all rows has white space removed.
     */
    public Pipeline trim() {
        addStep("trim()") { Map row ->
            row.each { String key, Object value -> row[key] = (value as String).trim() }
            return row
        }
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline that is passed to the given closure.  The given closure
     * can configure additional steps on the branched Pipeline.  The rows passed through this Pipeline are not modified.
     *
     * @param split The closure that is passed a new Pipeline where all the rows from this Pipeline are copied onto.
     * @return this Pipeline
     */
    public Pipeline branch( Closure<Void> split) {
        Pipeline branch = new Pipeline( name )

        split( branch )

        addStep( "branch()" ) { Map row ->
            branch.process( row )
            return row
        }
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline where the given condition returns is true.  The given
     * condition works the same way {@link #filter(java.util.Map)} does.  This ia combination of branch and filter.
     *
     * @param condition The conditions that must be equal in order for the row to be copied to the branch.
     * @param split The closure that is passed the branch Pipeline.
     * @return this Pipeline
     */
    public Pipeline branch(Map<String,Object> condition, Closure<Void> split) {
        Pipeline branch = new Pipeline( name )
        split(branch)

        addStep( "branch(${condition})" ) { Map row ->
            if( matches( condition, row )) {
                branch.process( row )
            }
            return row
        }
    }

    /**
     * Returns Pipeline that joins the columns from this Pipeline with the given Pipeline where the columns are
     * equal.  It will perform a left or right join depending on the left parameter.  A left join will return the
     * row even if it doesn't find a match row on the right (also known as a left join).  A right join (ie left = false) will not return a
     * row if it doesn't find a matching row on the right (Also known as an inner join).  Default is left = false.
     *
     * @param other The right side Pipeline to use for the join
     * @param columns The columns to join on
     * @param left perform a left join (ie true) or a right join (false)
     * @return A Pipeline where the rows contain all columns from the this Pipeline and right Pipeline joined on the given columns.
     */
    public Pipeline join( Pipeline other, def columns, boolean left = false ) {
        Map<String,List<Map>> cache =[:]
        other.addStep("join(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }

        return inject("join(${this.name}, ${columns})") { Map row ->
            if( !other.complete ) {
                other.go()
            }
            String key = keyOf( row, leftColumn(columns) )

            if( left ) {
                if( cache.containsKey(key) ) {
                    return cache[key].collect { Map k ->
                        Map j = k.clone()
                        j.putAll(row)
                        return j
                    }
                } else {
                    // make sure we add columns even if they are null so sources write out columns we expect.
                    if( !cache.isEmpty() ) {
                        String c = cache.keySet().first()
                        cache[c].first().each { String i, Object v ->
                            if( !row.containsKey(i) ) row[i] = null
                        }
                    }
                    return [row]
                }
            } else if( cache.containsKey(key) ) {
                return cache[key].collect { Map k ->
                    Map j = k.clone()
                    j.putAll(row)
                    return j
                }
            } else {
                reject("Could not join on ${columns}", RejectionCategory.IGNORE_ROW )
            }
        }
    }

    /**
     * This returns a Pipeline where the rows with empty columns are filled in using the values in the previous row depending on
     * what the given closure returns.  If the closure returns true then any empty column (value == null or value.isEmpty()) will
     * be populated by the values in the previous row.
     *
     * @param decider a Closure which decides if the values from a prior row will be used to fill in missing values in the current row.
     * @return A Pipeline where the row's empty column values are filled in by the previous row.
     */
    public Pipeline fillDownBy( Closure<Boolean> decider ) {
        Map previousRow = null
        addStep("fillDownBy()") { Map row ->
            if( previousRow && decider( row, previousRow ) ) {
                row.each { String col, Object value ->
                    // todo refactor valid_to out for excluded
                    if (col != "valid_To" && (value == null || value.isEmpty())) {
                        row[col] = previousRow[col]
                    }
                }
            }
            previousRow = row.clone()
            return row
        }
        return this
    }

    /**
     * Renames a row's columns in the given map to the values of the corresponding keys.
     *
     * @param fieldNames The Map of src column to renamed names.
     * @return A Pipeline where all of the columns in the keys of the Map are renamed to the Map's corresponding values.
     */
    public Pipeline renameFields( Map fieldNames ) {
        addStep("renameFields(${fieldNames}") { Map row ->
            for( String src : fieldNames.keySet() ) {
                String dest = fieldNames.get( src )
                row[dest] = row.remove( src )
            }
            return row
        }
        return this
    }

    /**
     * Returns a Pipeline where all of the rows from this Pipeline and adds a single column 
     * "included" with a true/false value depending on whether the current row is occurs
     * in the given Pipeline and the values of the specified columns are equal in both 
     * Pipelines.
     *
     * @param other Pipeline to verify if the rows where the columns of those rows are equal 
     * to the rows in this Pipeline
     * @param the list of columns to check against (either Map or Collection).
     * return A Pipeline where each row in this Pipeline is in the new Pipeline and a colun 
     * included is added based on if this row according to the given columns also occurs in 
     * the other Pipeline
     */

    public Pipeline intersect( Pipeline other, def columns ) {
        Map <String,List<Map>> cache = [:]
        other.addStep("intersect(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }.start()

        addStep("intersect(${this.name}, ${columns})") { Map row ->
            String key = keyOf( row, leftColumn(columns) )
            row.included = cache.containsKey(key)
            return row
//            return cache.containsKey(key) ? row : null
        }

        return this
    }

    private List<String> leftColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).keySet().asList()
        } else {
            return [columns.toString()]
        }
    }

    private List<String> rightColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).values().asList()
        } else {
            return [columns.toString()]
        }
    }

    private String nameOf(Map columns) {
        return columns.keySet().collect() { key -> key + "->" + columns[key] }.join(',')
    }

    /**
     * Returns a Pipeline where the row is grouped by the given columns.  The resulting Pipeline will only
     * return a single row where the keys of that row will be the first column passed to the groupBy() method.
     * All other columns given will occur under the respective keys.  This yields a tree like structure where
     * the height of the tree is equal to the columns.length.  In the leaves of the tree are the rows that
     * matched all of their parents.
     *
     * @param columns The columns to group each row by.
     * @return A Pipeline that yields a single row that represents the tree grouped by the given columns.
     */
    public Pipeline groupBy( String... columns ) {
        Map cache = [:]
        addStep("groupBy(${columns.join(',')})") { Map row ->
            Map current = cache
            columns.eachWithIndex { String col, int i ->
                if( !current.containsKey(row[col]) ) {
                    if( i + 1 < columns.size() ) {
                        current[row[col]] = [:]
                        current = (Map)current[row[col]]
                    } else {
                        current[row[col]] = []
                    }
                } else if( i + 1 < columns.size() ) {
                    current = (Map)current[row[col]]
                }
            }

            current[ row[columns.last()] ] << row
            return row
        }

        Pipeline parent = this
        Pipeline other = new Pipeline( statistic.name )
        other.src = new Source() {
            @Override
            void start(Closure closure) {
                parent.start() // first start our parent pipeline
                closure( cache )
            }
        }
        other.copyStatistics( this )
        return other
    }

    /**
     * Returns a Pipeline where the rows are ordered by the given columns.  The value of 
     * each column is compared using the <=> operator.
     * @param columns to sort by
     * @return a Pipeline that where it's rows are ordered according to the given columns.
     */
    public Pipeline sort(String... columns) {
        // todo sort externally

        Comparator<Map> comparator = new Comparator<Map>() {
            @Override
            int compare(Map o1, Map o2) {
                for( String key : columns ) {
                    int value = o1[key] <=> o2[key]
                    if( value != 0 ) return value;
                }
                return 0
            }
        }

        List<Map> ordered = []
        addStep("sort(${columns})") { Map row ->
            //int index = Collections.binarySearch( ordered, row, comparator )
            //ordered.add( Math.abs(index + 1), row )
            ordered << row
            return row
        }

        Pipeline next = new Pipeline(statistic.name)
        next.src = new ChainedSource( this )
        after {
            next.statistic.rejectionsByCategory = this.statistic.rejectionsByCategory
            next.statistic.start = this.statistic.start
            ordered.sort( comparator )
            ((ChainedSource)next.src).process( ordered )
            null
        }

        return next
    }

    /**
     * Returns a Pipeline where the given column is coverted from a string to a java.lang.Double.
     * @param column The name of the column to convert into a Double
     * @return A Pipeline where all rows contains a java.lang.Double at the given column
     */
    Pipeline asDouble(String column) {
        addStep("asDouble(${column})") { Map row ->
            String value = row[column] as String
            try {
                if (value) row[column] = Double.parseDouble(value)
                return row
            } catch( NumberFormatException ex) {
                reject("Could not parse ${value} as a Double", RejectionCategory.INVALID_FORMAT)
            }
        }
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Integer value.
     * @param column containing a string to be turned into a java.lang.Integer
     * @return A Pipeline where all rows contain a java.lang.Integer at given column
     */
    Pipeline asInt(String column) {
        addStep("asInt(${column})") { Map row ->
            String value = row[column] as String
            try {
                if( value ) row[column] = Integer.parseInt(value)
                return row
            } catch( NumberFormatException ex ) {
                reject("Could not parse ${value} to an integer.", RejectionCategory.INVALID_FORMAT)
            }
        }
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Boolean value.  It understands values like: Y/N, YES/NO, TRUE/FALSE, 1/0, T/F.
     * @param column containing a string to be turned into a java.lang.Boolean
     * @return A Pipeline where all rows contain a java.lang.Boolean at given column
     */
    Pipeline asBoolean(String column) {
        addStep("asBoolean(${column}") { Map row ->
            String value = row[column]
            if( value ) {
                switch( value ) {
                    case "Y":
                    case "y":
                    case "yes":
                    case "YES":
                    case "Yes":
                    case "1":
                    case "T":
                    case "t":
                        row[column] = true
                        break
                    case "n":
                    case "N":
                    case "NO":
                    case "no":
                    case "No":
                    case "0":
                    case "F":
                    case "f":
                    case "null":
                    case "Null":
                    case "NULL":
                    case null:
                        row[column] = false
                        break
                    default:
                        row[column] = Boolean.parseBoolean(value)
                        break
                }
            }
            return row
        }
    }

    /**
     * Parses the string at the given column name into a Date object using the given format.  Any value not
     * parseable by the format is rejected.
     * @param column The field to use to find the string value to parse
     * @param format The format of the string to use to parse into a java.util.Date
     * @return A Pipeline where all rows contain a java.util.Date at given field name
     */
    Pipeline asDate(String column, String format = "yyyy-MM-dd") {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format)
        addStep("asDate(${column}, ${format})") { Map row ->
            String val = row[column] as String
            try {
                if (val) row[column] = dateFormat.parse(val)
                return row
            } catch( ParseException ex ) {
                reject( "${row[column]} could not be parsed by format ${format}", RejectionCategory.INVALID_FORMAT )
            }
        }
        return this
    }

    /**
     * This writes each row to the specified filename as a CSV separated by the given separator.  It can optionally
     * select a subset of column names from each row.  If unspecified all columns will be saved.
     *
     * @param filename the filename to write the CSV file to
     * @param separator the field separator to use between each field value (default ",")
     * @param columns the list of fields to write from each row.  (default null)
     * @return A Pipeline
     */
    public Pipeline save( String filename, String separator = ",", List<String> columns = null ) {
        CSVFile out = new CSVFile( filename, separator )
        addStep("Save to ${out.file.name}") { Map row ->
            if( columns ) {
                out.write( row, columns?.toArray(new String[columns.size()]) )
            } else {
                out.write( row )
            }
            return row
        }

        after {
            out.close()
        }
        return this
    }

    /**
     * Prints the values of the given columns for each row to the console, or all columns if no columns are given.
     * @param columns The names of the columns to print to the console
     * @return this Pipeline
     */
    public Pipeline printRow(String... columns) {
        addStep("print()") { Map row ->
            if( columns ) {
                println( "[ ${columns.toList().collect { row[it] }.join(',')} ]" )
            } else {
                println( row )
            }
            return row
        }
        return this
    }

    public Pipeline progress( int col = 50 ) {
        int line = 1
        addStep("progress()") { Map row ->
            line++
            printf(".")
            if( line % col ) println()
            row
        }
    }

    /**
     * Sets a fieldName in each row to the given value.
     * @param fieldName The new field name to add
     * @param value the value of the new field name
     * @return The Pipeline where each row has a fieldname set to the given value
     */
    public Pipeline setField(String fieldName, Object value ) {
        addStep("setField(${fieldName})") { Map row ->
            row[fieldName] = value
            return row
        }
        return this
    }
    /**
     * Adds a new field to each row with the value returned by the given closure.
     * @param fieldName The new field name to add
     * @param fieldValue The closure that returns a value to set the given field's name to.
     * @return The Pipeline where the fieldname exists in every row
     */
    public Pipeline addField(String fieldName, Closure fieldValue) {
        addStep("addField(${fieldName})") { Map row ->
            Object value = fieldValue(row)
            if( value instanceof Rejection ) return value
            row[fieldName] = value
            return row
        }
        return this
    }

    /**
     * Only allows rows that are unique per the given column.
     *
     * @param column The column name to use for checking uniqueness
     * @return A Pipeline that only contains the unique rows for the given column
     */
    Pipeline unique(String column) {
        Set<Object> unique = [:] as HashSet
        addStep("unique(${column})") { Map row ->
            if( unique.contains(row[column]) ) return reject("Non-unique row returned", RejectionCategory.IGNORE_ROW)
            unique.add( row[column] )
            return row
        }
        return this
    }

    /**
     * This takes a closure that takes Map and returns a Collection<Map>.  Each member of the returned collection will
     * be fed into downstream steps.  The reset flag specifies whether the statistics should be reset (true) or the
     * existing statistics will be carried (false).
     * @param name The name of the step
     * @param closure Takes a Map and returns a Collection<Map> that will be fed into the downstream steps
     * @return The Pipeline that will received all members of the Collection returned from the closure.
     */
    public Pipeline inject(String name, Closure closure) {
        Pipeline next = new Pipeline(name)
        next.src = new ChainedSource( this )

        addStep(name) { Map row ->
            def result = closure( row )
            if( result instanceof Rejection ) {
                next.doRejections((Rejection)result, row, name, -1)
                return result
            } else {
                Collection<Map> cc = result
                ((ChainedSource)next.src).process( cc )
            }
            return row
        }
        next.copyStatistics( this )
        return next
    }

    void copyStatistics(Pipeline src) {
        after {
            this.statistic.start = src.statistic.start
            for( RejectionCategory cat : src.statistic.rejectionsByCategory.keySet() ) {
                if( !this.statistic.rejectionsByCategory.containsKey(cat) ) {
                    this.statistic.rejectionsByCategory[ cat ] = 0
                }
                this.statistic.rejectionsByCategory[ cat ] = this.statistic.rejectionsByCategory[ cat ] + src.statistic.rejectionsByCategory[ cat ]
            }
        }
    }
/**
     * This takes a closure that returns a Pipeline which is used to feed the returned Pipeline.  The closure will be called
     * for each row emitted from this Pipeline so the closure could create multiple Pipelines, and all data from every Pipeline
     * will be fed into the returned Pipeline.
     *
     * @param closure A closure that takes a Map and returns a pipeline that it's data will be fed on to the returned pipeline.
     *
     * @return A Pipeilne whose reocrds consist of the records from all Pipelines returned from the closure
     */
    public Pipeline exchange(Closure<Pipeline> closure) {
        Pipeline next = new Pipeline( name )
        next.src = new ChainedSource(this)
        addStep("exchange()") { Map row ->
            Pipeline pipeline = closure( row )
            pipeline.start { Map current ->
                next.process( current )
                return current
            }
            return row
        }
        next.copyStatistics( this )
        return next
    }

    /**
     * @param closure A closure that 
     */
    public Pipeline inject( Closure closure) {
        this.inject("inject()", closure )
    }

    public void start(Closure closure = null) {
        if( closure ) addStep("tail", closure)

        statistic.start = System.currentTimeMillis()
        int line = 1
        src.start { Map row ->
            line++
            return process(row, line)
        }
        statistic.end = System.currentTimeMillis()

        statistic.timed("Done Callbacks") {
            doneChain.each { Closure current ->
                current()
            }
        }
        complete = true
    }

    /**
     * Starts processing the rows returned from the {@link gratum.source.Source} into the Pipeline.  When the
     * {@link gratum.source.Source} is finished this method returns the {@link gratum.etl.LoadStatistic} for
     * this Pipeline.  The given closure is added as the final step in the Pipeline if provided.
     *
     * @param closure The final step added to the Pipeline if non-null.
     * @return The LoadStatistic instance from this Pipeline.
     */
    public LoadStatistic go(Closure<Map> closure = null) {
        start(closure)
        return statistic
    }

    /**
     * This method is used to send rows to the Pipeline for processing.  Each row passed will start at the first step of
     * the Pipeline and proceed through each step.
     * @param row The row to be processed by the Pipeline's steps.
     * @param lineNumber The lineNumber from the {@link gratum.source.Source} to use when tracking this row through the Pipeline
     */
    public boolean process(Map row, int lineNumber = -1) {
        Map current = new LinkedHashMap(row)
        for (Step step : processChain) {
            try {
                boolean stop = statistic.timed(step.name) {
                    def ret = step.step(current)
                    if (!ret || ret instanceof Rejection ) {
                        doRejections((Rejection)ret, current, step.name, lineNumber)
                        return true
                    }
                    current = ret
                    return false
                }
                if (stop) return false
            } catch (Exception ex) {
                throw new RuntimeException("Line ${lineNumber > 0 ? lineNumber : row}: Error encountered in step ${statistic.name}.${step.name}", ex)
            }
        }
        statistic.loaded++
        return false // don't stop!
    }

    private void doRejections(Rejection ret, Map current, String stepName, int lineNumber) {
        Rejection rejection = ret ?: new Rejection("Unknown reason", RejectionCategory.REJECTION)
        rejection.step = stepName
        current.rejectionCategory = rejection.category
        current.rejectionReason = rejection.reason
        current.rejectionStep = rejection.step
        statistic.reject(rejection?.category ?: RejectionCategory.REJECTION)
        rejections?.process(current, lineNumber)
    }

    private String keyOf( Map row, List<String> columns ) {
        return columns.collect { key -> row[key] }.join(":")
    }

    public static Rejection reject( String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        return new Rejection( reason, category )
    }
  
}
