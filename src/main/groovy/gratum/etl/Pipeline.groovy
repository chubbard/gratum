package gratum.etl

import gratum.pgp.PgpContext
import gratum.sink.CsvSink
import gratum.sink.JsonSink
import gratum.sink.Sink
import gratum.source.AbstractSource
import gratum.csv.HaltPipelineException
import gratum.source.ChainedSource
import gratum.source.ClosureSource
import gratum.source.Source

import groovy.transform.CompileStatic

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.regex.Pattern

/**
 * A Pipeline represents a series of steps that will be performed on 1 or more rows.  Rows are Map objects
 * that are passed from the Source through each step individually.  A row can be modified, transformed into 
 * something else, or rejected by a step.  If a row is rejected in any step all successive steps are not 
 * processed and the row is passed to the rejections Pipeline (if one exists).  Any rows that pass through 
 * the entire Pipeline are said to have been loaded.  Any row that does not pass through all steps is said 
 * to be rejected.
 * 
 * Rows originate from the Pipeline's {@link gratum.source.Source}.  The {@link gratum.source.Source} sends
 * rows into the Pipeline until it finishes.  The Pipeline keeps a set of statistics about how many rows
 * were loaded, rejected, the types of rejections, timing, etc.  This is kept in the {@link gratum.etl.LoadStatistic}
 * instance and returned from the {@link Pipeline#go()} method.
 *
 * Example:
 *
 * <pre>
 *      LoadStatistic stats = http("http://api.open-notify.org/astros.json").
 *          inject { Map json -&gt;
 *              json.people
 *          }.
 *          printRow().
 *          go()
 * </pre>
 *
 * In the above example you can see it using an {@link gratum.source.HttpSource} to fetch JSON data.  
 * That data is returned as a Map object which has other nested objects within it.  In this case it's
 * pulling out the "people" column which is a Collection of people objects.  Then it injects those members
 * into the down stream steps which uses printRow to print it to the console.  The output would look like
 * the following:
 * 
 * <pre>
 *    [name:Sergey Prokopyev, craft:ISS]
 *    [name:Alexander Gerst, craft:ISS]
 *    [name:Serena Aunon-Chancellor, craft:ISS]
 *    ===&gt;
 *    ----
 *    ==&gt; inject()
 *    loaded 3
 *    rejected 0
 *    took 1 ms
 * </pre>
 */
//@CompileStatic
public class Pipeline {

    public static final String REJECTED_KEY = "__reject__"
    public static final int DO_NOT_TRACK = -1

    String name
    Source src
    List<Step> processChain = []
    List<AfterStep> doneChain = []
    Pipeline parent
    Pipeline rejections
    boolean complete = false
    int loaded = 0

    Pipeline(String name, Pipeline parent = null) {
        this.name = name
        this.parent = parent
    }

    /**
     * Creates a pipeline where startClosure is the source.  The startClosure is passed another closure that it can use
     * to pass an individual row to the Pipeline.
     *
     * @param name name of the pipeline to create
     * @param startClosure A closure that is called with a closure.  The startClosure can call the closure argument it's 
     * passed to send a row into the pipeline.
     * @return The pipeline attached to results of the startClosure.
     */
    public static Pipeline create( String name, @DelegatesTo(Pipeline) Closure startClosure ) {
        Pipeline pipeline = new Pipeline(name)
        pipeline.src = new ClosureSource(startClosure)
        return pipeline
    }

    /**
     * The name of the pipeline.  This is used by {@link LoadStatistic} to identify what Pipeline it came from.
     *
     * @return The name of the Pipeline
     */
    public String getName() {
        return this.name
    }

    /**
     * Apply a closure to the Pipeline allowing it to configure the Pipeline by passing this Pipeline as an argument
     * to the closure.  The Pipeline returned by the closure will be returned by this method for chaining.
     *
     * @param template A Closure that will be passed this Pipeline and returns a Pipeline.
     * @return The Pipeline returned by the given Closure.
     */
    public Pipeline configure( Closure<Pipeline> template ) {
        return template.call( this )
    }


    /**
     * Prepend a step to the pipeline.
     * @param name The Step name
     * @param step The code used to process each row on the Pipeline.
     * @return this Pipeline.
     */
    public Pipeline prependStep( String name = null, @DelegatesTo(Pipeline) Closure<Map> step ) {
        step.delegate = this
        processChain.add(0, new Step( name, step ) )
        return this
    }

    /**
     * Adds a step to the pipeline.  It's passed an optional name to identify the step by, and a closure that represents
     * the individual step.  It returns the Map to be processed by the next step in the pipeline, typically it simply returns the same
     * row it was passed.  If it returns null or {@link Rejection} then it will reject this row, stop processing additional
     * steps, and pass the current row to the rejections pipeline.
     *
     * @param name The step name
     * @param step The code used to process each row on the Pipeline.
     * @return this Pipeline.
     */
    public Pipeline addStep( String name = null, @DelegatesTo(Pipeline) Closure<Map> step ) {
        step.delegate = this
        processChain << new Step( name, step )
        return this
    }

    public Pipeline addStep(GString name, @DelegatesTo(Pipeline) Closure<Map<String,Object>> step) {
        this.addStep( name.toString(), step )
    }

    /**
     * Adds a closure to the end of the Pipeline.  This is called after all rows are processed.  This closure is
     * invoked without any arguments.
     *
     * @param step the Closure that is invoked after all rows have been processed.
     * @return this Pipeline.
     */
    public Pipeline after( @DelegatesTo(Pipeline) Closure<Void> step ) {
        doneChain << new AfterStep(step)
        return this
    }

    /**
     * Takes a closure that is passed the rejection Pipeline.  The closure can register steps on the rejection
     * pipeline, and any rejections from the parent pipeline will be passed through the given rejection pipeline.
     *
     * @param branch Closure that's passed the rejection the pipeline
     * @return this Pipeline
     */
    public Pipeline onRejection( @DelegatesTo(Pipeline) Closure<Void> branch ) {
        if( parent ) {
            parent.onRejection( branch )
        } else {
            if( !rejections ) rejections = new Pipeline("Rejections(${name})")
            rejections.addStep("Remap rejections to columns") { Map row ->
                Map current = (Map)row.clone()
                Rejection rejection = (Rejection)current.remove(REJECTED_KEY)
                current.rejectionCategory = rejection.category
                current.rejectionReason = rejection.reason
                current.rejectionStep = rejection.step
                return current
            }
            branch.delegate = rejections
            branch( rejections )
            after {
                rejections.doneChain.each { AfterStep step ->
                    step.execute()
                }
                return
            }
        }
        return this
    }

    /**
     * Concatenates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
     * rows from this pipeline and the src pipeline.
     *
     * @param src The pipeline
     * @return Returns a new pipeline that combines all of the rows from this pipeline and the src pipeline.
     */
    public Pipeline concat( Pipeline src ) {
        Pipeline original = this
        src.parent = this
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
    public Pipeline filter(String name = "filter()", @DelegatesTo(Pipeline) Closure callback) {
        callback.delegate = this
        addStep( name ) { Map<String,Object> row ->
            if( !callback(row) ) {
                return reject(row,"Row did not match the filter closure.", RejectionCategory.IGNORE_ROW )
            }
            return row
        }
        return this
    }

    /**
     * This adds a step to the Pipeline that passes all rows where the values of the columns on the given Map are equal
     * to the columns in the row.  This is a boolean AND between columns.  For example:
     *
     * .filter( [ hair: 'Brown', eyeColor: 'Blue' ] )
     *
     * In this example all rows where hair = Brown AND eyeColor = Blue are passed through the filter.
     *
     * You can also pass a java.util.Collection of values to compare a column to.  For example,
     *
     * .filter( [hair: ['Brown','Blonde'], eyeColor: ['Blue','Green'] ] )
     *
     * In this example you need hair of 'Brown' or 'Blonde' and eyeColor of 'Blue' or 'Green'.
     *
     * You can also pass a groovy.lang.Closure as a value for a column and it will filter based
     * on the value returned by the Closure.  For example,
     *
     * .filter( [hair: { String v -> v.startsWith('B') }, eyeColor: 'Brown'] )
     *
     * In this example it invokes the Closure with the value at row['hair'] and the Closure evaluates
     * to a boolean to decide if a row is filtered or not.
     *
     * @param columns a Map that contains the columns, and their values that are passed through
     * @return A pipeline that only includes the rows matching the given filter.
     */
    public Pipeline filter( Map columns ) {
        Condition condition = new Condition( columns )
        addStep( "filter ${ condition }" ) { Map row ->
            if( condition.matches(row) ) {
                return row
            } else {
                return reject( row,"Row did not match the filter ${columns}", RejectionCategory.IGNORE_ROW )
            }
        }
        return this
    }

    /**
     * Returns a Pipeline where all white space is removed from all columns contained within the rows.
     *
     * @return Pipeline where all columns of each row has white space removed.
     */
    public Pipeline trim() {
        addStep("trim()") { Map<String,Object> row ->
            row.each { String key, Object value -> row[key] = (value as String)?.trim() }
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
    public Pipeline branch( String branchName = "branch", Closure<Pipeline> split) {
        final Pipeline branch = new Pipeline( "${name}/${branchName}", this )

        Pipeline tail = split( branch )

        addStep( "branch(${branchName})" ) { Map row ->
            branch.process( new LinkedHashMap(row) )
            return row
        }

        after {
            tail.start()
        }
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline where the given condition returns is true.  The given
     * condition works the same way {@link #filter(java.util.Map)} does.  This ia combination of branch and filter.
     * No rows on this pipeline are filtered out.  Only the rows on the branch will be filtered.
     *
     * @param condition The conditions that must be equal in order for the row to be copied to the branch.
     * @param split The closure that is passed the branch Pipeline.
     * @return this Pipeline
     */
    public Pipeline branch(Map<String,Object> condition, Closure<Pipeline> split) {
        Pipeline branch = new Pipeline( "${name}/branch(${condition})", this )
        Pipeline tail = split(branch)

        Condition selection = new Condition( condition )
        addStep( "branch(${condition})" ) { Map row ->
            if( selection.matches( row )) {
                branch.process( new LinkedHashMap(row) )
            }
            return row
        }

        after {
            tail.start()
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
        Map<String,List<Map<String,Object>>> cache =[:]
        other.addStep("join(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }

        return this.inject("join(${this.name}, ${columns})") { Map<String,Object> row ->
            if( !other.complete ) {
                other.go()
            }
            String key = keyOf( row, leftColumn(columns) )

            if( left ) {
                if( cache.containsKey(key) ) {
                    return cache[key].collect { Map k ->
                        Map j = (Map)k.clone()
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
                    Map j = (Map)k.clone()
                    j.putAll(row)
                    return j
                }
            } else {
                return [ reject( row,"Could not join on ${columns}", RejectionCategory.IGNORE_ROW ) ]
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
        Map<String,Object> previousRow = null
        addStep("fillDownBy()") { Map<String,Object> row ->
            if( previousRow && decider( row, previousRow ) ) {
                row.each { String col, Object value ->
                    // todo refactor valid_to out for excluded
                    if (col != "valid_To" && (value == null || !value)) {
                        row[col] = previousRow[col]
                    }
                }
            }
            previousRow = (Map<String,Object>)row.clone()
            return row
        }
        return this
    }

    /**
     * Rename a row's columns in the given map to the value of the corresponding key.
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
     * Return a Pipeline where all of the rows from this Pipeline and adds a single column
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
            return ((Collection<String>)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).keySet().asList()
        } else {
            return [columns.toString()]
        }
    }

    private List<String> rightColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection<String>)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).values().asList()
        } else {
            return [columns.toString()]
        }
    }

    /**
     * Return a Pipeline where the row is grouped by the given columns.  The resulting Pipeline will only
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

            ((List)current[ row[columns.last()] ]) << row
            return row
        }

        Pipeline parent = this
        Pipeline other = new Pipeline( name, this )
        other.src = new AbstractSource() {
            @Override
            void start(Pipeline pipeline) {
                parent.start() // first start our parent pipeline
                pipeline.process( cache, 1 )
            }
        }
        return other
    }

    /**
     * Return a Pipeline where the rows are ordered by the given columns.  The value of
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
                    int value = (Comparable)o1[key] <=> (Comparable)o2[key]
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

        Pipeline next = new Pipeline(name, this)
        next.src = new ChainedSource( this )
        after {
            ordered.sort( comparator )
            ((ChainedSource)next.src).process( ordered )
            null
        }

        return next
    }

    /**
     * Return a Pipeline where the given column is converted from a string to a java.lang.Double.
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
                return reject( row,"Could not parse ${value} as a Double", RejectionCategory.INVALID_FORMAT)
            }
        }
    }

    /**
     * Parses the string value at given column into a java.lang.Integer value.
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
                return reject( row,"Could not parse ${value} to an integer.", RejectionCategory.INVALID_FORMAT)
            }
        }
    }

    /**
     * Parses the string value at given column into a java.lang.Boolean value.  It understands values like: Y/N, YES/NO, TRUE/FALSE, 1/0, T/F.
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
     * Parses the string at the given column name into a Date object using the given format.  Any value that
     * cannot be parsed by the format is rejected.  Null values or empty strings are not rejected.
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
                return reject( row, "${row[column]} could not be parsed by format ${format}", RejectionCategory.INVALID_FORMAT )
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
     * @return A Pipeline that returns a row for the csv file.
     */
    public Pipeline save(String filename, String separator = ",", List<String> columns = null ) {
        return this.save( new CsvSink(filename, separator, columns) )
    }

    /**
     * Write the rows to a provided {@link gratum.sink.Sink}.  It returns the
     * {@link gratum.etl.Pipeline} that returns the results of the given sink.
     *
     * @param sink the concrete Sink instance to save data to.
     * @return The resulting {@link gratum.etl.Pipeline}.
     */
    public Pipeline save(Sink<Map<String,Object>> sink ) {
        sink.attach( this )

        Pipeline next = new Pipeline( sink.name, this )
        next.loaded = DO_NOT_TRACK
        next.src = new ChainedSource( this )
        after {
            sink.close()
            next.process(sink.result)
            return
        }
        return next
    }

    /**
     * Write out the rows produced to JSON file.
     * @param filename the filename to save the JSON into.
     * @param columns Optional list of columns to use clip out certain columns you want to include in the output.  If left
     * off all columns are included.
     * @return A Pipeline unmodified.
     */
    public Pipeline json(String filename, List<String> columns = null) {
        return save( new JsonSink( new File(filename), columns) )
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
    public Pipeline addField(String fieldName, @DelegatesTo(Pipeline) Closure fieldValue) {
        fieldValue.delegate = this
        addStep("addField(${fieldName})") { Map row ->
            Object value = fieldValue(row)
            if( value instanceof Rejection ) {
                row[REJECTED_KEY] = value
                return row
            }
            row[fieldName] = value
            return row
        }
        return this
    }

    /**
     * Removes a field based on whether the given closure returns true or false.  The closure is optional
     * which will always remove the fieldName if not provided.
     *
     * @param fieldName the name of the field to remove depending on what the optional closure returns
     * @param removeLogic an optiona closure that when given can return true or false to indicate to remove
     * the field or not.  If not provided the field is always removed.
     * @return The pipeline where the fieldName has been removed when the removeLogic closure returns true or itself is null.
     */
    public Pipeline removeField(String fieldName, @DelegatesTo(Pipeline) Closure removeLogic = null) {
        removeLogic?.delegate = this
        addStep( "removeField(${fieldName})") { Map row ->
            if( removeLogic == null || removeLogic(row) ) {
                row.remove(fieldName)
            }
            return row
        }

        return this
    }

    /**
     * Remove all columns from each row so that only the fields given will be returned.
     * @param columns THe columns names to retain from each row
     * @return The pipeline where only the given columns are returned
     */
    public Pipeline clip(String... columns) {
        addStep( "clip(${columns.join(",")}") { Map row ->
            Map<String,Object> result = [:] as Map<String,Object>
            for( String key : row.keySet() ) {
                if( columns.contains(key) ) {
                    result[key] = row[key]
                }
            }
            return result
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
            if( unique.contains(row[column]) ) {
                return reject(row, "Non-unique row returned", RejectionCategory.IGNORE_ROW)
            }
            unique.add( row[column] )
            return row
        }
        return this
    }

    /**
     * Just a helper method for using GString in {@link #inject(String,Closure)}.
     *
     * @param name Name of the inject step
     * @param closure Closure returns a an Iterable used to inject those rows into down stream steps.
     * @return Pipeline that will receive all members of the Iterable returned from the given closure.
     */
    public Pipeline inject(GString name, @DelegatesTo(Pipeline) Closure<Iterable<Map<String,Object>>> closure ) {
        return this.inject( name.toString(), closure )
    }

    /**
     * Injects the Collection&lt;Map&gt; returned from the given closure into the downstream steps as individual rows.
     * The given closure is called for every row passed through the preceding step.  Each member of the returned
     * collection will be fed into downstream steps as separate rows.
     * @param name The name of the step
     * @param closure Takes a Map and returns a Collection&lt;Map&gt; that will be fed into the downstream steps
     * @return The Pipeline that will receive all members of the Iterable returned from the closure.
     */
    public Pipeline inject(String name, @DelegatesTo(Pipeline) Closure<Iterable<Map<String,Object>>> closure) {
        Pipeline next = new Pipeline(name, this)
        next.src = new ChainedSource( this )
        closure.delegate = this
        addStep(name) { Map row ->
            Iterable<Map<String,Object>> result = closure.call( row )
            if( result == null ) {
                row = reject( row, "Unknown Reason", RejectionCategory.REJECTION )
                next.doRejections(row, name, -1)
                return row
            } else {
                Iterable<Map<String,Object>> cc = result
                for( Map<String,Object> r : result ) {
                    if( !r[REJECTED_KEY] ) {
                        next.process( r, -1 )
                    }
                }
            }
            return row
        }
        return next
    }

    /**
     * This takes a closure that returns a Pipeline which is used to feed the Pipeline returned by exchange().  The
     * closure will be called for each row emitted from this Pipeline so the closure could create multiple Pipelines,
     * and all data from every Pipeline will be fed into the returned Pipeline.
     *
     * @param closure A closure that takes a Map and returns a pipeline that it's data will be fed on to the
     * returned pipeline.
     *
     * @return A Pipeline whose records consist of the records from all Pipelines returned from the closure
     */
    public Pipeline exchange(Closure<Pipeline> closure) {
        Pipeline next = new Pipeline( name, this )
        next.src = new ChainedSource(this)
        addStep("exchange(${next.name})") { Map row ->
            Pipeline pipeline = closure( row )
            pipeline.addStep("Exchange Bridge(${pipeline.name})") { Map current ->
                next.process( current )
                return current
            }
            pipeline.start()
            return row
        }
        return next
    }

    /**
     * Delegates to {@link #inject(java.lang.String, groovy.lang.Closure)} with a default name.
     * @param closure Takes a Map and returns a Collection&lt;Map&gt; that will be fed into the downstream steps
     * @return The Pipeline that will received all members of the Collection returned from the closure.
     */
    public Pipeline inject( @DelegatesTo(Pipeline) Closure closure) {
        this.inject("inject()", closure )
    }

    /**
     * Sets the destination column to the value within the given Map when the destination column is empty/null.
     * @param defaults A Map of destination column to value where destination column is key and the values are the default
     * values used when the destination column is empty/null.
     * @return A Pipeline where the destination columns are initialized to the values in the Map when the destination
     * column is empty.
     */
    public Pipeline defaultValues( Map<String,Object> defaults ) {
        this.addStep("defaultValues for ${defaults.keySet()}") { Map row ->
            defaults.each { String column, Object value ->
                if( !row[column] ) row[column] = value
            }
            return row
        }
    }

    /**
     * Sets the destination column to the value of the source column if it is empty or null.
     * @param defaults A Map containing the destination column as the key and source column as the value.
     * @return A pipeline where the rows will have the destination columns set to the source column if empty/null.
     */
    public Pipeline defaultsBy( Map<String,String> defaults ) {
        this.addStep("defaultsBy for ${defaults.keySet()}") { Map row ->
            defaults.each { String destColumn, String srcColumn ->
                if( !row[destColumn] ) row[destColumn] = row[srcColumn]
            }
            return row
        }
    }

    /**
     * Limit the number of rows you take from a source to an upper bound.  After the upper bound is hit it will either
     * stop processing rows immediately or continue processing rows but simply reject the remaining based on if halt
     * is true or false, respectively.  If it's rejected rows then all steps above the limit will be executed for
     * all rows.  All steps after the limit operation will only process limit number of rows.
     *
     * @param limit An upper limit on the number of rows this pipeline will process.
     * @param halt after limit has been exceeded stop processing any additional rows (halt = true),
     *              or continue to process rows but simply reject all rows (halt = false).
     * @return A pipeline where only limit number of rows will be sent to down stream steps.
     */
    public Pipeline limit(long limit, boolean halt = true) {
        int current = 0
        this.addStep("Limit(${limit})") { Map row ->
            current++
            if( current > limit ) {
                if( halt ) {
                    throw new HaltPipelineException("Over the maximum limit of ${limit}")
                } else {
                    return reject(row, "Over the maximum limit of ${limit}", RejectionCategory.IGNORE_ROW)
                }
            }
            return row
        }
    }

    /**
     * Passed a closure that is called with this Pipeline.  This enables you to perform operation
     * on the Pipeline itself without breaking the flow of functional chain.  This does not
     * add a step to the Pipeline.
     * @param The Closure
     * @return this Pipeline
     */
    public Pipeline apply(Closure<Void> applyToPipeline) {
        applyToPipeline.call( this )
        return this
    }

    /**
     * Replaces all occurrences of the regular expression with given withClause.  The
     * withClause can use $1, $2, $3, etc to refer to groups within the regular expression
     * just like in replaceAll method.
     *
     * @param column the name of the column to pull the value from.
     * @param regEx the regular expression to use for replacing sections of the value.
     * @param withClause the replacement clause swap out the portion matched by the
     * regular expression.
     * @return this Pipeline where the content matched by the regular expression
     * replaced with the given withClause
     */
    public Pipeline replaceAll(String column, Pattern regEx, String withClause) {
        addStep( "replaceAll(${column}, ${regEx.toString()})") { Map row ->
            String v = row[column]
            row[column] = v?.replaceAll( regEx, withClause )
            return row
        }
    }

    /**
     * Given a column replace all values at that column that match the given
     * key with the value of the values Map.  This basically transform values[key]
     * into values[value] and stores that in the given column.
     *
     * @param column the column to compare against the value map key.
     * @param values the key and value pair that will be substituted given the value at the column
     * @return this Pipeline
     */
    public Pipeline replaceValues(String column, Map<String,String> values ) {
        addStep( "replaceValues(${column}, ${values})" ) { Map row ->
            String v = row[column]
            if( values.containsKey(v) ) {
                row[column] = values[ v ] ?: row[column]
            }
            return row
        }
    }

    /**
     * Encrypts using PGP a stream on the pipeline and rewrite that stream back to the pipeline.  It looks for
     * a stream on the Pipeline at streamProperty. Further configuration is performed by the provided Closure
     * that is passed a {@link gratum.pgp.PpgContext}.  You are required to setup the identities, secret key collection,
     * and/or public key collection in order to encrypt.  This will write the encrypted stream back to the Pipeline
     * on the provided streamProperty.  It also adds the file and filename properties to the existing row.
     * @param streamProperty The property that holds a stream object to be encrypted.
     * @param configure The Closure that is passed the PgpContext used to configure how the stream will be encrypted.
     */
    public Pipeline encryptPgp(String streamProperty, Closure configure ) {
        PgpContext pgp = new PgpContext()
        configure.call( pgp )
        addStep("encrypt(${streamProperty})") { Map row ->
            File encryptedTemp = File.createTempFile("pgp-encrypted-output-${streamProperty}".toString(), ".gpg")
            InputStream stream = row[streamProperty] as InputStream
            try {
                encryptedTemp.withOutputStream { OutputStream out ->
                    pgp.encrypt((String) row.filename, new Date(), stream, out)
                }
            } finally {
                stream.close()
            }
            if( pgp.isOverwrite() ) {
                File f = row?.file as File
                f?.delete()
            }
            row.file = encryptedTemp
            row.filename = encryptedTemp.getName()
            row[streamProperty] = new FileOpenable(encryptedTemp)
            return row
        }
        return this
    }

    /**
     * Decrypts using PGP a stream on the Pipeline and rewrites the stream back onto the Pipeline.  It looks for
     * a stream at the given streamProperty.  Further configuration is performed by the provided Closure
     * that is passed a {@link gratum.pgp.PpgContext}.  You are required to setup the identity passphrase and the secret
     * key collection used to decrypt. It also adds the file and filename properties to the existing row.
     * @param streamProperty The property within the row on the Pipeline that stores a stream.
     * @param configure The closure called with a PgpContext object to further configure how it will decrypt the stream.
     * @return a Pipeline where the streamProperty contains decrypted stream.
     */
    public Pipeline decryptPgp(String streamProperty, Closure configure ) {
        PgpContext pgp = new PgpContext()
        configure.call( pgp )
        addStep("decrypt(${streamProperty})") { Map row ->
            InputStream stream = row[streamProperty] as InputStream
            File decryptedFile = File.createTempFile("pgp-decrypted-output-${streamProperty}", "out")
            try {
                decryptedFile.withOutputStream { OutputStream out ->
                    pgp.decrypt( stream, out )
                }
            } finally {
                stream.close()
            }

            // todo should we get the original file name??!!
            row.file = decryptedFile
            row.filename = decryptedFile.name
            row[streamProperty] = new FileOpenable( decryptedFile )
            return row
        }
    }

    /**
     * Start processing rows from the source of the pipeline.
     */
    public void start() {
        try {
            src?.start(this)

            doneChain.each { AfterStep current ->
                current.execute()
            }
        } catch( HaltPipelineException ex ) {
            // ignore as we were asked to halt.
        }
        complete = true
    }

    /**
     * Starts processing the rows returned from the {@link gratum.source.Source} into the Pipeline.  When the
     * {@link gratum.source.Source} is finished this method returns the {@link gratum.etl.LoadStatistic} for
     * this Pipeline.
     *
     * @return The LoadStatistic instance from this Pipeline.
     */
    public LoadStatistic go() {
        long s = System.currentTimeMillis()
        start()
        long e = System.currentTimeMillis()
        return toLoadStatistic(s, e)
    }

    /**
     * This method is used to send rows to the Pipeline for processing.  Each row passed will start at the first step of
     * the Pipeline and proceed through each step.
     * @param row The row to be processed by the Pipeline's steps.
     * @param lineNumber The lineNumber from the {@link gratum.source.Source} to use when tracking this row through the Pipeline
     */
    public boolean process(Map row, int lineNumber = -1) {
        Map<String,Object> next = row
        for (Step step : processChain) {
            try {
                next = step.execute( this, next, lineNumber )
                if( next[REJECTED_KEY] ) return false
            } catch( HaltPipelineException ex ) {
                throw ex
            } catch (Exception ex) {
                throw new RuntimeException("Line ${(lineNumber > 0 ? lineNumber : row)}: Error encountered in step ${name}.${step.name}", ex)
            }
        }
        if( loaded > DO_NOT_TRACK ) loaded++
        return false // don't stop!
    }

    protected void doRejections(Map<String,Object> current, String stepName, int lineNumber) {
        if( parent ) {
            parent.doRejections( current, stepName, lineNumber )
        } else {
            Rejection rejection = (Rejection)current[REJECTED_KEY]
            rejection.step = stepName
            rejections?.process(current, lineNumber)
        }
    }

    private String keyOf( Map row, List<String> columns ) {
        return columns.collect { key -> row[key] }.join(":")
    }

    /**
     * Helper method to create a {@link Rejection} object.
     * @param reason A text explanation for what caused the rejection
     * @param category The rejection category to group this specific rejection
     * @return
     */
    public static Rejection reject( String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        return new Rejection( reason, category )
    }

    public static Map reject(Map row, String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        row[ REJECTED_KEY ] = reject( reason, category )
        return row
    }

    LoadStatistic toLoadStatistic(long start, long end) {
        LoadStatistic stat
        if( parent ) {
            stat = parent.toLoadStatistic(start, end)
        } else {
            stat = new LoadStatistic(name: name, start: start, end: end)
        }

        for( Step s : processChain ) {
            s.rejections.each { RejectionCategory cat, Integer count ->
                stat.addRejection( cat, s.name, count )
            }

            stat.addTiming(s.name, s.duration )
        }

        if( doneChain ) {
            stat.addTiming("${name}.after", (Long)doneChain.sum() {it.duration } )
        }

        if( loaded > DO_NOT_TRACK ) stat.loaded = loaded
        return stat
    }
}
