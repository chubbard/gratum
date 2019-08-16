package gratum.etl

import gratum.operators.Operator
import gratum.operators.Operators

import gratum.source.Source

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
 * into the down stream steps which uses printRow to print it to the console.  The input would look like
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
public class Pipeline<T> implements Source {

    LoadStatistic statistic
    Source src
    List<Step> processChain = []
    List<Closure> doneChain = []
    Pipeline<Rejection> rejections
    boolean complete = false

    Rejection lastRejection

    Pipeline(String name) {
        this.statistic = new LoadStatistic([name: name])
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
    public Pipeline addStep( String name = null, Closure<T> step ) {
        step.delegate = this
        processChain << new Step( name, step )
        return this
    }

    public Pipeline addStep(GString name, Closure<T> step) {
        return this.addStep( name.toString(), step )
    }

    public <Dest> Pipeline<Dest> add(Operator<T,Dest> operator) {
        return operator.attach( this )
    }

    public <Dest> Pipeline<Dest> rightShift( Operator<T,Dest> operator ) {
        return add( operator )
    }

    public Pipeline<T> rightShift( Closure<T> closure ) {
        return addStep( "addStep()", closure )
    }

    /**
     * Adds a closure to the end of the Pipeline.  This is called after all rows are processed.  This closure is
     * invoked without any arguments.
     *
     * @param step the Closure that is invoked after all rows have been processed.
     * @return this Pipeline.
     */
    public Pipeline<T> after( Closure<Void> step ) {
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
    public Pipeline<Rejection> onRejection( Closure<Void> branch ) {
        initRejections()
        branch( rejections )
        after {
            rejections.doneChain.each { Closure c ->
                c()
            }
            return
        }
        return this
    }

    private void initRejections() {
        if (!rejections) {
            rejections = new Pipeline<Rejection>("Rejections(${name})")
        }
    }

    /**
     * Concatentates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
     * rows from this pipeline and the src pipeline.
     *
     * @param src The pipeline
     * @return Returns a new pipeline that combines all of the rows from this pipeline and the src pipeline.
     */
    public Pipeline<T> concat( Pipeline<T> src ) {
        return this >> Operators.concat( src )
    }

    /**
     * Returns a Pipeline where all white space is removed from all columns contained within the rows.
     *
     * @return Pipeline where all rows has white space removed.
     */
    public Pipeline<Map> trim() {
        return this >> Operators.trim()
    }

    /**
     * Renames a row's columns in the given map to the values of the corresponding keys.
     *
     * @param fieldNames The Map of src column to renamed names.
     * @return A Pipeline where all of the columns in the keys of the Map are renamed to the Map's corresponding values.
     */
    public Pipeline<Map> renameFields( Map fieldNames ) {
        return this >> Operators.rename( fieldNames )
    }

    /**
     * Sets a fieldName in each row to the given value.
     * @param fieldName The new field name to add
     * @param value the value of the new field name
     * @return The Pipeline where each row has a fieldname set to the given value
     */
    public Pipeline<Map> setField(String fieldName, Object value ) {
        return this >> Operators.set( fieldName, value )
    }

    /**
     * Adds a new field to each row with the value returned by the given closure.
     * @param fieldName The new field name to add
     * @param fieldValue The closure that returns a value to set the given field's name to.
     * @return The Pipeline where the fieldname exists in every row
     */
    public Pipeline<Map> addField(String fieldName, Closure fieldValue) {
        return this >> Operators.addField( fieldName, fieldValue )
    }

    /**
     * Only allows rows that are unique per the given column.
     *
     * @param column The column name to use for checking uniqueness
     * @return A Pipeline that only contains the unique rows for the given column
     */
    Pipeline<T> unique(String column) {
        return this >> Operators.unique( column )
    }

    /**
     * Returns a Pipeline where the given column is coverted from a string to a java.lang.Double.
     * @param column The name of the column to convert into a Double
     * @return A Pipeline where all rows contains a java.lang.Double at the given column
     */
    Pipeline<Map> asDouble(String column) {
        return this >> Operators.asDouble( column )
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Integer value.
     * @param column containing a string to be turned into a java.lang.Integer
     * @return A Pipeline where all rows contain a java.lang.Integer at given column
     */
    Pipeline<Map> asInt(String column) {
        return this >> Operators.asInt( column )
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Boolean value.  It understands values like: Y/N, YES/NO, TRUE/FALSE, 1/0, T/F.
     * @param column containing a string to be turned into a java.lang.Boolean
     * @return A Pipeline where all rows contain a java.lang.Boolean at given column
     */
    Pipeline<Map> asBoolean(String column) {
        return this >> Operators.asBoolean( column )
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
    public Pipeline<Map> groupBy( String... columns ) {
        return this >> Operators.groupBy(columns)
    }

    /**
     * This adds a step to the Pipeline that passes all rows where the given closure returns true to the next step
     * on the pipeline.  All rows where the closure returns false are rejected.
     *
     * @param callback A callback that is passed a row, and returns a boolean.  All rows that return a false are rejected.
     * @return A Pipeline that contains only the rows that matched the filter.
     */
    public Pipeline<T> filter(Closure<Boolean> callback) {
        return this >> Operators.filter( callback )
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
    public Pipeline<Map> filter( Map<String,Object> columns ) {
        return this >> Operators.filterFields( columns )
    }

    /**
     * Parses the string at the given column name into a Date object using the given format.  Any value not
     * parseable by the format is rejected.
     * @param column The field to use to find the string value to parse
     * @param format The format of the string to use to parse into a java.util.Date
     * @return A Pipeline where all rows contain a java.util.Date at given field name
     */
    Pipeline<Map> asDate(String column, String format = "yyyy-MM-dd") {
        return this >> Operators.asDate( column, format )
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline that is passed to the given closure.  The given closure
     * can configure additional steps on the branched Pipeline.  The rows passed through this Pipeline are not modified.
     *
     * @param split The closure that is passed a new Pipeline where all the rows from this Pipeline are copied onto.
     * @return this Pipeline
     */
    public Pipeline<T> branch( Closure<Void> split) {
        Operator<T,T> op = Operators.branch( "branch", split )
        return this >> op
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline where the given condition returns is true.  The given
     * condition works the same way {@link #filter(java.util.Map)} does.  This ia combination of branch and filter.
     *
     * @param condition The conditions that must be equal in order for the row to be copied to the branch.
     * @param split The closure that is passed the branch Pipeline.
     * @return this Pipeline
     */
    public Pipeline<Map> branch(Map<String,Object> condition, Closure<Void> split) {
        return this >> Operators.branch("branch", condition, split )
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
    public Pipeline<Map> join( Pipeline other, def columns, boolean left = false ) {
        return this >> Operators.join( other, columns, left )
    }

    /**
     * This returns a Pipeline where the rows with empty columns are filled in using the values in the previous row depending on
     * what the given closure returns.  If the closure returns true then any empty column (value == null or value.isEmpty()) will
     * be populated by the values in the previous row.
     *
     * @param decider a Closure which decides if the values from a prior row will be used to fill in missing values in the current row.
     * @return A Pipeline where the row's empty column values are filled in by the previous row.
     */
    public Pipeline<Map> fillDownBy( Closure<Boolean> decider ) {
        return this >> Operators.fillDownBy( decider )
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
    public Pipeline<Map> intersect( Pipeline<Map> other, def columns ) {
        return this >> Operators.intersect( other, columns )
    }

    /**
     * Returns a Pipeline where the rows are ordered by the given columns.  The value of 
     * each column is compared using the <=> operator.
     * @param columns to sort by
     * @return a Pipeline that where it's rows are ordered according to the given columns.
     */
    public Pipeline<Map> sort(String... columns) {
        return this >> Operators.sort( columns )
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
    public <Dest> Pipeline<Dest> exchange(Closure<Pipeline<Dest>> closure) {
        return exchange("exchange()", closure )
    }

    /**
     * This takes a closure that returns a Pipeline which is used to feed the returned Pipeline.  The closure will be called
     * for each row emitted from this Pipeline so the closure could create multiple Pipelines, and all data from every Pipeline
     * will be fed into the returned Pipeline.
     *
     * @param Name of the step for the exchange step.
     * @param closure A closure that takes a Map and returns a pipeline that it's data will be fed on to the returned pipeline.
     *
     * @return A Pipeilne whose reocrds consist of the records from all Pipelines returned from the closure
     */
    public <Dest> Pipeline<Dest> exchange(String name, Closure<Pipeline<Dest>> closure) {
        return this >> Operators.exchange(name, closure)
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
        return this >> Operators.inject( name, closure )
    }

    /**
     * @param closure A closure that
     */
    public Pipeline inject( Closure closure) {
        this.inject("inject()", closure )
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
        return this << Operators.saveAsCsv( filename, separator, columns.toArray( new String[ columns.size() ] ) )
    }

    /**
     * Prints the values of the given columns for each row to the console, or all columns if no columns are given.
     * @param columns The names of the columns to print to the console
     * @return this Pipeline
     */
    public Pipeline printRow(String... columns) {
        return this >> Operators.printRow( columns )
    }

    public Pipeline progress( int col = 50 ) {
        int line = 1
        addStep("progress()") { T row ->
            line++
            printf(".")
            if( line % col ) println()
            row
        }
    }

    void copyStatistics(Pipeline<?> src) {
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

    public void start(Closure closure = null) {
        if( closure ) addStep("tail", closure)

        statistic.start = System.currentTimeMillis()
        int line = 1
        src.start { T row ->
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
    public boolean process(T row, int lineNumber = -1) {
        T current = row instanceof Map ? new LinkedHashMap<String,Object>((Map)row) : row
        for (Step step : processChain) {
            try {
                boolean stop = statistic.timed(step.name) {
                    T ret = step.step(current)
                    if (!ret ) {
                        doRejections( current, step.name, lineNumber )
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

    public void doRejections( T current, String stepName, int lineNumber ) {
        initRejections()
        lastRejection = lastRejection ?: new Rejection<T>("Unknown reason", RejectionCategory.REJECTION, stepName)
        lastRejection.source = current
        lastRejection.step = stepName
        statistic.reject( lastRejection?.category ?: RejectionCategory.REJECTION)
        rejections.process(lastRejection, lineNumber)
    }

    public void reject( String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        lastRejection = new Rejection( reason, category )
    }
  
}
