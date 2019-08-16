package gratum.operators

import gratum.csv.CSVFile
import gratum.etl.Pipeline
import gratum.etl.PipelineOutput
import gratum.source.SingleSource

import static gratum.operators.DataTypesOperator.*

class Operators {

    /**
     * Sets a fieldName in each row to the given value.
     * @param fieldName The new field name to add
     * @param value the value of the new field name
     * @return An operator where each row has a fieldname set to the given value
     */
    public static set( String fieldName, Object value ) {
        return new SetFieldOperator( fieldName, value );
    }

    public static addField( String fieldName, Closure<Map<String,Object>> fieldValue ) {
        return new AddFieldOperator( fieldName, fieldValue )
    }

    public static Operator<Map,Map> rename( Map<String,String> fieldNames ) {
        return new RenameOperator( fieldNames );
    }

    public static <T> Operator<T,T> branch( String name, Closure<Void> split ) {
        return new BranchOperator<T>(name, null, split)
    }

    public static Operator<Map,Map> branch( String name, Map<String,Object> condition, Closure<Void> split) {
        return new BranchOperator<Map>( null, condition, split )
    }

    public static Pipeline<Map<String,Object>> csv( String filename, String separator = ",") {
        File file = new File( filename )
        return SingleSource.of( (Reader)file.newReader(), filename )
                .add( new CsvLoadOperator(separator) )
    }

    static Operator<Map, PipelineOutput<Map>> saveAsCsv(String filename, String separator ) {
        CSVFile out = new CSVFile( filename, separator )
        return new CsvSaveOperator( out )
    }

    static Operator<Map,PipelineOutput<Map>> saveAsCsv(String filename, String separator, String... columns) {
        CSVFile out = new CSVFile( filename, separator )
        return new CsvSaveOperator( out, columns )
    }

    /**
     * Returns a Pipeline where the given column is coverted from a string to a java.lang.Double.
     * @param column The name of the column to convert into a Double
     * @return An Operator where all rows contains a java.lang.Double at the given column
     */
    public static Operator<Map,Map> asDouble(String fieldName) {
        return convert( "Double", fieldName ) { String value ->
            Double.parseDouble(value)
        }
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Integer value.
     * @param column containing a string to be turned into a java.lang.Integer
     * @return An Operator where all rows contain a java.lang.Integer at given column
     */
    public static Operator<Map,Map> asInt(String fieldName) {
        return convert( "Integer", fieldName ) { String value ->
            Integer.parseInt(value)
        }
    }

    /**
     * Parses the string value at given fieldname into a java.lang.Boolean value.  It understands values like: Y/N, YES/NO, TRUE/FALSE, 1/0, T/F.
     * @param column containing a string to be turned into a java.lang.Boolean
     * @return An Operator where all rows contain a java.lang.Boolean at given column
     */
    public static Operator<Map,Map> asBoolean(String fieldName) {
        return convert( "Boolean", fieldName ) { String value ->
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
                        return true
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
                        return false
                    default:
                        return Boolean.parseBoolean(value)
                }
            }
            return null
        }
    }

    /**
     * Parses the string at the given column name into a Date object using the given format.  Any value not
     * parseable by the format is rejected.
     * @param column The field to use to find the string value to parse
     * @param format The format of the string to use to parse into a java.util.Date
     * @return An Operator where all rows contain a java.util.Date at given field name
     */
    public static Operator<Map,Map> asDate(String column, String format = "yyyy-MM-dd") {
        return new DateOperator( column, format )
    }

    public static <Src,Dest> Operator<Src,Dest> exchange( String name, Closure<Pipeline<Dest>> closure  ) {
        return new ExchangeOperator<Src,Dest>( name, closure )
    }

    public static Operator<Map,Map> fillDownBy( Closure<Boolean> decider ) {
        return new FillDownOperator( decider )
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
    public static Operator<Map,Map> filterFields(Map<String,Object> fields ) {
        return new FilterFieldsOperator( fields )
    }

    public static <T> Operator<T,T> filter( Closure<Boolean> callback ) {
        return new FilterOperator<T>( callback )
    }

    /**
     * Returns a Pipeline where the row is grouped by the given columns.  The resulting Pipeline will only
     * return a single row where the keys of that row will be the first column passed to the groupBy() method.
     * All other columns given will occur under the respective keys.  This yields a tree like structure where
     * the height of the tree is equal to the columns.length.  In the leaves of the tree are the rows that
     * matched all of their parents.
     *
     * @param columns The columns to group each row by.
     * @return A Operator that yields a single row that represents the tree grouped by the given columns.
     */
    public static Operator groupBy( String... columns ) {
        return new GroupByOperator( columns )
    }

    public static <T> Operator<T,T> inject( String name, Closure<Collection<T>> inject ) {
        return new InjectOperator( name, inject )
    }

    public static Operator<Map,Map> intersect( Pipeline<Map> pipeline, def columns ) {
        return new IntersectOperator( pipeline, columns )
    }

    public static join(Pipeline<Map> other, def columns, boolean left = false ) {
        return new JoinOperator(other, columns,left)
    }

    /**
     * Concatentates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
     * rows from this pipeline and the src pipeline.
     *
     * @param source The pipeline
     * @return Returns a new pipeline that combines all of the rows from this pipeline and the src pipeline.
     */
    public static <T> Operator<T,T> concat(Pipeline<T> source ) {
        return new ConcatOperator(source)
    }

    public static Operator<Map,Map> printRow(String... columns) {
        return new PrintRowOperator(columns)
    }

    public static Operator<Map,Map> sort( String... columns ) {
        return new SortOperator( columns )
    }

    /**
     * Returns a Pipeline where all white space is removed from all columns contained within the rows.
     *
     * @return Operator where all rows has white space removed.
     */
    public static Operator<Map,Map> trim() {
        return new TrimOperator()
    }

    /**
     * Only allows rows that are unique per the given column.
     *
     * @param column The column name to use for checking uniqueness
     * @return A Pipeline that only contains the unique rows for the given column
     */
    public static Operator unique(String column) {
        return new UniqueOperator( column )
    }
}
