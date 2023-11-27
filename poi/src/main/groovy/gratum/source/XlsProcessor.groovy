package gratum.source

import gratum.etl.Pipeline
import groovy.transform.CompileStatic
import org.apache.poi.hssf.eventusermodel.EventWorkbookBuilder
import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory
import org.apache.poi.hssf.eventusermodel.HSSFListener
import org.apache.poi.hssf.eventusermodel.HSSFRequest
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord
import org.apache.poi.hssf.model.HSSFFormulaParser
import org.apache.poi.hssf.record.BOFRecord
import org.apache.poi.hssf.record.BlankRecord
import org.apache.poi.hssf.record.BoolErrRecord
import org.apache.poi.hssf.record.BoundSheetRecord
import org.apache.poi.hssf.record.CellValueRecordInterface
import org.apache.poi.hssf.record.FormulaRecord
import org.apache.poi.hssf.record.LabelRecord
import org.apache.poi.hssf.record.LabelSSTRecord
import org.apache.poi.hssf.record.NoteRecord
import org.apache.poi.hssf.record.NumberRecord
import org.apache.poi.hssf.record.RKRecord
import org.apache.poi.hssf.record.Record
import org.apache.poi.hssf.record.SSTRecord
import org.apache.poi.hssf.record.StringRecord
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.poifs.filesystem.POIFSFileSystem
import org.apache.poi.ss.formula.constant.ErrorConstant

@CompileStatic
class XlsProcessor implements HSSFListener {
    private Pipeline pipeline
    private int startOnRow = 0
    private String sheet

    private EventWorkbookBuilder.SheetRecordCollectingListener workbookBuildingListener
    private SSTRecord sstRecord
    private FormatTrackingHSSFListener formatListener
    private BoundSheetRecord[] orderedBSRs
    private final List<BoundSheetRecord> boundSheetRecords = new ArrayList<>()
    private HSSFWorkbook stubWorkbook

    // For handling formulas with string results
    private int nextRow
    private int nextColumn
    private boolean outputNextStringRecord

    private List<String> header
    private Map<String,Object> row = [:]
    boolean outputFormulaValues = true
    private int sheetIndex = -1

    XlsProcessor(Pipeline pipeline, Integer startOnRow = 0, Boolean outputFormulaValues = true, String sheet = null) {
        this.pipeline = pipeline
        this.startOnRow = startOnRow
        this.outputFormulaValues = outputFormulaValues
        this.sheet = sheet
    }

    void parse(POIFSFileSystem fs) {
        MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this)
        this.formatListener = new FormatTrackingHSSFListener(listener)

        HSSFEventFactory factory = new HSSFEventFactory()
        HSSFRequest request = new HSSFRequest()

        if(outputFormulaValues) {
            request.addListenerForAllRecords(formatListener)
        } else {
            this.workbookBuildingListener = new EventWorkbookBuilder.SheetRecordCollectingListener(formatListener)
            request.addListenerForAllRecords(workbookBuildingListener)
        }

        factory.processWorkbookEvents(request, fs)
    }

    @Override
    void processRecord(Record record) {
        if( record instanceof CellValueRecordInterface ) {
            CellValueRecordInterface cellRecord = (CellValueRecordInterface)record
            if( cellRecord.getRow() < startOnRow ) {
                return
            }
        }
        if( record instanceof LastCellOfRowDummyRecord ) {
            LastCellOfRowDummyRecord cellRecord = (LastCellOfRowDummyRecord)record
            if( cellRecord.getRow() < startOnRow ) {
                return
            }
        }

        if( orderedBSRs && sheet && orderedBSRs[sheetIndex].getSheetname() != sheet ) {
            return // skip sheets we're not interested in
        }

        int thisRow = -1
        int thisColumn = -1

        switch (record.getSid())
        {
            case BoundSheetRecord.sid:
                boundSheetRecords.add((BoundSheetRecord)record)
                break
            case BOFRecord.sid:
                BOFRecord br = (BOFRecord)record
                if(br.getType() == BOFRecord.TYPE_WORKSHEET) {
                    // Create sub workbook if required
                    if(workbookBuildingListener != null && stubWorkbook == null) {
                        stubWorkbook = workbookBuildingListener.getStubHSSFWorkbook()
                    }

                    // Output the worksheet name
                    // Works by ordering the BSRs by the location of
                    //  their BOFRecords, and then knowing that we
                    //  process BOFRecords in byte offset order
                    sheetIndex++
                    header = []
                    if(orderedBSRs == null) {
                        orderedBSRs = BoundSheetRecord.orderByBofPosition(boundSheetRecords)
                    }
                }
                break

            case SSTRecord.sid:
                sstRecord = (SSTRecord) record
                break

            case BlankRecord.sid:
                BlankRecord brec = (BlankRecord) record

                thisRow = brec.getRow()
                thisColumn = brec.getColumn()
                addToCurrentRow( null, thisRow, thisColumn )

                break
            case BoolErrRecord.sid:
                BoolErrRecord berec = (BoolErrRecord) record

                thisRow = berec.getRow()
                thisColumn = berec.getColumn()
                String value = berec.isBoolean() ? berec.getBooleanValue() : "ERROR:${ErrorConstant.valueOf(berec.getErrorValue()).getText()}"
                addToCurrentRow( value, thisRow, thisColumn )
                break

            case FormulaRecord.sid:
                FormulaRecord frec = (FormulaRecord) record

                thisRow = frec.getRow()
                thisColumn = frec.getColumn()

                if(outputFormulaValues) {
                    if(Double.isNaN( frec.getValue() )) {
                        // Formula result is a string
                        // This is stored in the next record
                        outputNextStringRecord = true
                        nextRow = frec.getRow()
                        nextColumn = frec.getColumn()
                    } else {
                        // todo convert to a date instead of string
                        addToCurrentRow( formatListener.formatNumberDateCell(frec), thisRow, thisColumn )
                    }
                } else {
                    String formula = HSSFFormulaParser.toFormulaString(stubWorkbook, frec.getParsedExpression())
                    addToCurrentRow( formula, thisRow, thisColumn )
                }
                break
            case StringRecord.sid:
                if(outputNextStringRecord) {
                    // String for formula
                    StringRecord srec = (StringRecord)record
                    addToCurrentRow( srec.getString(), nextRow, nextColumn )
                    thisRow = nextRow
                    thisColumn = nextColumn
                    outputNextStringRecord = false
                }
                break

            case LabelRecord.sid:
                LabelRecord lrec = (LabelRecord) record

                thisRow = lrec.getRow()
                thisColumn = lrec.getColumn()
                addToCurrentRow( lrec.getValue(), thisRow, thisColumn )
                break
            case LabelSSTRecord.sid:
                LabelSSTRecord lsrec = (LabelSSTRecord) record

                thisRow = lsrec.getRow()
                thisColumn = lsrec.getColumn()
                if(sstRecord == null) {
                    addToCurrentRow( "(No SST Record, can't identify string)", thisRow, thisColumn )
                } else {
                    addToCurrentRow( sstRecord.getString(lsrec.getSSTIndex()).toString(), thisRow, thisColumn )
                }
                break
            case NoteRecord.sid:
                NoteRecord nrec = (NoteRecord) record

                thisRow = nrec.getRow()
                thisColumn = nrec.getColumn()
                // TODO: Find object to match nrec.getShapeId()
//                String note = sstRecord.getString( nrec.getShapeId() )
//                addToCurrentRow( note, thisRow, thisColumn )
                break
            case NumberRecord.sid:
                NumberRecord numrec = (NumberRecord) record

                thisRow = numrec.getRow()
                thisColumn = numrec.getColumn()

                // Format
                addToCurrentRow( formatListener.formatNumberDateCell(numrec), thisRow, thisColumn )
                break
            case RKRecord.sid:
                RKRecord rkrec = (RKRecord) record

                thisRow = rkrec.getRow()
                thisColumn = rkrec.getColumn()
                addToCurrentRow( rkrec.getRKNumber(), thisRow, thisColumn )
                break
            default:
                break
        }

        if(record instanceof MissingCellDummyRecord) {
            // Handle missing column
            MissingCellDummyRecord mc = (MissingCellDummyRecord)record
            thisRow = mc.getRow()
            thisColumn = mc.getColumn()
            addToCurrentRow( null, thisRow, thisColumn )
        }

        if(record instanceof LastCellOfRowDummyRecord) {
            thisRow = ((LastCellOfRowDummyRecord)record).getRow()
            if( thisRow > startOnRow ) {
                // Handle end of row
                pipeline.process( row, thisRow )
                row = [:]
            }
        }
    }

    void addToCurrentRow(Object value, int row, int col) {
        if( row == startOnRow ) {
            this.header.add( value.toString() )
        } else {
            String key = col < this.header.size() ? header.get(col) : "col_${col}"
            this.row[ key ] = value
        }
    }
}
