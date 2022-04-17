package gratum.source

import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.ss.usermodel.DateUtil

import java.text.DecimalFormat
import java.text.SimpleDateFormat


class CustomDateDataFormatter extends DataFormatter {

    private String pattern

    CustomDateDataFormatter(String pattern) {
        this.pattern = pattern
    }

    @Override
    public String formatRawCellContents(double value,
                                        int formatIndex,
                                        String formatString,
                                        boolean use1904Windowing) {

        // Is it a date?
        if (DateUtil.isADateFormat(formatIndex, formatString)) {
            if (DateUtil.isValidExcelDate(value)) {
                Date d = DateUtil.getJavaDate(value, use1904Windowing)
                try {
                    return new SimpleDateFormat(pattern).format(d)
                } catch (Exception e) {
                    LOG.error("Bad date value in Excel: " + d, e)
                }
            }
        }
        return new DecimalFormat("##0.#####").format(value)
    }
}
