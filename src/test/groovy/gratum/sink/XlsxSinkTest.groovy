package gratum.sink

import gratum.etl.LoadStatistic
import gratum.source.CollectionSource
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.time.LocalDate

class XlsxSinkTest {

    @TempDir
    File tempFolder

    @Test
    void testXlsxSink() {
        File output = new File(tempFolder, "purchases.xlsx")

        LoadStatistic stat = CollectionSource.from([
                name: 'Toaster',
                color: 'red',
                count: 1,
                cost: 23.99,
                purchasedOn: LocalDate.of(2021, 3, 15)
        ], [
                name: 'Wine Glasses',
                color: 'green',
                count: 6,
                cost: 13.99,
                purchasedOn: LocalDate.of(2021, 10, 25)

        ], [
                name: 'Dinner Plate',
                color: 'blue',
                count: 6,
                cost: 35.99,
                purchasedOn: LocalDate.of(2021, 11, 3)

        ], [
                name: 'Knife',
                color: 'black',
                count: 1,
                cost:33.99,
                purchasedOn: LocalDate.of(2022, 1, 11)
        ]).save(new XlsxSink( output ) )
        .go()

        assert stat.loaded == 4
        assert output.exists()
        assert output.length() > 0
    }
}
