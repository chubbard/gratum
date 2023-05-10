package gratum.dsl

import org.junit.Test

import static gratum.dsl.Dsl.*

class DslTest {

    @Test
    void testHeaderProcessing() {
        boolean headerCalled = false
        GroupDsl output = group {
            pipeline {
                csv(new File("src/test/resources/titanic.csv") ) {
                    header { List<String> header ->
                        headerCalled = true
                        assert header.size() == 11
                    }
                }
                trim()
                limit(0)
            }
        }

        assert output.results.size() > 0
        assert headerCalled
    }
}
