import kotlinx.coroutines.runBlocking
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class QuickSortBenchmark(private val arraySize: Int, private val pqsBlockSize: Int = 16384) {
    fun benchmark(repeatCount: Int) {
        val parallelQuickSort = ParallelQuickSort(pqsBlockSize, true)
        val sequentialQuickSort = SequentialQuickSort()
        val array = generateArray(arraySize)
        val seqTime = (0..<repeatCount).map { testImplementation(array, sequentialQuickSort) }.average()
        val parallelTime =
            (0..<repeatCount).map { testImplementation(array, parallelQuickSort) }.average()

        println("Sequential time is $seqTime ms")
        println("Parallel time is $parallelTime ms")
        println("Speed up for ${seqTime / parallelTime}")
//            bs *= 2
//            break
//        }
    }

    private fun testImplementation(array: IntArray, qs: AbstractQuickSort): Long {
        val testArray = array.copyOf(array.size)
        return runBlocking {
            val startTime = Instant.now()
            qs.sort(testArray)
            val endTime = Instant.now()
            val validSortedArray = array.copyOf(array.size)
            Arrays.sort(validSortedArray)
            if (!testArray.contentEquals(validSortedArray)) {
                throw AssertionError("array from " + qs + " is n't sorted: " + testArray.contentToString() + " sorted: " + validSortedArray.contentToString())
            }
            startTime.until(endTime, ChronoUnit.MILLIS)
        }
    }

    private fun generateArray(length: Int): IntArray {
        val array = IntArray(length)
        for (i in 0..<length) {
            array[i] = random.nextInt()
        }
        return array
    }

    private val random = Random(333)
}

fun main() {
    QuickSortBenchmark(100000000).benchmark(3)
}
