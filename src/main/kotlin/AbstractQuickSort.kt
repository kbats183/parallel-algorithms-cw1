import java.util.Random

abstract class AbstractQuickSort {
    abstract suspend fun sort(array: IntArray)

    protected fun choosePartition(l: Int, r: Int): Int {
        return l + random.nextInt(r - l)
    }

    private val random = Random(9)
}
