import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicIntegerArray

class ParallelQuickSort(private val blockSize: Int = 100000, private val seqPartition: Boolean = false) :
    AbstractQuickSort() {
    @OptIn(DelicateCoroutinesApi::class)
    private val scope = CoroutineScope(newFixedThreadPoolContext(4, ""))
    private val sequentialQuickSort = SequentialQuickSort()

    private fun swap(array: IntArray, x: Int, y: Int) {
        val t = array[x]
        array[x] = array[y]
        array[y] = t
    }

    private suspend fun fork2Join(block1: suspend () -> Unit, block2: suspend () -> Unit) {
//        block1()
//        block2()
        val t1 = scope.launch { block1() }
        val t2 = scope.launch { block2() }
        t1.join()
        t2.join()
    }

    private suspend fun parallelFor(l: Int, r: Int, blockSize: Int, block: suspend (i: Int) -> Unit) {
        if (l + blockSize >= r) {
            for (i in l..<r) {
                block(i)
            }
            return
        }
        val m = (l + r) / 2
        fork2Join({ parallelFor(l, m, blockSize, block) }, { parallelFor(m, r, blockSize, block) })
    }

    private suspend fun partitionFilter(
        array: AtomicIntegerArray,
        l: Int,
        r: Int,
        v: Int,
        mode: Boolean,
        result: AtomicIntegerArray,
        offset: Int = 0
    ): Int {
        val pow2size = (r - l - 1).takeHighestOneBit() * 2
        val mask = AtomicIntegerArray(pow2size)
        parallelFor(l, r, 4) { i -> mask[i - l] = if ((array[i] < v) == mode) 1 else 0 }

        val reduced = AtomicIntegerArray(pow2size * 2 - 1)
        reduceImpl(mask, 0, pow2size, 0, reduced, 4)
        val scanned = AtomicIntegerArray(pow2size)
        scanPropagate(reduced, 0, pow2size, 0, 0, scanned, 4)

        parallelFor(0, r - l, 4) { i ->
            if (mask[i] == 1) {
                result[offset + scanned[i] - 1] = array[l + i]
            }
        }

        return scanned.get(scanned.length() - 1)
    }

    private suspend fun reduceImpl(
        array: AtomicIntegerArray,
        l: Int,
        r: Int,
        x: Int,
        reduced: AtomicIntegerArray,
        blockSize: Int = 4048
    ) {
        if (l + 1 == r) {
            reduced[x] = array[l]
            return
        }
        val m = (l + r) / 2
        if (l + blockSize >= r) {
            reduceImpl(array, l, m, x * 2 + 1, reduced, blockSize)
            reduceImpl(array, m, r, x * 2 + 2, reduced, blockSize)
        } else {
            fork2Join({
                reduceImpl(array, l, m, x * 2 + 1, reduced, blockSize)
            }, {
                reduceImpl(array, m, r, x * 2 + 2, reduced, blockSize)
            })
        }
        reduced[x] = reduced[x * 2 + 1] + reduced[x * 2 + 2]
    }

    private suspend fun scanPropagate(
        array: AtomicIntegerArray,
        l: Int,
        r: Int,
        x: Int,
        fromLeft: Int,
        result: AtomicIntegerArray,
        blockSize: Int = 4048
    ) {
        if (l + 1 == r) {
            result[l] = array[x] + fromLeft
            return
        }
        val m = (l + r) / 2
        if (l + blockSize >= r) {
            scanPropagate(array, l, m, x * 2 + 1, fromLeft, result, blockSize)
            scanPropagate(array, m, r, x * 2 + 2, fromLeft + array[x * 2 + 1], result, blockSize)
        } else {
            fork2Join({
                scanPropagate(array, l, m, x * 2 + 1, fromLeft, result, blockSize)
            }, {
                scanPropagate(array, m, r, x * 2 + 2, fromLeft + array[x * 2 + 1], result, blockSize)
            })
        }
    }

    private fun partition(array: IntArray, l: Int, r: Int, v: Int): Int {
        var i = l
        var j = r - 1
        while (i <= j) {
            while (array[i] < v) {
                i++
            }
            while (array[j] > v) {
                j--
            }
            if (i >= j) {
                break
            }
            swap(array, i, j)
            i++
            j--
        }
        return j
    }

    private suspend fun sort(array: IntArray, l: Int, r: Int) {
        if (l + blockSize >= r) {
            sequentialQuickSort.sort(array, l, r)
            return
        }

        val middle = choosePartition(l, r)

        if (!seqPartition) {
            val partitionResult = AtomicIntegerArray(r - l)
            val sp = partitionFilter(AtomicIntegerArray(array), l, r, array[middle], true, partitionResult)
            partitionFilter(AtomicIntegerArray(array), l, r, array[middle], false, partitionResult, sp)
            parallelFor(0, r - l, 1) { i -> array[i + l] = partitionResult[i] }
            fork2Join({ sort(array, l, sp) }, { sort(array, sp, r) })
        } else {
            val s = partition(array, l, r, array[middle])
            fork2Join({ sort(array, l, s + 1) }, { sort(array, s + 1, r) })
        }
    }

    override suspend fun sort(array: IntArray) {
        sort(array, 0, array.size)
    }
}
