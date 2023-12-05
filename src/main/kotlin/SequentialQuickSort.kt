class SequentialQuickSort : AbstractQuickSort() {
    override suspend fun sort(array: IntArray) {
        sort(array, 0, array.size)
    }

    private fun partition(array: IntArray?, l: Int, r: Int, v: Int): Int {
        var i = l
        var j = r - 1
        while (i <= j) {
            while (array!![i] < v) {
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

    private fun swap(array: IntArray, x: Int, y: Int) {
        val t = array[x]
        array[x] = array[y]
        array[y] = t
    }

    fun sort(array: IntArray, l: Int, r: Int) {
        if (l + 1 >= r) {
            return
        } else if (l + 2 == r) {
            if (array[l] > array[l + 1]) {
                swap(array, l, l + 1)
            }
            return
        }
        val middle = choosePartition(l, r)
        val s = partition(array, l, r, array[middle])
        sort(array, l, s + 1)
        sort(array, s + 1, r)
    }
}
