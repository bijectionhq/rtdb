package xtdb.trie;

final class LeafMerge {
    private LeafMerge() {}

    interface RowOrder {
        //@ public model instance \datagroup scratch;
        //@ public model no_state \bigint rank(int row);

        /*@ public normal_behavior
          @ assignable scratch;
          @ ensures (\result < 0) <==> (rank(left) < rank(right));
          @ ensures (\result == 0) <==> (rank(left) == rank(right));
          @*/
        int compare(int left, int right);
    }

    /*@ normal_behavior
      @ requires left.length + right.length <= Integer.MAX_VALUE;
      @ requires (\forall int i, j; 0 <= i && i < j && j < left.length; order.rank(left[i]) <= order.rank(left[j]));
      @ requires (\forall int i, j; 0 <= i && i < j && j < right.length; order.rank(right[i]) <= order.rank(right[j]));
      @ assignable order.scratch;
      @ ensures (\forall int i; 0 <= i && i < left.length; left[i] == \old(left[i]));
      @ ensures (\forall int i; 0 <= i && i < right.length; right[i] == \old(right[i]));
      @ ensures \fresh(\result);
      @ ensures \result.length == left.length + right.length;
      @ ensures (\forall int i, j; 0 <= i && i < j && j < \result.length; order.rank(\result[i]) <= order.rank(\result[j]));
      @ ensures (\exists int[] fromLeft, fromRight, origins;
      @       fromLeft != null && fromRight != null && origins != null
      @       && fromLeft.length == left.length && fromRight.length == right.length
      @       && origins.length == \result.length;
      @       (\forall int i; 0 <= i && i < left.length;
      @          0 <= fromLeft[i] && fromLeft[i] < \result.length
      @          && \result[fromLeft[i]] == left[i] && origins[fromLeft[i]] == i)
      @    && (\forall int j; 0 <= j && j < right.length;
      @          0 <= fromRight[j] && fromRight[j] < \result.length
      @          && \result[fromRight[j]] == right[j] && origins[fromRight[j]] == -1-j)
      @    && (\forall int out; 0 <= out && out < \result.length;
      @           (origins[out] >= 0 && origins[out] < left.length && fromLeft[origins[out]] == out)
      @        || (origins[out] < 0 && -1-origins[out] < right.length && fromRight[-1-origins[out]] == out)));
      @*/
    static int[] merge(int[] left, int[] right, RowOrder order) {
        int[] merged = new int[left.length + right.length];
        //@ ghost int[] fromLeft = new int[left.length];
        //@ ghost int[] fromRight = new int[right.length];
        //@ ghost int[] origins = new int[merged.length];
        int leftIndex = 0;
        int rightIndex = 0;
        int next = 0;
        /*@ loop_invariant 0 <= leftIndex && leftIndex <= left.length && 0 <= rightIndex && rightIndex <= right.length;
          @ loop_invariant next == leftIndex + rightIndex && next <= merged.length;
          @ loop_invariant fromLeft.length == left.length && fromRight.length == right.length;
          @ loop_invariant origins.length == merged.length;
          @ loop_invariant fromLeft != fromRight && fromLeft != origins && fromRight != origins;
          @ loop_invariant merged != left && merged != right && merged != origins;
          @ loop_invariant fromLeft != merged && fromLeft != left && fromLeft != right;
          @ loop_invariant fromRight != merged && fromRight != left && fromRight != right;
          @ loop_invariant origins != left && origins != right;
          @ loop_invariant (\forall int i, j; 0 <= i && i < j && j < next;
          @                       order.rank(merged[i]) <= order.rank(merged[j]));
          @ loop_invariant (\forall int i, j; 0 <= i && i < next && leftIndex <= j && j < left.length;
          @                       order.rank(merged[i]) <= order.rank(left[j]));
          @ loop_invariant (\forall int i, j; 0 <= i && i < next && rightIndex <= j && j < right.length;
          @                       order.rank(merged[i]) <= order.rank(right[j]));
          @ loop_invariant (\forall int i; 0 <= i && i < leftIndex;
          @                       0 <= fromLeft[i] && fromLeft[i] < next
          @                       && merged[fromLeft[i]] == left[i] && origins[fromLeft[i]] == i);
          @ loop_invariant (\forall int j; 0 <= j && j < rightIndex;
          @                       0 <= fromRight[j] && fromRight[j] < next
          @                       && merged[fromRight[j]] == right[j] && origins[fromRight[j]] == -1-j);
          @ loop_invariant (\forall int out; 0 <= out && out < next;
          @                         (origins[out] >= 0 && origins[out] < leftIndex && fromLeft[origins[out]] == out)
          @                      || (origins[out] < 0 && -1-origins[out] < rightIndex && fromRight[-1-origins[out]] == out));
          @ loop_writes leftIndex, rightIndex, next, merged[*], fromLeft[*], fromRight[*], origins[*], order.scratch;
          @ decreases merged.length - next;
          @*/
        //@ split;
        while (next < merged.length) {
            //@ split;
            if (rightIndex == right.length || leftIndex < left.length && order.compare(left[leftIndex], right[rightIndex]) < 0) {
                //@ assert rightIndex == right.length || order.rank(left[leftIndex]) < order.rank(right[rightIndex]);
                //@ assert (\forall int j; rightIndex <= j && j < right.length; order.rank(left[leftIndex]) <= order.rank(right[j]));
                //@ assert (\forall int j; leftIndex < j && j < left.length; order.rank(left[leftIndex]) <= order.rank(left[j]));
                //@ assert (\forall int j; 0 <= j && j < next; order.rank(merged[j]) <= order.rank(left[leftIndex]));
                merged[next] = left[leftIndex];
                //@ set fromLeft[leftIndex] = next;
                //@ set origins[next] = leftIndex;
                leftIndex++;
            } else {
                //@ assert leftIndex == left.length || order.rank(right[rightIndex]) <= order.rank(left[leftIndex]);
                //@ assert (\forall int j; leftIndex <= j && j < left.length; order.rank(right[rightIndex]) <= order.rank(left[j]));
                //@ assert (\forall int j; rightIndex < j && j < right.length; order.rank(right[rightIndex]) <= order.rank(right[j]));
                //@ assert (\forall int j; 0 <= j && j < next; order.rank(merged[j]) <= order.rank(right[rightIndex]));
                merged[next] = right[rightIndex];
                //@ set fromRight[rightIndex] = next;
                //@ set origins[next] = -1-rightIndex;
                rightIndex++;
            }
            next++;
        }
        //@ assert leftIndex == left.length && rightIndex == right.length;
        return merged;
    }
}
