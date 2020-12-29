package com.clei.algorithm.other;

import com.clei.utils.NumUtil;
import com.clei.utils.PrintUtil;

/**
 * 建网勘测看山高
 * 来源：images/2.png
 *
 * @author KIyA
 */
public class LookMountain {

    public static void main(String[] args) {
        PrintUtil.log(mountainCount(new int[]{16, 5, 3, 10, 21, 7}));
        PrintUtil.log(mountainCount(new int[]{1, 3, 4, 4, 2}));
    }

    /**
     * 可以看到的山的数量
     *
     * @param height 从左到右山高
     * @return
     */
    private static int mountainCount(int[] height) {
        // 从1开始 奇左看偶右看
        // 从0开始 奇右看偶左看
        int length = height.length;
        // 0 1 2座山啥都看不到
        if (length < 3) {
            return 0;
        }
        // 返回结果
        int sum = 0;
        // 第0个看不见，从1迭代
        for (int i = 1; i < length; i++) {
            // 不管高低都能看见
            int temp = 0;
            // 奇右看
            if (NumUtil.isOdd(i)) {
                // 不是最后一个
                if (i != length - 1) {
                    // 右边肯定有一个
                    temp++;
                    // 不是倒数第二
                    if (i != length - 2) {
                        int max = height[i + 1];
                        for (int j = i; j < length - 2; j++) {
                            if (height[j + 2] > max) {
                                temp++;
                                max = height[j + 2];
                            }
                        }
                    }
                }
            } else {
                // 偶左看
                int max = height[i - 1];
                // 左边肯定有一个 因为迭代中最小的偶数是2
                temp++;
                for (int j = i; j > 1; j--) {
                    if (height[j - 2] > max) {
                        temp++;
                        max = height[j - 2];
                    }
                }
            }
            sum += temp;
            PrintUtil.log(temp);
        }
        return sum;
    }
}

