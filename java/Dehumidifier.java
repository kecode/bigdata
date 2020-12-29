package com.clei.algorithm.other;

import com.clei.utils.PrintUtil;

/**
 * 除湿机
 * 来源：images/1.png
 *
 * @author KIyA
 */
public class Dehumidifier {

    public static void main(String[] args) {
        PrintUtil.log(dehumidification(new int[]{3, 2, 3}, 2, 3));
        PrintUtil.log(dehumidification(new int[]{4, 2, 5}, 3, 4));
        PrintUtil.log(dehumidification(new int[]{4, 0}, 2, 2));
    }

    /**
     * 除湿
     *
     * @param humidity 各个房间湿度
     * @param single   小型除湿机的功力
     * @param multiple 中央除湿机的功力
     * @return
     */
    private static int dehumidification(int[] humidity, int single, int multiple) {
        // 房间数
        int rooms = humidity.length;
        if (0 == rooms) {
            return 0;
        }
        // 最大湿度和最小湿度
        int max = 0, min = 0;
        for (int i : humidity) {
            if (i > max) {
                max = i;
            }
            if (i < min) {
                min = i;
            }
        }
        // 厉害的和不厉害的
        int a, b;
        if (single > multiple) {
            a = single;
            b = multiple;
        } else {
            a = multiple;
            b = single;
        }
        // 湿度大的交给厉害的解决，湿度小的交给不厉害的解决
        // 因为正好每分钟能对所有房间进行处理
        // 厉害的花费分钟数
        int minutesA = max / a;
        if (0 != max % a) {
            minutesA++;
        }
        // 不厉害的花费分钟数
        int minutesB = min / b;
        if (0 != min % b) {
            minutesB++;
        }
        // 返回大的分钟数
        return minutesA > minutesB ? minutesA : minutesB;
    }

}
