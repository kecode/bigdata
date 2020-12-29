package com.clei.algorithm.other;

import com.clei.utils.ArrayUtil;
import com.clei.utils.PrintUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 自动售货机
 * 来源：images/3_2.png
 *
 * @author KIyA
 */
public class VendingMachineSystem {

    public static void main(String[] args) {
        VendingMachineSystem v = new VendingMachineSystem(2, 5);
        PrintUtil.log(v.addProduct(3, new int[]{3, 5, 4, 6, 2}));
        PrintUtil.log(Arrays.toString(v.buyProduct(3, 3)));
        PrintUtil.log(Arrays.toString(v.queryProduct()));
    }

    /**
     * 轨道数
     */
    private int trayNum;

    /**
     * 轨道容量
     */
    private int trayCapacity;

    /**
     * 商品
     */
    private Map<Integer, int[]> products;

    /**
     * 添加商品
     *
     * @param brandId       品牌id
     * @param productIdList 商品id list
     */
    public boolean addProduct(int brandId, int[] productIdList) {
        Integer key = Integer.valueOf(brandId);
        int[] oldProduct = this.products.get(key);
        // 售货机有此商品
        if (null != oldProduct) {
            // 容量充足
            if (this.trayCapacity - oldProduct.length >= productIdList.length) {
                // 数组合并
                int[] productList = ArrayUtil.merge(oldProduct, productIdList);
                // 放入
                this.products.put(key, productList);
                return true;
            } else {
                return false;
            }
        } else {
            // 售货机无此商品
            // 容量充足
            if (this.trayCapacity >= productIdList.length) {
                // 放入
                this.products.put(key, productIdList);
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * 购买商品
     *
     * @param brandId 品牌id
     * @param num     购买数量
     */
    public int[] buyProduct(int brandId, int num) {
        Integer key = Integer.valueOf(brandId);
        int[] oldProduct = this.products.get(key);
        // 售货机有此商品
        if (null != oldProduct) {
            // 储量充足
            if (oldProduct.length == num) {
                this.products.remove(key);
                return oldProduct;
            } else if (oldProduct.length > num) {
                // 返回结果
                int[] result = new int[num];
                System.arraycopy(oldProduct, 0, result, 0, num);
                // 剩余商品
                int[] productList = new int[oldProduct.length - num];
                System.arraycopy(oldProduct, num, productList, 0, productList.length);
                this.products.put(key, productList);
                return result;
            }
        }
        return new int[0];
    }

    /**
     * 查询商品
     *
     * @return
     */
    public int[] queryProduct() {
        Set<Integer> keys = this.products.keySet();
        if (0 == keys.size()) {
            return new int[0];
        }
        List<Integer> products = keys.stream().sorted().collect(Collectors.toList());
        int[] result = new int[products.size()];
        for (int i = 0; i < products.size(); i++) {
            result[i] = this.products.get(products.get(i))[0];
        }
        return result;
    }

    public VendingMachineSystem(int trayNum, int trayCapacity) {
        this.trayNum = trayNum;
        this.trayCapacity = trayCapacity;
        // 初始化 未考虑并发用的HashMap
        products = new HashMap<>(trayNum);
        PrintUtil.log(null);
    }

}
