package cn.fancychuan.spark;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 自定义二次排序
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if (clickCount < that.getClickCount()) return true;
        else if (clickCount == that.getClickCount() && orderCount < that.getOrderCount()) return true;
        else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount < that.getPayCount()) return true;
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if (clickCount > that.getClickCount()) return true;
        else if (clickCount == that.getClickCount() && orderCount > that.getOrderCount()) return true;
        else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount > that.getPayCount()) return true;
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if ($less(that)) return true;
        else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount == that.getPayCount()) return true;
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if ($greater(that)) {return true;}
        else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount == that.getPayCount()) return true;
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) return (int) (clickCount - that.getClickCount());
        else if (orderCount - that.getOrderCount() != 0) return (int) (orderCount - that.getOrderCount());
        else if (payCount - that.getPayCount() != 0) return (int) (payCount - that.getPayCount());
        return 0;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public long getPayCount() {
        return payCount;
    }
}
