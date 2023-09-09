package org.example;

public class Order
{
    private long ordertime;
    private int orderid;
    private String itemid;
    private double orderunits;
    private Address address;

    public long getOrdertime() {
        return ordertime;
    }

    public void setOrdertime(long ordertime) {
        this.ordertime = ordertime;
    }

    public int getOrderid() {
        return orderid;
    }

    public void setOrderid(int orderid) {
        this.orderid = orderid;
    }

    public String getItemid() {
        return itemid;
    }

    public void setItemid(String itemid) {
        this.itemid = itemid;
    }

    public double getOrderunits() {
        return orderunits;
    }

    public void setOrderunits(double orderunits) {
        this.orderunits = orderunits;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }
}
