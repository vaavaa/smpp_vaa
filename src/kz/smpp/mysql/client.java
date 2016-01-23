package kz.smpp.mysql;

import java.util.Date;

public class client {

    private long addrs;
    private int status;
    private int id;
    private Date helpDate;

    public Date getHelpDate() {
        return helpDate;
    }

    public void setHelpDate(Date helpDate) {
        this.helpDate = helpDate;
    }

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public int getStatus() {
        return status;
    }
    public void setStatus(int status) {
        this.status = status;
    }
    public void setAddrs(long addrs) {
        this.addrs = addrs;
    }
    public long getAddrs() {
        return addrs;
    }
}
