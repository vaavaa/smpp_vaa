package kz.smpp.mysql;

public class client {

    public long addrs;
    public int status;
    public int id;

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
