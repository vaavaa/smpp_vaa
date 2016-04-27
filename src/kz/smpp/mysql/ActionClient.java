package kz.smpp.mysql;

public class ActionClient {
    private int clientId;
    private int actionType;
    private int actionStatus;
    private int contentType;

    public int getClientId() {
        return clientId;
    }

    public int getActionStatus() {
        return actionStatus;
    }

    public int getActionType() {
        return actionType;
    }

    public int getContentType() {
        return contentType;
    }

    public void setActionStatus(int actionStatus) {
        this.actionStatus = actionStatus;
    }

    public void setActionType(int actionType) {
        this.actionType = actionType;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public void setContentType(int contentType) {
        this.contentType = contentType;
    }
}
