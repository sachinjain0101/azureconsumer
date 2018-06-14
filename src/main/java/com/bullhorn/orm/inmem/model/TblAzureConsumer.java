package com.bullhorn.orm.inmem.model;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Objects;

@Entity
public class TblAzureConsumer implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull
    @Column(name = "RecordID")
    private long recordID;
    @Id
    @NotNull
    @Size(min = 1, max = 200)
    @Column(name = "MessageID")
    private String messageID;
    @NotNull
    @Column(name = "SequenceNumber")
    private long sequenceNumber;
    @Lob
    @Column(name = "Message", length=5000000)
    private String message;
    @Column(name = "FrontOfficeSystemRecordID")
    private Integer frontOfficeSystemRecordID;

    public long getRecordID() {
        return recordID;
    }

    public void setRecordID(long recordID) {
        this.recordID = recordID;
    }

    public String getMessageID() {
        return messageID;
    }

    public void setMessageID(String messageID) {
        this.messageID = messageID;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getFrontOfficeSystemRecordID() {
        return frontOfficeSystemRecordID;
    }

    public void setFrontOfficeSystemRecordID(Integer frontOfficeSystemRecordID) {
        this.frontOfficeSystemRecordID = frontOfficeSystemRecordID;
    }

    public TblAzureConsumer(@NotNull long recordID, @NotNull @Size(min = 1, max = 200) String messageID, @NotNull long sequenceNumber, String message, Integer frontOfficeSystemRecordID) {
        this.recordID = recordID;
        this.messageID = messageID;
        this.sequenceNumber = sequenceNumber;
        this.message = message;
        this.frontOfficeSystemRecordID = frontOfficeSystemRecordID;
    }

    public TblAzureConsumer() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TblAzureConsumer that = (TblAzureConsumer) o;
        return recordID == that.recordID &&
                sequenceNumber == that.sequenceNumber &&
                Objects.equals(messageID, that.messageID) &&
                Objects.equals(message, that.message) &&
                Objects.equals(frontOfficeSystemRecordID, that.frontOfficeSystemRecordID);
    }

    @Override
    public int hashCode() {

        return Objects.hash(recordID, messageID, sequenceNumber, message, frontOfficeSystemRecordID);
    }

    @Override
    public String toString() {
        return "TblAzureConsumer{" +
                "recordID=" + recordID +
                ", messageID='" + messageID + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", message='" + message + '\'' +
                ", frontOfficeSystemRecordID=" + frontOfficeSystemRecordID +
                '}';
    }
}
