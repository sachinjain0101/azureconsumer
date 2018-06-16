/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bullhorn.orm.refreshWork.model;

import java.io.Serializable;
import java.util.Date;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author sachin.jain
 */
@Entity
@Table(name = "tblIntegration_ServiceBusMessages", schema = "dbo")
public class TblIntegrationServiceBusMessages implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "RecordID")
    private Long recordID;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 200)
    @Column(name = "MessageID")
    private String messageID;
    @Basic(optional = false)
    @NotNull
    @Column(name = "SequenceNumber")
    private long sequenceNumber;
    @Column(name = "Message")
    private String message;
    @Column(name = "FrontOfficeSystemRecordID")
    private Integer frontOfficeSystemRecordID;

    public TblIntegrationServiceBusMessages() {
    }

    public TblIntegrationServiceBusMessages(Long recordID) {
        this.recordID = recordID;
    }

    public TblIntegrationServiceBusMessages(Long recordID, String messageID, long sequenceNumber) {
        this.recordID = recordID;
        this.messageID = messageID;
        this.sequenceNumber = sequenceNumber;
    }

    public Long getRecordID() {
        return recordID;
    }

    public void setRecordID(Long recordID) {
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

    public TblIntegrationServiceBusMessages(@NotNull @Size(min = 1, max = 200) String messageID, @NotNull long sequenceNumber, String message, Integer frontOfficeSystemRecordID) {
        this.messageID = messageID;
        this.sequenceNumber = sequenceNumber;
        this.message = message;
        this.frontOfficeSystemRecordID = frontOfficeSystemRecordID;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (recordID != null ? recordID.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof TblIntegrationServiceBusMessages)) {
            return false;
        }
        TblIntegrationServiceBusMessages other = (TblIntegrationServiceBusMessages) object;
        if ((this.recordID == null && other.recordID != null) || (this.recordID != null && !this.recordID.equals(other.recordID))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.bullhorn.orm.refreshwork.model.TblIntegrationServiceBusMessages[ recordID=" + recordID + " ]";
    }

}
