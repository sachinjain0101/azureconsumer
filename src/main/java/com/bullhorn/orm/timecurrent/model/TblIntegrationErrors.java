//package com.bullhorn.orm.timecurrent.model;
//
//import java.io.Serializable;
//import java.util.Date;
//
//public class TblIntegrationErrors implements Serializable {
//
//    private static final long serialVersionUID = 1L;
//    private Integer recordId;
//    private String integrationKey;
//    private String client;
//    private Integer frontOfficeSystemRecordId;
//    private String processName;
//    private String messageId;
//    private String errorSource;
//    private String errorCode;
//    private String errorDescription;
//    private Date createDateTime;
//
//    public TblIntegrationErrors() {
//    }
//
//    public TblIntegrationErrors(Integer recordId) {
//        this.recordId = recordId;
//    }
//
//    public TblIntegrationErrors(Integer recordId, Date createDateTime) {
//        this.recordId = recordId;
//        this.createDateTime = createDateTime;
//    }
//
//    public Integer getRecordId() {
//        return recordId;
//    }
//
//    public void setRecordId(Integer recordId) {
//        this.recordId = recordId;
//    }
//
//    public String getIntegrationKey() {
//        return integrationKey;
//    }
//
//    public void setIntegrationKey(String integrationKey) {
//        this.integrationKey = integrationKey;
//    }
//
//    public String getClient() {
//        return client;
//    }
//
//    public void setClient(String client) {
//        this.client = client;
//    }
//
//    public Integer getFrontOfficeSystemRecordId() {
//        return frontOfficeSystemRecordId;
//    }
//
//    public void setFrontOfficeSystemRecordId(Integer frontOfficeSystemRecordId) {
//        this.frontOfficeSystemRecordId = frontOfficeSystemRecordId;
//    }
//
//    public String getProcessName() {
//        return processName;
//    }
//
//    public void setProcessName(String processName) {
//        this.processName = processName;
//    }
//
//    public String getMessageId() {
//        return messageId;
//    }
//
//    public void setMessageId(String messageId) {
//        this.messageId = messageId;
//    }
//
//    public String getErrorSource() {
//        return errorSource;
//    }
//
//    public void setErrorSource(String errorSource) {
//        this.errorSource = errorSource;
//    }
//
//    public String getErrorCode() {
//        return errorCode;
//    }
//
//    public void setErrorCode(String errorCode) {
//        this.errorCode = errorCode;
//    }
//
//    public String getErrorDescription() {
//        return errorDescription;
//    }
//
//    public void setErrorDescription(String errorDescription) {
//        this.errorDescription = errorDescription;
//    }
//
//    public Date getCreateDateTime() {
//        return createDateTime;
//    }
//
//    public void setCreateDateTime(Date createDateTime) {
//        this.createDateTime = createDateTime;
//    }
//
//    @Override
//    public String toString() {
//        return "com.bullhorn.orm.timecurrent.model.TblIntegrationErrors[ recordId=" + recordId + " ]";
//    }
//
//}
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bullhorn.orm.timecurrent.model;

import java.io.Serializable;
import java.util.Date;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
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
@Table(name = "tblIntegration_Errors")
@XmlRootElement
@NamedQueries({
        @NamedQuery(name = "TblIntegrationErrors.findAll", query = "SELECT t FROM TblIntegrationErrors t")
        , @NamedQuery(name = "TblIntegrationErrors.findByRecordId", query = "SELECT t FROM TblIntegrationErrors t WHERE t.recordId = :recordId")
        , @NamedQuery(name = "TblIntegrationErrors.findByIntegrationKey", query = "SELECT t FROM TblIntegrationErrors t WHERE t.integrationKey = :integrationKey")
        , @NamedQuery(name = "TblIntegrationErrors.findByClient", query = "SELECT t FROM TblIntegrationErrors t WHERE t.client = :client")
        , @NamedQuery(name = "TblIntegrationErrors.findByFrontOfficeSystemRecordId", query = "SELECT t FROM TblIntegrationErrors t WHERE t.frontOfficeSystemRecordId = :frontOfficeSystemRecordId")
        , @NamedQuery(name = "TblIntegrationErrors.findByProcessName", query = "SELECT t FROM TblIntegrationErrors t WHERE t.processName = :processName")
        , @NamedQuery(name = "TblIntegrationErrors.findByMessageId", query = "SELECT t FROM TblIntegrationErrors t WHERE t.messageId = :messageId")
        , @NamedQuery(name = "TblIntegrationErrors.findByErrorSource", query = "SELECT t FROM TblIntegrationErrors t WHERE t.errorSource = :errorSource")
        , @NamedQuery(name = "TblIntegrationErrors.findByErrorCode", query = "SELECT t FROM TblIntegrationErrors t WHERE t.errorCode = :errorCode")
        , @NamedQuery(name = "TblIntegrationErrors.findByCreateDateTime", query = "SELECT t FROM TblIntegrationErrors t WHERE t.createDateTime = :createDateTime")})
public class TblIntegrationErrors implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "RecordId")
    private Integer recordId;
    @Size(max = 100)
    @Column(name = "IntegrationKey")
    private String integrationKey;
    @Size(max = 4)
    @Column(name = "Client")
    private String client;
    @Column(name = "FrontOfficeSystemRecordId")
    private Integer frontOfficeSystemRecordId;
    @Size(max = 100)
    @Column(name = "ProcessName")
    private String processName;
    @Size(max = 1000)
    @Column(name = "MessageId")
    private String messageId;
    @Size(max = 1000)
    @Column(name = "ErrorSource")
    private String errorSource;
    @Size(max = 50)
    @Column(name = "ErrorCode")
    private String errorCode;
    @Lob
    @Column(name = "ErrorDescription")
    private String errorDescription;
    @Basic(optional = false)
    @NotNull
    @Column(name = "CreateDateTime")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createDateTime;

    public TblIntegrationErrors() {
    }

    public TblIntegrationErrors(Integer recordId) {
        this.recordId = recordId;
    }

    public TblIntegrationErrors(Integer recordId, Date createDateTime) {
        this.recordId = recordId;
        this.createDateTime = createDateTime;
    }

    public Integer getRecordId() {
        return recordId;
    }

    public void setRecordId(Integer recordId) {
        this.recordId = recordId;
    }

    public String getIntegrationKey() {
        return integrationKey;
    }

    public void setIntegrationKey(String integrationKey) {
        this.integrationKey = integrationKey;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public Integer getFrontOfficeSystemRecordId() {
        return frontOfficeSystemRecordId;
    }

    public void setFrontOfficeSystemRecordId(Integer frontOfficeSystemRecordId) {
        this.frontOfficeSystemRecordId = frontOfficeSystemRecordId;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getErrorSource() {
        return errorSource;
    }

    public void setErrorSource(String errorSource) {
        this.errorSource = errorSource;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public Date getCreateDateTime() {
        return createDateTime;
    }

    public void setCreateDateTime(Date createDateTime) {
        this.createDateTime = createDateTime;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (recordId != null ? recordId.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof TblIntegrationErrors)) {
            return false;
        }
        TblIntegrationErrors other = (TblIntegrationErrors) object;
        if ((this.recordId == null && other.recordId != null) || (this.recordId != null && !this.recordId.equals(other.recordId))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "com.bullhorn.orm.timecurrent.model.TblIntegrationErrors[ recordId=" + recordId + " ]";
    }

}
