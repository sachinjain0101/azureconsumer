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
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
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
@Table(name = "tblIntegration_Client")
@XmlRootElement
@NamedQueries({
        @NamedQuery(name = "TblIntegrationClient.findAll", query = "SELECT t FROM TblIntegrationClient t")
        , @NamedQuery(name = "TblIntegrationClient.findByRecordId", query = "SELECT t FROM TblIntegrationClient t WHERE t.recordId = :recordId")
        , @NamedQuery(name = "TblIntegrationClient.findByIntegrationKey", query = "SELECT t FROM TblIntegrationClient t WHERE t.integrationKey = :integrationKey")
        , @NamedQuery(name = "TblIntegrationClient.findByClient", query = "SELECT t FROM TblIntegrationClient t WHERE t.client = :client")
        , @NamedQuery(name = "TblIntegrationClient.findByIsMapped", query = "SELECT t FROM TblIntegrationClient t WHERE t.isMapped = :isMapped")
        , @NamedQuery(name = "TblIntegrationClient.findByMap", query = "SELECT t FROM TblIntegrationClient t WHERE t.map = :map")
        , @NamedQuery(name = "TblIntegrationClient.findByIsEmailSent", query = "SELECT t FROM TblIntegrationClient t WHERE t.isEmailSent = :isEmailSent")
        , @NamedQuery(name = "TblIntegrationClient.findByEnvironment", query = "SELECT t FROM TblIntegrationClient t WHERE t.environment = :environment")
        , @NamedQuery(name = "TblIntegrationClient.findByAssignmentLastReceivedDate", query = "SELECT t FROM TblIntegrationClient t WHERE t.assignmentLastReceivedDate = :assignmentLastReceivedDate")
        , @NamedQuery(name = "TblIntegrationClient.findByCreatedBy", query = "SELECT t FROM TblIntegrationClient t WHERE t.createdBy = :createdBy")
        , @NamedQuery(name = "TblIntegrationClient.findByMaintUserName", query = "SELECT t FROM TblIntegrationClient t WHERE t.maintUserName = :maintUserName")
        , @NamedQuery(name = "TblIntegrationClient.findByCreateDateTime", query = "SELECT t FROM TblIntegrationClient t WHERE t.createDateTime = :createDateTime")
        , @NamedQuery(name = "TblIntegrationClient.findByMaintDateTime", query = "SELECT t FROM TblIntegrationClient t WHERE t.maintDateTime = :maintDateTime")
        , @NamedQuery(name = "TblIntegrationClient.findByRecordStatus", query = "SELECT t FROM TblIntegrationClient t WHERE t.recordStatus = :recordStatus")
        , @NamedQuery(name = "TblIntegrationClient.findByExternalApiUserId", query = "SELECT t FROM TblIntegrationClient t WHERE t.externalApiUserId = :externalApiUserId")
        , @NamedQuery(name = "TblIntegrationClient.findByExternalApiPassword", query = "SELECT t FROM TblIntegrationClient t WHERE t.externalApiPassword = :externalApiPassword")
        , @NamedQuery(name = "TblIntegrationClient.findByPayFileIntegrated", query = "SELECT t FROM TblIntegrationClient t WHERE t.payFileIntegrated = :payFileIntegrated")
        , @NamedQuery(name = "TblIntegrationClient.findByCheckEmplDups", query = "SELECT t FROM TblIntegrationClient t WHERE t.checkEmplDups = :checkEmplDups")
        , @NamedQuery(name = "TblIntegrationClient.findByAllowEmptyDept", query = "SELECT t FROM TblIntegrationClient t WHERE t.allowEmptyDept = :allowEmptyDept")
        , @NamedQuery(name = "TblIntegrationClient.findByPayfileSendIndividualJSON", query = "SELECT t FROM TblIntegrationClient t WHERE t.payfileSendIndividualJSON = :payfileSendIndividualJSON")
        , @NamedQuery(name = "TblIntegrationClient.findByGetAllAssignments", query = "SELECT t FROM TblIntegrationClient t WHERE t.getAllAssignments = :getAllAssignments")})
public class TblIntegrationClient implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "RecordId")
    private Integer recordId;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 100)
    @Column(name = "IntegrationKey")
    private String integrationKey;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 4)
    @Column(name = "Client")
    private String client;
    @Column(name = "IsMapped")
    private Boolean isMapped;
    @Size(max = 1000)
    @Column(name = "Map")
    private String map;
    @Column(name = "IsEmailSent")
    private Boolean isEmailSent;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 50)
    @Column(name = "Environment")
    private String environment;
    @Lob
    @Column(name = "EmailAddresses")
    private String emailAddresses;
    @Column(name = "AssignmentLastReceivedDate")
    @Temporal(TemporalType.TIMESTAMP)
    private Date assignmentLastReceivedDate;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 1000)
    @Column(name = "CreatedBy")
    private String createdBy;
    @Basic(optional = false)
    @NotNull
    @Size(min = 1, max = 1000)
    @Column(name = "MaintUserName")
    private String maintUserName;
    @Basic(optional = false)
    @NotNull
    @Column(name = "CreateDateTime")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createDateTime;
    @Column(name = "MaintDateTime")
    @Temporal(TemporalType.TIMESTAMP)
    private Date maintDateTime;
    @Basic(optional = false)
    @NotNull
    @Column(name = "RecordStatus")
    private boolean recordStatus;
    @Size(max = 1000)
    @Column(name = "ExternalApiUserId")
    private String externalApiUserId;
    @Size(max = 1000)
    @Column(name = "ExternalApiPassword")
    private String externalApiPassword;
    @Basic(optional = false)
    @NotNull
    @Column(name = "PayFileIntegrated")
    private boolean payFileIntegrated;
    @Basic(optional = false)
    @NotNull
    @Column(name = "CheckEmplDups")
    private boolean checkEmplDups;
    @Basic(optional = false)
    @NotNull
    @Column(name = "AllowEmptyDept")
    private boolean allowEmptyDept;
    @Column(name = "PayfileSendIndividualJSON")
    private Integer payfileSendIndividualJSON;
    @Basic(optional = false)
    @NotNull
    @Column(name = "GetAllAssignments")
    private boolean getAllAssignments;
    @JoinColumn(name = "StatusRecordId", referencedColumnName = "RecordId")
    @ManyToOne(optional = false)
    private TblIntegrationClientStatus statusRecordId;
    @JoinColumn(name = "FrontOfficeSystemRecordId", referencedColumnName = "RecordId")
    @ManyToOne(optional = false)
    private TblIntegrationFrontOfficeSystem frontOfficeSystemRecordId;

    public TblIntegrationClient() {
    }

    public TblIntegrationClient(Integer recordId) {
        this.recordId = recordId;
    }

    public TblIntegrationClient(Integer recordId, String integrationKey, String client, String environment, String createdBy, String maintUserName, Date createDateTime, boolean recordStatus, boolean payFileIntegrated, boolean checkEmplDups, boolean allowEmptyDept, boolean getAllAssignments) {
        this.recordId = recordId;
        this.integrationKey = integrationKey;
        this.client = client;
        this.environment = environment;
        this.createdBy = createdBy;
        this.maintUserName = maintUserName;
        this.createDateTime = createDateTime;
        this.recordStatus = recordStatus;
        this.payFileIntegrated = payFileIntegrated;
        this.checkEmplDups = checkEmplDups;
        this.allowEmptyDept = allowEmptyDept;
        this.getAllAssignments = getAllAssignments;
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

    public Boolean getIsMapped() {
        return isMapped;
    }

    public void setIsMapped(Boolean isMapped) {
        this.isMapped = isMapped;
    }

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    public Boolean getIsEmailSent() {
        return isEmailSent;
    }

    public void setIsEmailSent(Boolean isEmailSent) {
        this.isEmailSent = isEmailSent;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getEmailAddresses() {
        return emailAddresses;
    }

    public void setEmailAddresses(String emailAddresses) {
        this.emailAddresses = emailAddresses;
    }

    public Date getAssignmentLastReceivedDate() {
        return assignmentLastReceivedDate;
    }

    public void setAssignmentLastReceivedDate(Date assignmentLastReceivedDate) {
        this.assignmentLastReceivedDate = assignmentLastReceivedDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getMaintUserName() {
        return maintUserName;
    }

    public void setMaintUserName(String maintUserName) {
        this.maintUserName = maintUserName;
    }

    public Date getCreateDateTime() {
        return createDateTime;
    }

    public void setCreateDateTime(Date createDateTime) {
        this.createDateTime = createDateTime;
    }

    public Date getMaintDateTime() {
        return maintDateTime;
    }

    public void setMaintDateTime(Date maintDateTime) {
        this.maintDateTime = maintDateTime;
    }

    public boolean getRecordStatus() {
        return recordStatus;
    }

    public void setRecordStatus(boolean recordStatus) {
        this.recordStatus = recordStatus;
    }

    public String getExternalApiUserId() {
        return externalApiUserId;
    }

    public void setExternalApiUserId(String externalApiUserId) {
        this.externalApiUserId = externalApiUserId;
    }

    public String getExternalApiPassword() {
        return externalApiPassword;
    }

    public void setExternalApiPassword(String externalApiPassword) {
        this.externalApiPassword = externalApiPassword;
    }

    public boolean getPayFileIntegrated() {
        return payFileIntegrated;
    }

    public void setPayFileIntegrated(boolean payFileIntegrated) {
        this.payFileIntegrated = payFileIntegrated;
    }

    public boolean getCheckEmplDups() {
        return checkEmplDups;
    }

    public void setCheckEmplDups(boolean checkEmplDups) {
        this.checkEmplDups = checkEmplDups;
    }

    public boolean getAllowEmptyDept() {
        return allowEmptyDept;
    }

    public void setAllowEmptyDept(boolean allowEmptyDept) {
        this.allowEmptyDept = allowEmptyDept;
    }

    public Integer getPayfileSendIndividualJSON() {
        return payfileSendIndividualJSON;
    }

    public void setPayfileSendIndividualJSON(Integer payfileSendIndividualJSON) {
        this.payfileSendIndividualJSON = payfileSendIndividualJSON;
    }

    public boolean getGetAllAssignments() {
        return getAllAssignments;
    }

    public void setGetAllAssignments(boolean getAllAssignments) {
        this.getAllAssignments = getAllAssignments;
    }

    public TblIntegrationClientStatus getStatusRecordId() {
        return statusRecordId;
    }

    public void setStatusRecordId(TblIntegrationClientStatus statusRecordId) {
        this.statusRecordId = statusRecordId;
    }

    public TblIntegrationFrontOfficeSystem getFrontOfficeSystemRecordId() {
        return frontOfficeSystemRecordId;
    }

    public void setFrontOfficeSystemRecordId(TblIntegrationFrontOfficeSystem frontOfficeSystemRecordId) {
        this.frontOfficeSystemRecordId = frontOfficeSystemRecordId;
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
        if (!(object instanceof TblIntegrationClient)) {
            return false;
        }
        TblIntegrationClient other = (TblIntegrationClient) object;
        if ((this.recordId == null && other.recordId != null) || (this.recordId != null && !this.recordId.equals(other.recordId))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TblIntegrationClient[ recordId=" + recordId + " ]";
    }

}
