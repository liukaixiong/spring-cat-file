package org.unidal.cat.message.bean;

public class Task {
    private int m_id;

    private String m_producer;

    private String m_consumer;

    private int m_failureCount;

    private String m_reportName;

    private String m_reportDomain;

    private java.util.Date m_reportPeriod;

    private int m_status;

    private int m_taskType;

    private java.util.Date m_creationDate;

    private java.util.Date m_startDate;

    private java.util.Date m_endDate;

    private int m_keyId;

    private int m_count;

    private int m_startLimit;

    private int m_endLimit;

    public String getConsumer() {
        return m_consumer;
    }

    public int getCount() {
        return m_count;
    }

    public java.util.Date getCreationDate() {
        return m_creationDate;
    }

    public java.util.Date getEndDate() {
        return m_endDate;
    }

    public int getEndLimit() {
        return m_endLimit;
    }

    public int getFailureCount() {
        return m_failureCount;
    }

    public int getId() {
        return m_id;
    }

    public int getKeyId() {
        return m_keyId;
    }

    public String getProducer() {
        return m_producer;
    }

    public String getReportDomain() {
        return m_reportDomain;
    }

    public String getReportName() {
        return m_reportName;
    }

    public java.util.Date getReportPeriod() {
        return m_reportPeriod;
    }

    public java.util.Date getStartDate() {
        return m_startDate;
    }

    public int getStartLimit() {
        return m_startLimit;
    }

    public int getStatus() {
        return m_status;
    }

    public int getTaskType() {
        return m_taskType;
    }

    public Task setConsumer(String consumer) {

        m_consumer = consumer;
        return this;
    }

    public Task setCount(int count) {

        m_count = count;
        return this;
    }

    public Task setCreationDate(java.util.Date creationDate) {

        m_creationDate = creationDate;
        return this;
    }

    public Task setEndDate(java.util.Date endDate) {

        m_endDate = endDate;
        return this;
    }

    public Task setEndLimit(int endLimit) {

        m_endLimit = endLimit;
        return this;
    }

    public Task setFailureCount(int failureCount) {

        m_failureCount = failureCount;
        return this;
    }

    public Task setId(int id) {

        m_id = id;


        m_keyId = id;
        return this;
    }

    public Task setKeyId(int keyId) {

        m_keyId = keyId;
        return this;
    }

    public Task setProducer(String producer) {

        m_producer = producer;
        return this;
    }

    public Task setReportDomain(String reportDomain) {

        m_reportDomain = reportDomain;
        return this;
    }

    public Task setReportName(String reportName) {

        m_reportName = reportName;
        return this;
    }

    public Task setReportPeriod(java.util.Date reportPeriod) {

        m_reportPeriod = reportPeriod;
        return this;
    }

    public Task setStartDate(java.util.Date startDate) {

        m_startDate = startDate;
        return this;
    }

    public Task setStartLimit(int startLimit) {

        m_startLimit = startLimit;
        return this;
    }

    public Task setStatus(int status) {

        m_status = status;
        return this;
    }

    public Task setTaskType(int taskType) {

        m_taskType = taskType;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(1024);

        sb.append("Task[");
        sb.append("consumer: ").append(m_consumer);
        sb.append(", count: ").append(m_count);
        sb.append(", creation-date: ").append(m_creationDate);
        sb.append(", end-date: ").append(m_endDate);
        sb.append(", end-limit: ").append(m_endLimit);
        sb.append(", failure-count: ").append(m_failureCount);
        sb.append(", id: ").append(m_id);
        sb.append(", key-id: ").append(m_keyId);
        sb.append(", producer: ").append(m_producer);
        sb.append(", report-domain: ").append(m_reportDomain);
        sb.append(", report-name: ").append(m_reportName);
        sb.append(", report-period: ").append(m_reportPeriod);
        sb.append(", start-date: ").append(m_startDate);
        sb.append(", start-limit: ").append(m_startLimit);
        sb.append(", status: ").append(m_status);
        sb.append(", task-type: ").append(m_taskType);
        sb.append("]");
        return sb.toString();
    }

}
