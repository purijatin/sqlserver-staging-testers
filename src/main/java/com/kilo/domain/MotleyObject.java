
package com.kilo.domain;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class MotleyObject {

    public static final String BULK_INSERT_FIELD_SEPARATOR = "\t";

    private Date date;

    private String name;

    private Integer id;

    private BigDecimal price;

    private BigDecimal amount;

    private BigDecimal fxRate;

    /**
     * Intentionally chosen non-primitive
     */
    private Boolean isValid;

    private Date knowledgeTime;

    public MotleyObject(Date date, String name, Integer id, BigDecimal price,
            BigDecimal amount, BigDecimal fxRate, Boolean isValid,
            Date knowledgeTime) {
        this.date = date;
        this.name = name;
        this.id = id;
        this.price = price;
        this.amount = amount;
        this.fxRate = fxRate;
        this.isValid = isValid;
        this.knowledgeTime = knowledgeTime;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getFxRate() {
        return fxRate;
    }

    public void setFxRate(BigDecimal fxRate) {
        this.fxRate = fxRate;
    }

    public Boolean getIsValid() {
        return isValid;
    }

    public void setIsValid(Boolean isValid) {
        this.isValid = isValid;
    }

    public Date getKnowledgeTime() {
        return knowledgeTime;
    }

    public void setKnowledgeTime(Date knowledgeTime) {
        this.knowledgeTime = knowledgeTime;
    }

    public String toBulkInsertString() {
        StringBuilder bulkInsertString = new StringBuilder();
        bulkInsertString.append(
                DateFormatUtils.formatUTC(date, "yyyyMMdd hh:mm:ss.SSS"))
                .append(BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(name).append(BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(id).append(BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(price.intValue()).append(
                BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(amount.intValue()).append(
                BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(fxRate.intValue()).append(
                BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append((isValid != null) ? (isValid ? "1" : "0") : "")
                .append(BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(DateFormatUtils.formatUTC(knowledgeTime,
                "yyyyMMdd hh:mm:ss.SSS"));
        return bulkInsertString.toString();
    }

    public String toValueString(){
        StringBuilder ans = new StringBuilder();
        ans.append("'").append(DateFormatUtils.formatUTC(date, "yyyyMMdd hh:mm:ss.SSS")).append("'").append(",")
                .append("'").append(name).append("'").append(",")
                .append(id).append(",")
                .append(price.toPlainString()).append(",")
                .append(amount.toPlainString()).append(",")
                .append(fxRate.toPlainString()).append(",")
                .append((isValid != null) ? (isValid ? "1" : "0") : "").append(",")
                .append("'").append(DateFormatUtils.formatUTC(knowledgeTime, "yyyyMMdd hh:mm:ss.SSS")).append("'");
        return ans.toString();
    }

    public String toXMLInsertString() {
        StringBuilder xmlInsertString = new StringBuilder();
        xmlInsertString.append("<r>");
        xmlInsertString.append("<d>");
        xmlInsertString.append(DateFormatUtils.formatUTC(date,
                "yyyyMMdd hh:mm:ss.SSS"));
        xmlInsertString.append("</d>");
        xmlInsertString.append("<n>");
        xmlInsertString.append(name);
        xmlInsertString.append("</n>");
        xmlInsertString.append("<i>");
        xmlInsertString.append(id);
        xmlInsertString.append("</i>");
        xmlInsertString.append("<p>");
        xmlInsertString.append(price.toPlainString());
        xmlInsertString.append("</p>");
        xmlInsertString.append("<a>");
        xmlInsertString.append(amount.toPlainString());
        xmlInsertString.append("</a>");
        xmlInsertString.append("<f>");
        xmlInsertString.append(fxRate.toPlainString());
        xmlInsertString.append("</f>");
        xmlInsertString.append("<v>");
        xmlInsertString.append((isValid != null) ? (isValid ? "1" : "0") : "");
        xmlInsertString.append("</v>");
        xmlInsertString.append("<k>");
        xmlInsertString.append(DateFormatUtils.formatUTC(knowledgeTime,
                "yyyyMMdd hh:mm:ss.SSS"));
        xmlInsertString.append("</k>");
        xmlInsertString.append("</r>");
        return xmlInsertString.toString();
    }
}
