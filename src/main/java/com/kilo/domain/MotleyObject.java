
package com.kilo.domain;

import java.math.BigDecimal;
import java.util.Date;

public class MotleyObject {

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

}
