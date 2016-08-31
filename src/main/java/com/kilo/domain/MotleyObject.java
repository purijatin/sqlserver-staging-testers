
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
    private Integer var1;
    private Integer var2;
    private Integer var3;
    private Integer var4;
    private Integer var5;
    private Integer var6;
    private Integer var7;
    private Integer var8;
    private Integer var9;
    private Integer var10;
    private Integer var11;
    private Integer var12;
    private Integer var13;
    private Integer var14;
    private Integer var15;
    private Integer var16;
    private Integer var17;
    private Integer var18;
    private Integer var19;
    private Integer var20;
    private Integer var21;
    private Integer var22;
    private Integer var23;
    private Integer var24;
    private Integer var25;
    private Integer var26;
    private Integer var27;
    private Integer var28;
    private Integer var29;
    private Integer var30;
    private Integer var31;
    private Integer var32;
    private Integer var33;
    private Integer var34;
    private Integer var35;
    private Integer var36;
    private Integer var37;
    private Integer var38;
    private Integer var39;
    private Integer var40;
    private Integer var41;
    private Integer var42;
    private Integer var43;
    private Integer var44;
    private Integer var45;
    private Integer var46;
    private Integer var47;
    private Integer var48;
    private Integer var49;
    private Integer var50;
    private Integer var51;
    private Integer var52;
    private Integer var53;
    private Integer var54;
    private Integer var55;
    private Integer var56;
    private Integer var57;
    private Integer var58;
    private Integer var59;
    private Integer var60;
    private Integer var61;
    private Integer var62;
    private Integer var63;
    private Integer var64;
    private Integer var65;
    private Integer var66;
    private Integer var67;
    private Integer var68;
    private Integer var69;
    private Integer var70;
    private Integer var71;
    private Integer var72;
    private Integer var73;
    private Integer var74;
    private Integer var75;
    private Integer var76;
    private Integer var77;
    private Integer var78;
    private Integer var79;
    private Integer var80;
    private Integer var81;
    private Integer var82;
    private Integer var83;
    private Integer var84;
    private Integer var85;
    private Integer var86;
    private Integer var87;
    private Integer var88;
    private Integer var89;
    private Integer var90;
    private Integer var91;
    private Integer var92;
    private Integer var93;
    private Integer var94;
    private Integer var95;
    private Integer var96;
    private Integer var97;
    private Integer var98;
    private Integer var99;
    private Integer var100;

    public MotleyObject() {
    }

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
        bulkInsertString.append(price.toPlainString()).append(
                BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(amount.toPlainString()).append(
                BULK_INSERT_FIELD_SEPARATOR);
        bulkInsertString.append(fxRate.toPlainString()).append(
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

    public void setValid(Boolean valid) {
        isValid = valid;
    }

    public void setVar1(Integer var1) {
        this.var1 = var1;
    }

    public void setVar2(Integer var2) {
        this.var2 = var2;
    }

    public void setVar3(Integer var3) {
        this.var3 = var3;
    }

    public void setVar4(Integer var4) {
        this.var4 = var4;
    }

    public void setVar5(Integer var5) {
        this.var5 = var5;
    }

    public void setVar6(Integer var6) {
        this.var6 = var6;
    }

    public void setVar7(Integer var7) {
        this.var7 = var7;
    }

    public void setVar8(Integer var8) {
        this.var8 = var8;
    }

    public void setVar9(Integer var9) {
        this.var9 = var9;
    }

    public void setVar10(Integer var10) {
        this.var10 = var10;
    }

    public void setVar11(Integer var11) {
        this.var11 = var11;
    }

    public void setVar12(Integer var12) {
        this.var12 = var12;
    }

    public void setVar13(Integer var13) {
        this.var13 = var13;
    }

    public void setVar14(Integer var14) {
        this.var14 = var14;
    }

    public void setVar15(Integer var15) {
        this.var15 = var15;
    }

    public void setVar16(Integer var16) {
        this.var16 = var16;
    }

    public void setVar17(Integer var17) {
        this.var17 = var17;
    }

    public void setVar18(Integer var18) {
        this.var18 = var18;
    }

    public void setVar19(Integer var19) {
        this.var19 = var19;
    }

    public void setVar20(Integer var20) {
        this.var20 = var20;
    }

    public void setVar21(Integer var21) {
        this.var21 = var21;
    }

    public void setVar22(Integer var22) {
        this.var22 = var22;
    }

    public void setVar23(Integer var23) {
        this.var23 = var23;
    }

    public void setVar24(Integer var24) {
        this.var24 = var24;
    }

    public void setVar25(Integer var25) {
        this.var25 = var25;
    }

    public void setVar26(Integer var26) {
        this.var26 = var26;
    }

    public void setVar27(Integer var27) {
        this.var27 = var27;
    }

    public void setVar28(Integer var28) {
        this.var28 = var28;
    }

    public void setVar29(Integer var29) {
        this.var29 = var29;
    }

    public void setVar30(Integer var30) {
        this.var30 = var30;
    }

    public void setVar31(Integer var31) {
        this.var31 = var31;
    }

    public void setVar32(Integer var32) {
        this.var32 = var32;
    }

    public void setVar33(Integer var33) {
        this.var33 = var33;
    }

    public void setVar34(Integer var34) {
        this.var34 = var34;
    }

    public void setVar35(Integer var35) {
        this.var35 = var35;
    }

    public void setVar36(Integer var36) {
        this.var36 = var36;
    }

    public void setVar37(Integer var37) {
        this.var37 = var37;
    }

    public void setVar38(Integer var38) {
        this.var38 = var38;
    }

    public void setVar39(Integer var39) {
        this.var39 = var39;
    }

    public void setVar40(Integer var40) {
        this.var40 = var40;
    }

    public void setVar41(Integer var41) {
        this.var41 = var41;
    }

    public void setVar42(Integer var42) {
        this.var42 = var42;
    }

    public void setVar43(Integer var43) {
        this.var43 = var43;
    }

    public void setVar44(Integer var44) {
        this.var44 = var44;
    }

    public void setVar45(Integer var45) {
        this.var45 = var45;
    }

    public void setVar46(Integer var46) {
        this.var46 = var46;
    }

    public void setVar47(Integer var47) {
        this.var47 = var47;
    }

    public void setVar48(Integer var48) {
        this.var48 = var48;
    }

    public void setVar49(Integer var49) {
        this.var49 = var49;
    }

    public void setVar50(Integer var50) {
        this.var50 = var50;
    }

    public void setVar51(Integer var51) {
        this.var51 = var51;
    }

    public void setVar52(Integer var52) {
        this.var52 = var52;
    }

    public void setVar53(Integer var53) {
        this.var53 = var53;
    }

    public void setVar54(Integer var54) {
        this.var54 = var54;
    }

    public void setVar55(Integer var55) {
        this.var55 = var55;
    }

    public void setVar56(Integer var56) {
        this.var56 = var56;
    }

    public void setVar57(Integer var57) {
        this.var57 = var57;
    }

    public void setVar58(Integer var58) {
        this.var58 = var58;
    }

    public void setVar59(Integer var59) {
        this.var59 = var59;
    }

    public void setVar60(Integer var60) {
        this.var60 = var60;
    }

    public void setVar61(Integer var61) {
        this.var61 = var61;
    }

    public void setVar62(Integer var62) {
        this.var62 = var62;
    }

    public void setVar63(Integer var63) {
        this.var63 = var63;
    }

    public void setVar64(Integer var64) {
        this.var64 = var64;
    }

    public void setVar65(Integer var65) {
        this.var65 = var65;
    }

    public void setVar66(Integer var66) {
        this.var66 = var66;
    }

    public void setVar67(Integer var67) {
        this.var67 = var67;
    }

    public void setVar68(Integer var68) {
        this.var68 = var68;
    }

    public void setVar69(Integer var69) {
        this.var69 = var69;
    }

    public void setVar70(Integer var70) {
        this.var70 = var70;
    }

    public void setVar71(Integer var71) {
        this.var71 = var71;
    }

    public void setVar72(Integer var72) {
        this.var72 = var72;
    }

    public void setVar73(Integer var73) {
        this.var73 = var73;
    }

    public void setVar74(Integer var74) {
        this.var74 = var74;
    }

    public void setVar75(Integer var75) {
        this.var75 = var75;
    }

    public void setVar76(Integer var76) {
        this.var76 = var76;
    }

    public void setVar77(Integer var77) {
        this.var77 = var77;
    }

    public void setVar78(Integer var78) {
        this.var78 = var78;
    }

    public void setVar79(Integer var79) {
        this.var79 = var79;
    }

    public void setVar80(Integer var80) {
        this.var80 = var80;
    }

    public void setVar81(Integer var81) {
        this.var81 = var81;
    }

    public void setVar82(Integer var82) {
        this.var82 = var82;
    }

    public void setVar83(Integer var83) {
        this.var83 = var83;
    }

    public void setVar84(Integer var84) {
        this.var84 = var84;
    }

    public void setVar85(Integer var85) {
        this.var85 = var85;
    }

    public void setVar86(Integer var86) {
        this.var86 = var86;
    }

    public void setVar87(Integer var87) {
        this.var87 = var87;
    }

    public void setVar88(Integer var88) {
        this.var88 = var88;
    }

    public void setVar89(Integer var89) {
        this.var89 = var89;
    }

    public void setVar90(Integer var90) {
        this.var90 = var90;
    }

    public void setVar91(Integer var91) {
        this.var91 = var91;
    }

    public void setVar92(Integer var92) {
        this.var92 = var92;
    }

    public void setVar93(Integer var93) {
        this.var93 = var93;
    }

    public void setVar94(Integer var94) {
        this.var94 = var94;
    }

    public void setVar95(Integer var95) {
        this.var95 = var95;
    }

    public void setVar96(Integer var96) {
        this.var96 = var96;
    }

    public void setVar97(Integer var97) {
        this.var97 = var97;
    }

    public void setVar98(Integer var98) {
        this.var98 = var98;
    }

    public void setVar99(Integer var99) {
        this.var99 = var99;
    }

    public void setVar100(Integer var100) {
        this.var100 = var100;
    }
}
