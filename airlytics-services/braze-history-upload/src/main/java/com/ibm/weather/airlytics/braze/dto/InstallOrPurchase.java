package com.ibm.weather.airlytics.braze.dto;

import java.time.Instant;

public class InstallOrPurchase {

    private String userId;

    private Instant installDate;

    private String product;

    private boolean trial = false;

    private boolean premium = false;

    private boolean premiumAutoRenewStatus;

    private Instant premiumAutoRenewChangeDate;

    private Instant startDate;

    private Instant endDate;

    private int trialLengthDays = 0;

    public InstallOrPurchase() {
    }

    public InstallOrPurchase(String userId, Instant installDate, String product, boolean trial, Instant startDate, int trialLengthDays) {
        this.userId = userId;
        this.installDate = installDate;
        this.product = product;
        this.trial = trial;
        this.startDate = startDate;
        this.trialLengthDays = trialLengthDays;
    }

    public InstallOrPurchase(String userId, String product, Instant startDate, Instant endDate) {
        this.userId = userId;
        this.product = product;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public InstallOrPurchase(String userId, boolean premium, boolean premiumAutoRenewStatus, Instant premiumAutoRenewChangeDate) {
        this.userId = userId;
        this.premium = premium;
        this.premiumAutoRenewStatus = premiumAutoRenewStatus;
        this.premiumAutoRenewChangeDate = premiumAutoRenewChangeDate;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Instant getInstallDate() {
        return installDate;
    }

    public void setInstallDate(Instant installDate) {
        this.installDate = installDate;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public boolean isTrial() {
        return trial;
    }

    public void setTrial(boolean trial) {
        this.trial = trial;
    }

    public Instant getStartDate() {
        return startDate;
    }

    public void setStartDate(Instant startDate) {
        this.startDate = startDate;
    }

    public Instant getEndDate() {
        return endDate;
    }

    public void setEndDate(Instant endDate) {
        this.endDate = endDate;
    }

    public int getTrialLengthDays() {
        return trialLengthDays;
    }

    public void setTrialLengthDays(int trialLengthDays) {
        this.trialLengthDays = trialLengthDays;
    }

    public boolean isPremium() {
        return premium;
    }

    public void setPremium(boolean premium) {
        this.premium = premium;
    }

    public boolean isPremiumAutoRenewStatus() {
        return premiumAutoRenewStatus;
    }

    public void setPremiumAutoRenewStatus(boolean premiumAutoRenewStatus) {
        this.premiumAutoRenewStatus = premiumAutoRenewStatus;
    }

    public Instant getPremiumAutoRenewChangeDate() {
        return premiumAutoRenewChangeDate;
    }

    public void setPremiumAutoRenewChangeDate(Instant premiumAutoRenewChangeDate) {
        this.premiumAutoRenewChangeDate = premiumAutoRenewChangeDate;
    }
}
