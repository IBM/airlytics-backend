package com.ibm.airlytics.consumer.purchase.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;


public class UserAttributeDetectedEvent extends BaseSubscriptionEvent<UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes> {

    public static final String EVENT_NAME = "user-attribute-detected";
    protected UserAttributeDetectedEventAttributes attributes;
    protected Attributes previousValues;

    public UserAttributeDetectedEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                      AirlyticsPurchase updatedAirlyticsPurchase,
                                      String airlockProductId, Long eventTime,
                                      String originalEventId,
                                      String platform) {
        super(currentAirlyticsPurchase, updatedAirlyticsPurchase, airlockProductId,
                eventTime, originalEventId, platform, EVENT_NAME);
        this.attributes = createAttributes(updatedAirlyticsPurchase);
        this.schemaVersion = "1.0";
        this.previousValues = currentAirlyticsPurchase == null ? new Attributes() :
                createAttributes(currentAirlyticsPurchase);

        //if updatedAirlyticsPurchase was upgraded
        if (updatedAirlyticsPurchase.getUpgradedToProduct() != null) {
            this.attributes.setPremiumProductId(updatedAirlyticsPurchase.getUpgradedToProduct().getProductId());
            this.eventTime = updatedAirlyticsPurchase.getUpgradedToProduct().getPurchaseDateMs();
        }
    }

    @Override
    public UserAttributeDetectedEventAttributes getAttributes() {
        return attributes;
    }

    public void setAttributes(UserAttributeDetectedEventAttributes attributes) {
        this.attributes = attributes;
    }

    public Attributes getPreviousValues() {
        return previousValues;
    }

    public void setPreviousValues(Attributes previousValues) {
        this.previousValues = previousValues;
    }

    public UserAttributeDetectedEventAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new UserAttributeDetectedEventAttributes(airlyticsPurchase);
    }


    public class UserAttributeDetectedEventAttributes extends Attributes {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public long premiumExpirationDate;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public long premiumStartDate;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public boolean premium;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public boolean premiumAutoRenewStatus;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public boolean premiumTrial;

        public UserAttributeDetectedEventAttributes(AirlyticsPurchase airlyticsPurchase) {
            super(airlyticsPurchase);
            if (airlyticsPurchase != null) {
                this.premium = airlyticsPurchase.getActive();
                this.premiumStartDate = airlyticsPurchase.getStartDate();
                this.premiumExpirationDate = airlyticsPurchase.getActualEndDate();
                this.premiumAutoRenewStatus = airlyticsPurchase.getAutoRenewStatus() == null ?
                        false : airlyticsPurchase.getAutoRenewStatus();
                this.premiumTrial = airlyticsPurchase.getTrial();
            }
        }

        public boolean isPremium() {
            return premium;
        }

        public boolean isPremiumTrial() {
            return premiumTrial;
        }

        public void setPremiumTrial(boolean premiumTrial) {
            this.premiumTrial = premiumTrial;
        }

        public String getPremiumProductId() {
            return premiumProductId;
        }

        public void setPremiumProductId(String premiumProductId) {
            this.premiumProductId = premiumProductId;
        }

        public long getPremiumExpirationDate() {
            return premiumExpirationDate;
        }

        public boolean isPremiumAutoRenewStatus() {
            return premiumAutoRenewStatus;
        }

        public void setPremiumAutoRenewStatus(boolean premiumAutoRenewStatus) {
            this.premiumAutoRenewStatus = premiumAutoRenewStatus;
        }

        public void setPremiumExpirationDate(long premiumExpirationDate) {
            this.premiumExpirationDate = premiumExpirationDate;
        }


        public long getPremiumStartDate() {
            return premiumStartDate;
        }

        public void setPremiumStartDate(long premiumStartDate) {
            this.premiumStartDate = premiumStartDate;
        }

        public Boolean getPremium() {
            return premium;
        }

        public void setPremium(boolean premium) {
            this.premium = premium;
        }
    }
}



