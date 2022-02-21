package com.ibm.airlytics.consumer.purchase.android;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.ibm.airlytics.consumer.purchase.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AndroidAirlyticsPurchaseEvent extends AndroidAirlyticsPurchaseState {


    private final List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents;
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public AndroidAirlyticsPurchaseEvent(SubscriptionPurchase subscriptionPurchase,
                                         PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                         AirlyticsInAppProduct airlyticsInAppProduct) {
        super(subscriptionPurchase, purchaseMetaData, airlyticsInAppProduct);
        airlyticsPurchaseEvents = new ArrayList<>();
    }


    public AndroidAirlyticsPurchaseEvent(SubscriptionPurchase subscriptionPurchase,
                                         PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                         AirlyticsInAppProduct airlyticsInAppProduct,
                                         AirlyticsPurchase currentState) {
        super(subscriptionPurchase, purchaseMetaData, airlyticsInAppProduct, currentState, null);

        airlyticsPurchaseEvents = new ArrayList<>();
    }

    public AndroidAirlyticsPurchaseEvent(SubscriptionPurchase subscriptionPurchase,
                                         PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                         AirlyticsInAppProduct airlyticsInAppProduct,
                                         AirlyticsPurchase currentState,
                                         Collection<AirlyticsPurchaseEvent> purchaseEventsHistory) {
        super(subscriptionPurchase, purchaseMetaData, airlyticsInAppProduct, currentState, purchaseEventsHistory);
        airlyticsPurchaseEvents = new ArrayList<>();
    }


    private void setBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent) {
        airlyticsPurchaseEvent.setPurchaseId(purchaseMetaData.getPurchaseToken());
        airlyticsPurchaseEvent.setExpirationDate(subscriptionPurchase.getExpiryTimeMillis());
        airlyticsPurchaseEvent.setAutoRenewStatus(subscriptionPurchase.getAutoRenewing());
        airlyticsPurchaseEvent.setProduct(purchaseMetaData.getProductId());
        airlyticsPurchaseEvent.setIntroPricing(subscriptionPurchase.getIntroductoryPriceInfo() != null);
        airlyticsPurchaseEvent.setPaymentState(AndroidPurchaseConsumer.PaymentState.
                getNameByState(subscriptionPurchase.getPaymentState()));
        airlyticsPurchaseEvent.setPlatform("android");
        airlyticsPurchaseEvent.setEventTime(purchaseMetaData.getNotificationTime());

        if (subscriptionPurchase.getCancelReason() != null &&
                AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                        != AndroidPurchaseConsumer.CancelReason.USER_CANCELED) {
            airlyticsPurchaseEvent.setCancellationReason(AndroidPurchaseConsumer.CancelReason.
                    getNameByReason(subscriptionPurchase.getCancelReason()));
        }

        if (subscriptionPurchase.getCancelSurveyResult() != null) {
            airlyticsPurchaseEvent.setSurveyResponse(AndroidPurchaseConsumer.
                    CancelSurveyReason.getNameByReason(subscriptionPurchase.getCancelSurveyResult().getCancelSurveyReason()));
        }
    }

    private void setBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, Long eventTime) {
        setBasicFields(airlyticsPurchaseEvent);
        airlyticsPurchaseEvent.setEventTime(eventTime);
    }

    private void addNewPurchaseSubscriptionEvent(SubscriptionPurchase subscriptionPurchase) {
        AirlyticsPurchaseEvent purchaseAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();

        // and PURCHASE event
        setBasicFields(purchaseAirlyticsPurchaseEvent, subscriptionPurchase.getStartTimeMillis());
        if (isInTrailPeriod()) {
            purchaseAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name());
        } else if (subscriptionPurchase.getLinkedPurchaseToken() != null) {
            purchaseAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.UPGRADE_PURCHASED.name());
            purchaseAirlyticsPurchaseEvent.setRevenueUsd(getPrice());
            purchaseAirlyticsPurchaseEvent.setUpgradedFrom(subscriptionPurchase.getLinkedPurchaseToken());
        } else {
            purchaseAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PURCHASED.name());
            purchaseAirlyticsPurchaseEvent.setRevenueUsd(getPrice());
        }

        airlyticsPurchaseEvents.add(purchaseAirlyticsPurchaseEvent);
    }


    public List<AirlyticsPurchaseEvent> getPurchaseEvents() throws JsonProcessingException {
        if (purchaseMetaData.getNotificationType() == null) {
            return getPurchaseEventFromRenewalsHistory();
        } else {
            return getPurchaseEventTriggeredByNotification();
        }
    }

    public List<AirlyticsPurchaseEvent> getPurchaseEventFromRenewalsHistory() {
        addNewPurchaseSubscriptionEvent(subscriptionPurchase);

        // we can't extract the renewal, the start renewals period is unknown
        // check whether a sub is already expired
        if (subscriptionPurchase.getCancelReason() != null && !isActive()) {
            AirlyticsPurchaseEvent expirationAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
            setBasicFields(expirationAirlyticsPurchaseEvent, subscriptionPurchase.getExpiryTimeMillis());
            processExpiration(expirationAirlyticsPurchaseEvent);
            airlyticsPurchaseEvents.add(expirationAirlyticsPurchaseEvent);
        }

        return airlyticsPurchaseEvents;
    }

    private long getPrice() {
        // check if the subscription in the introductory mode
        if (subscriptionPurchase.getIntroductoryPriceInfo() != null) {
            return airlyticsInAppProduct.getIntroPriceUSDMicros();
        } else {
            return airlyticsInAppProduct.getPriceUSDMicros();
        }
    }


    /**
     * Returns historical list of  all purchase events including the recently added
     *
     * @return list of all purchase events
     * @throws JsonProcessingException
     */
    public List<AirlyticsPurchaseEvent> getPurchaseEventTriggeredByNotification() throws JsonProcessingException {
        AirlyticsPurchaseEvent airlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
        setBasicFields(airlyticsPurchaseEvent);

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.PURCHASED.name())) {
            addNewPurchaseSubscriptionEvent(subscriptionPurchase);
            return airlyticsPurchaseEvents;
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.RENEWED.name())) {
            // if the current state is trial the renewed event is replaced by TRIAL_CONVERTED
            if (currentPurchaseState != null && currentPurchaseState.getTrial() != null && currentPurchaseState.getTrial()
                    || (currentPurchaseState != null && currentPurchaseState.getTrialStartDate() != null && currentPurchaseState.getPeriods() == 0)) {
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name());
            } else {
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWED.name());
                airlyticsPurchaseEvent.setEventTime(getPeriodStartDate(subscriptionPurchase));
            }
            airlyticsPurchaseEvent.setRevenueUsd(getPrice());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.RECOVERED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RECOVERED.name());

            AirlyticsPurchaseEvent renewedAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
            setBasicFields(renewedAirlyticsPurchaseEvent);

            if (currentPurchaseState != null && currentPurchaseState.getTrial() != null && currentPurchaseState.getTrial()
                    || (currentPurchaseState != null && currentPurchaseState.getTrialStartDate() != null && currentPurchaseState.getPeriods() == 0)) {
                renewedAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name());
            } else {
                renewedAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWED.name());
            }
            renewedAirlyticsPurchaseEvent.setRevenueUsd(getPrice());
            airlyticsPurchaseEvents.add(renewedAirlyticsPurchaseEvent);
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.ON_HOLD.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PLACED_ON_HOLD.name());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.IN_GRACE_PERIOD.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.IN_GRACE_PERIOD.name());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.CANCELED.name())
                || purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.RESTARTED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWAL_STATUS_CHANGED.name());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.EXPIRED.name())) {
            processExpiration(airlyticsPurchaseEvent);
            // in the case of expiration set the actual expiration date instead of notification date
            airlyticsPurchaseEvent.setEventTime(subscriptionPurchase.getExpiryTimeMillis());
        }

        // revoke with refund only if there was at least one paid period
        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.REVOKED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.CANCELED.name());
            airlyticsPurchaseEvent.setRevenueUsd((getPaidPeriodsFromPurchaseEventsHistory() > 0) ? -getPrice() : 0);
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.PRICE_CHANGE_CONFIRMED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PRICE_CHANGE_CONFIRMED.name());
            airlyticsPurchaseEvent.setRevenueUsd(-getPrice());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.PAUSED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PAUSED.name());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.PAUSE_SCHEDULE_CHANGED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PAUSE_SCHEDULE_CHANGED.name());
        }

        if (purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.DEFERRED.name())) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.DEFERRED.name());
        }

        airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);

        // run through all events and add to receipt
        for (AirlyticsPurchaseEvent event : airlyticsPurchaseEvents) {
            event.setReceipt(objectMapper.writeValueAsString(subscriptionPurchase));
        }
        return airlyticsPurchaseEvents;
    }


    private void processExpiration(AirlyticsPurchaseEvent airlyticsPurchaseEvent){
        if (currentPurchaseState != null && currentPurchaseState.getTrial() != null && currentPurchaseState.getTrial()
                || (currentPurchaseState != null && currentPurchaseState.getTrialStartDate() != null && currentPurchaseState.getPeriods() == 0)) {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name());
        } else {
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.EXPIRED.name());
        }

        // the sub was replaced by another sub
        // upgrades, downgrades, or resigns
        if (subscriptionPurchase.getCancelReason() != null && AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                == AndroidPurchaseConsumer.CancelReason.REPLACED_WITH_SUB) {
            if (currentPurchaseState != null && (currentPurchaseState.getTrial() == null || !currentPurchaseState.getTrial())) {
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.UPGRADED.name());
                airlyticsPurchaseEvent.setRevenueUsd(-(long) (getLastPartialPeriod() * getPrice()));
            }
        }

        // in the case of expiration set the actual expiration date instead of notification date
        airlyticsPurchaseEvent.setEventTime(subscriptionPurchase.getExpiryTimeMillis());
    }
}
