package com.ibm.airlytics.consumer.purchase.android;

import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.utilities.Duration;
import com.ibm.airlytics.utilities.MathUtils;

import java.util.Collection;
import java.util.Collections;


class AndroidAirlyticsPurchaseState {

    protected final AirlyticsInAppProduct airlyticsInAppProduct;
    protected final SubscriptionPurchase subscriptionPurchase;
    protected final AirlyticsPurchase updatedPurchase;
    protected final PurchaseConsumer.PurchaseMetaData purchaseMetaData;
    protected final long subscriptionDurationInMilliSeconds;
    protected long DATE_UTC_FIRST_PURCHASE_NOTIFICATION_EVENT_IS_COLLECTED = 1614549600000L;


    protected AirlyticsPurchase currentPurchaseState;
    protected Collection<AirlyticsPurchaseEvent> purchaseEventsHistory;


    AndroidAirlyticsPurchaseState(SubscriptionPurchase subscriptionPurchase,
                                  PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                  AirlyticsInAppProduct airlyticsInAppProduct) {
        this(subscriptionPurchase, purchaseMetaData, airlyticsInAppProduct, null, null);
    }

    AndroidAirlyticsPurchaseState(SubscriptionPurchase subscriptionPurchase,
                                  PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                  AirlyticsInAppProduct airlyticsInAppProduct, AirlyticsPurchase currentPurchaseState,
                                  Collection<AirlyticsPurchaseEvent> purchaseEventsHistory) {
        this.airlyticsInAppProduct = airlyticsInAppProduct;
        this.subscriptionPurchase = subscriptionPurchase;
        this.purchaseMetaData = purchaseMetaData;
        this.currentPurchaseState = currentPurchaseState;
        this.purchaseEventsHistory = purchaseEventsHistory == null ? Collections.emptyList() : purchaseEventsHistory;
        this.subscriptionDurationInMilliSeconds = Duration.durationToMilliSeconds(airlyticsInAppProduct.getSubscriptionPeriod());
        this.updatedPurchase = new AirlyticsPurchase();
    }

    private AirlyticsPurchase updateAirlyticsPurchase() {
        updatedPurchase.setId(purchaseMetaData.getPurchaseToken());
        updatedPurchase.setPlatform(purchaseMetaData.getPlatform());
        updatedPurchase.setProduct(purchaseMetaData.getProductId());
        setAirlyticsPurchaseBasicFields(updatedPurchase, this.subscriptionPurchase);

        updatePeriods();
        updateRevenueUsd();
        updateIsActive();
        updatesRenewal();
        updateIsGrace();
        updateIsTrial();
        updatePeriodStartDate();
        updateAutoRenewStatusChangeDate();
        updateRenewalCancellationReason();
        updateRenewalCancellationDate();
        updateModifierSource();
        updateRefundDate();
        updateSubscriptionStatus();
        updateIsUpgraded();
        return updatedPurchase;
    }

    private void updateIsUpgraded() {
        if (subscriptionPurchase.getCancelReason() != null && AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                == AndroidPurchaseConsumer.CancelReason.REPLACED_WITH_SUB) {
            updatedPurchase.setUpgraded(true);
        }
    }

    private void updateSubscriptionStatus() {
        if (purchaseMetaData.getNotificationType() != null &&
                purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.ON_HOLD.name())) {
            updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.ON_HOLD.getStatus());
        }
        if (purchaseMetaData.getNotificationType() != null &&
                purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.PAUSED.name())) {
            updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.PAUSED.getStatus());
        }
    }

    private void updateModifierSource() {
        if (purchaseMetaData.getNotificationType() != null) {
            updatedPurchase.setModifierSource(
                    purchaseMetaData.getNotificationType());
        } else if (purchaseMetaData.getType() != PurchaseConsumer.PurchaseMetaData.Type.RENEWAL_CHECK) {
            updatedPurchase.setModifierSource(purchaseMetaData.getType().name());
        }
    }

    private void updateRenewalCancellationReason() {
        if (subscriptionPurchase.getCancelReason() != null &&
                AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                        != AndroidPurchaseConsumer.CancelReason.USER_CANCELED) {
            updatedPurchase.setCancellationReason(AndroidPurchaseConsumer.CancelReason.getNameByReason(subscriptionPurchase.getCancelReason()));
        }
    }

    /**
     * A subscription can be revoked from a user for a variety of reasons,
     * including your app revoking the subscription by
     * using or the purchase being charged back.
     * A SUBSCRIPTION_REVOKED notification is also sent when this occurs.
     * When you receive this notification, the subscription resource returned
     * from the Google Play Developer API contains autoRenewing = false,
     * and expiryTimeMillis contains the date when the user should lose access to the subscription.
     */
    private void updateRefundDate() {
        if (purchaseMetaData.getType() == PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION
                && purchaseMetaData.getNotificationType().equals(AndroidPurchaseConsumer.NotificationType.REVOKED.name())) {
            updatedPurchase.setCancellationDate(subscriptionPurchase.getExpiryTimeMillis());
            updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.CANCELED.getStatus());
        }
    }

    private void updateRenewalCancellationDate() {
        if (subscriptionPurchase.getCancelReason() != null &&
                AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                        != AndroidPurchaseConsumer.CancelReason.USER_CANCELED) {
            updatedPurchase.setCancellationDate(subscriptionPurchase.getExpiryTimeMillis());
            updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.CANCELED.getStatus());
        }
    }


    protected long getPeriodStartDate(SubscriptionPurchase subscriptionPurchase) {

        // if the purchase was expired before original expiration data
        // it means the sub was upgrade/downgraded/resigned.
        // the start period date stays unchanged.
        if (AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                != AndroidPurchaseConsumer.CancelReason.REPLACED_WITH_SUB) {
            return Math.max(subscriptionPurchase.getStartTimeMillis(),
                    subscriptionPurchase.getExpiryTimeMillis() - subscriptionDurationInMilliSeconds);
        }

        return currentPurchaseState == null ? subscriptionPurchase.getStartTimeMillis() :
                currentPurchaseState.getPeriodStartDate();
    }

    private void updatePeriodStartDate() {
        updatedPurchase.setPeriodStartDate(getPeriodStartDate(subscriptionPurchase));
    }

    private void setAirlyticsPurchaseBasicFields(AirlyticsPurchase airlyticsPurchase, SubscriptionPurchase subscriptionPurchase) {
        airlyticsPurchase.setStartDate(subscriptionPurchase.getStartTimeMillis());
        airlyticsPurchase.setLastPaymentActionDate(subscriptionPurchase.getStartTimeMillis());
        airlyticsPurchase.setExpirationDate(subscriptionPurchase.getExpiryTimeMillis());
        airlyticsPurchase.setLinkedPurchaseToken(subscriptionPurchase.getLinkedPurchaseToken());
        airlyticsPurchase.setPurchaseIdUpgradedFrom(subscriptionPurchase.getLinkedPurchaseToken());
        airlyticsPurchase.setAutoRenewStatus(subscriptionPurchase.getAutoRenewing());
        airlyticsPurchase.setPaymentState(AndroidPurchaseConsumer.PaymentState.getNameByState(subscriptionPurchase.getPaymentState()));
        airlyticsPurchase.setIntroPricing(subscriptionPurchase.getIntroductoryPriceInfo() != null);
        airlyticsPurchase.setDuration(Duration.Periodicity.getNameByType(airlyticsInAppProduct.getSubscriptionPeriod()));
        airlyticsPurchase.setActualEndDate(subscriptionPurchase.getExpiryTimeMillis());
        airlyticsPurchase.setLastModifiedDate(System.currentTimeMillis());

        if (subscriptionPurchase.getCancelSurveyResult() != null) {
            updatedPurchase.setSurveyResponse(AndroidPurchaseConsumer.CancelSurveyReason.getNameByReason(subscriptionPurchase.getCancelSurveyResult().getCancelSurveyReason()));
        }
    }

    private void updatePeriods() {
        updatedPurchase.setPeriods(getPeriods());
    }

    /**
     * Order numbers for subscription renewals contain an additional integer that
     * represents a specific renewal instance. For example,
     * an initial subscription Order ID might be GPA.1234-5678-9012-34567 with subsequent
     * Order IDs being GPA.1234-5678-9012-34567..0 (first renewal),
     * GPA.1234-5678-9012-34567..1 (second renewal), and so on.
     *
     * @return returns double compatible with DB type
     */
    private double getPeriods() {

        if (isInTrailPeriod()) {
            return 0;
        }

        double renewalsPeriods = extractPeriodFromOrder(subscriptionPurchase.getOrderId(),
                AndroidPurchaseConsumer.PaymentState.
                        getValueByState(subscriptionPurchase.getPaymentState()));
        return renewalsPeriods;
    }

    private void updatesRenewal() {
        updatedPurchase.setAutoRenewStatus(subscriptionPurchase.getAutoRenewing());
    }

    private void updateIsGrace() {
        if (purchaseMetaData.getType() == PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION) {
            updatedPurchase.setGrace(purchaseMetaData.getNotificationType().
                    equals(AndroidPurchaseConsumer.NotificationType.IN_GRACE_PERIOD.name()));
        }
    }

    /**
     * The active column calculation is true if  is start-date < current-data < expiration date otherwise false
     */
    private void updateIsActive() {
        //set active
        updatedPurchase.setActive(isActive());
        if (isActive()) {
            updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus());
        } else {
            updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
        }
    }

    protected boolean isActive() {
        //set active
        return subscriptionPurchase.getExpiryTimeMillis() > System.currentTimeMillis() &&
                subscriptionPurchase.getStartTimeMillis() < System.currentTimeMillis();
    }


    private void updateIsTrial() {
        // trial
        updatedPurchase.setTrial(isInTrailPeriod());
        if (updatedPurchase.getTrial()) {
            updatedPurchase.setTrialStartDate(subscriptionPurchase.getStartTimeMillis());
            updatedPurchase.setTrialEndDate(subscriptionPurchase.getExpiryTimeMillis());
        }
    }


    protected boolean isInTrailPeriod() {
        if (airlyticsInAppProduct.getTrialPeriod() == null) {
            return false;
        }
        return (currentPurchaseState != null && currentPurchaseState.getTrialStartDate() != null && ((Duration.durationToMilliSeconds(airlyticsInAppProduct.getTrialPeriod()) + Duration.FOUR_DAYS_IN_MILLISECOND) >
                subscriptionPurchase.getExpiryTimeMillis() - subscriptionPurchase.getStartTimeMillis()))
                || AndroidPurchaseConsumer.PaymentState.
                getValueByState(subscriptionPurchase.getPaymentState()) == AndroidPurchaseConsumer.PaymentState.FREE_TRIAL;
    }

    private void updateRevenueUsd() {

        // check if the subscription in the introductory mode
        if (subscriptionPurchase.getIntroductoryPriceInfo() != null) {
            updatedPurchase.setRevenueUsd(updatedPurchase.getRevenueUsd() +
                    airlyticsInAppProduct.getIntroPriceUSDMicros());
        } else {
            updatedPurchase.setRevenueUsd((long) (updatedPurchase.getRevenueUsd() +
                    (airlyticsInAppProduct.getPriceUSDMicros()
                            * getPeriods())));
        }
    }

    protected double getLastPartialPeriod() {
        if (currentPurchaseState == null) {
            return 0;
        }
        return 1 - Math.min(MathUtils.roundOff((((double) (subscriptionPurchase.getExpiryTimeMillis() - currentPurchaseState.getPeriodStartDate())) /
                (double) subscriptionDurationInMilliSeconds), 3), 1);
    }


    private void updateAutoRenewStatusChangeDate() {
        if (subscriptionPurchase.getCancelReason() != null &&
                AndroidPurchaseConsumer.CancelReason.getValueByReason(subscriptionPurchase.getCancelReason())
                        == AndroidPurchaseConsumer.CancelReason.USER_CANCELED) {
            updatedPurchase.setAutoRenewStatusChangeDate(subscriptionPurchase.getUserCancellationTimeMillis());
        }
    }

    AirlyticsPurchase getUpdatedPurchase() {
        return updateAirlyticsPurchase();
    }


    private boolean isProductWithTrialPeriod() {
        return null != airlyticsInAppProduct.getTrialPeriod();
    }

    protected int getPaidPeriodsFromPurchaseEventsHistory() {
        return (int) this.purchaseEventsHistory.stream().filter(n -> n.getRevenueUsd() > 0).count();
    }

    private double extractPeriodFromOrder(String orderId, AndroidPurchaseConsumer.PaymentState paymentState) {

        // Order numbers for subscription renewals contain
        // an additional integer that represents a specific renewal instance.
        // For example, an initial subscription Order ID might be GPA.1234-5678-9012-34567
        // with subsequent Order IDs being GPA.1234-5678-9012-34567..0 (first renewal),
        // GPA.1234-5678-9012-34567..1 (second renewal), and so on.

        double period;
        if ((orderId == null || orderId.length() < 25)) {
            if (currentPurchaseState != null && currentPurchaseState.getTrialStartDate() != null && currentPurchaseState.getTrial()) {
                return 0;
            } else {
                period = 1;
            }
        } else {
            period = Integer.parseInt(orderId.substring(orderId.length() - 3).replace(".", ""));
            int periodStep = ((currentPurchaseState != null && currentPurchaseState.getTrialStartDate() != null) || isProductWithTrialPeriod()) ? 1 : 2;

            // increment the renewals only if the last payment state is AndroidPurchaseConsumer.PaymentState.PAYMENT_RECEIVED
            // the incrementation step depends on the sub type 1 - for trial, 2 - non-trial.
            if (paymentState == AndroidPurchaseConsumer.PaymentState.PENDING) {
                period = period + periodStep - 1;
            } else {
                period = period + periodStep;
            }
        }


        // if a purchase has a full notification history
        // a notification event sequence should be started with either of two events
        // PURCHASED
        // TRIAL_STARTED
        // the payment period will be calculated using notification history
        if (isPurchaseEventsHistoryIsFull()) {
            period = this.purchaseEventsHistory.stream().mapToDouble(AirlyticsPurchaseEvent::getRevenueUsd).sum() /
                    airlyticsInAppProduct.getPriceUSDMicros();

        }

        return period;
    }

    private boolean isPurchaseEventsHistoryIsFull() {
        if (this.purchaseEventsHistory != null && this.purchaseEventsHistory.size() > 0 &&
                this.purchaseEventsHistory.stream().anyMatch(event ->
                        ((event.getName().equals(PurchaseConsumer.PurchaseEventType.PURCHASED.name()) &&
                                event.getEventTime() > DATE_UTC_FIRST_PURCHASE_NOTIFICATION_EVENT_IS_COLLECTED) ||
                                (event.getName().equals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name()) &&
                                        event.getEventTime() > DATE_UTC_FIRST_PURCHASE_NOTIFICATION_EVENT_IS_COLLECTED)))) {
            return true;

        }
        return false;
    }
}
