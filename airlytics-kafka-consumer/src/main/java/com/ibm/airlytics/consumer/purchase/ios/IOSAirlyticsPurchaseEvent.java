package com.ibm.airlytics.consumer.purchase.ios;

import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.utilities.Duration;
import com.ibm.airlytics.utilities.MathUtils;


import java.util.*;


public class IOSAirlyticsPurchaseEvent extends IOSAirlyticsPurchaseState {


    public IOSAirlyticsPurchaseEvent(IOSubscriptionPurchase ioSubscriptionPurchase,
                                     PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                     Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts) {
        super(ioSubscriptionPurchase, purchaseMetaData, inAppAirlyticsProducts);
    }

    public List<AirlyticsPurchaseEvent> getPurchaseEvents() {

        List<RenewalsHistoricalSequence> renewalHistoricalSequences = groupSubscriptionByRenewals();
        List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = new ArrayList<>();
        if (purchaseMetaData.getNotificationType() == null) {
            for (RenewalsHistoricalSequence renewalHistoricalSequence : renewalHistoricalSequences) {
                if (renewalHistoricalSequence.isRenewableProduct()) {
                    airlyticsPurchaseEvents.addAll(getPurchaseEventFromRenewalsHistory(renewalHistoricalSequence));
                }
            }
        } else {
            airlyticsPurchaseEvents.addAll(
                    getPurchaseEventByNotification(renewalHistoricalSequences.get(renewalHistoricalSequences.size() - 1)));
        }


        return airlyticsPurchaseEvents;
    }

    private void setBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo,
                                RenewalsHistoricalSequence renewalsHistoricalSequence) {
        airlyticsPurchaseEvent.setPurchaseId(renewalsHistoricalSequence.getPurchaseTransactionId());
        airlyticsPurchaseEvent.setExpirationDate(latestReceiptInfo.getExpiresDateMs());
        airlyticsPurchaseEvent.setAutoRenewStatus(renewalsHistoricalSequence.getAutoRenewStatus(latestReceiptInfo));
        airlyticsPurchaseEvent.setProduct(latestReceiptInfo.getProductId());
        airlyticsPurchaseEvent.setIntroPricing(latestReceiptInfo.getIsInIntroOfferPeriod() == null ? false : latestReceiptInfo.getIsInIntroOfferPeriod());
        airlyticsPurchaseEvent.setPlatform("ios");
    }

    private void setBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo,
                                String purchaseId, boolean autoRenewStatus) {
        airlyticsPurchaseEvent.setPurchaseId(purchaseId);
        airlyticsPurchaseEvent.setExpirationDate(latestReceiptInfo.getExpiresDateMs());
        airlyticsPurchaseEvent.setAutoRenewStatus
                (autoRenewStatus);
        airlyticsPurchaseEvent.setProduct(latestReceiptInfo.getProductId());
        airlyticsPurchaseEvent.setPlatform("ios");
    }

    public List<AirlyticsPurchaseEvent> getPurchaseEventFromRenewalsHistory(RenewalsHistoricalSequence renewalsHistoricalSequence) {
        List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = new ArrayList<>();
        LatestReceiptInfo previousLatestReceiptInfo = null;


        for (LatestReceiptInfo latestReceiptInfo : renewalsHistoricalSequence.getLatestReceiptInfos()) {
            AirlyticsPurchaseEvent airlyticsPurchaseEvent = new AirlyticsPurchaseEvent();


            if (latestReceiptInfo.getWebOrderLineItemId().equals(renewalsHistoricalSequence.getPurchaseTransactionId()) &&
                    renewalsHistoricalSequence.getUpgradedFromProductReceipt() == null) {
                setBasicFields(airlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());

                if (latestReceiptInfo.getIsTrialPeriod()) {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name());
                } else {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PURCHASED.name());
                    airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
                }
                airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);

                if (latestReceiptInfo.getCancellationDateMs() != null && !latestReceiptInfo.isUpgraded()) {
                    // add upgrade of previous sub
                    AirlyticsPurchaseEvent canceledAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
                    setBasicFields(canceledAirlyticsPurchaseEvent, latestReceiptInfo, latestReceiptInfo.getTransactionId(), false);
                    canceledAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.CANCELED.name());
                    canceledAirlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getCancellationDateMs());
                    canceledAirlyticsPurchaseEvent.setRevenueUsd(-(long) ((1 - getPeriods(latestReceiptInfo, purchaseMetaData))
                            * getPrice(latestReceiptInfo)));
                    airlyticsPurchaseEvents.add(canceledAirlyticsPurchaseEvent);
                }
            } else if (renewalsHistoricalSequence.getUpgradedFromProductReceipt() != null) {

                LatestReceiptInfo upgradedFromReceipt = renewalsHistoricalSequence.getUpgradedFromProductReceipt();
                // add upgrade of previous sub
                AirlyticsPurchaseEvent upgradedAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
                setBasicFields(upgradedAirlyticsPurchaseEvent, upgradedFromReceipt, upgradedFromReceipt.getTransactionId(), false);
                upgradedAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.UPGRADED.name());
                upgradedAirlyticsPurchaseEvent.setEventTime(upgradedFromReceipt.getCancellationDateMs());
                upgradedAirlyticsPurchaseEvent.setRevenueUsd(-(long) (getPrice(upgradedFromReceipt) - (getPeriods(upgradedFromReceipt, purchaseMetaData))
                        * getPrice(upgradedFromReceipt)));
                upgradedAirlyticsPurchaseEvent.setUpgradedTo(renewalsHistoricalSequence.getPurchaseTransactionId());
                airlyticsPurchaseEvents.add(upgradedAirlyticsPurchaseEvent);


                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.UPGRADE_PURCHASED.name());
                setBasicFields(airlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);
                airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));

                airlyticsPurchaseEvent.setEventTime(upgradedFromReceipt.getCancellationDateMs());
                airlyticsPurchaseEvent.setUpgradedFrom(renewalsHistoricalSequence.getPreviousPurchaseId());
                // shouldn't be added twice
                renewalsHistoricalSequence.setUpgradedFromProductReceipt(null);

                airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);

            } else if (latestReceiptInfo.getCancellationDate() != null && !latestReceiptInfo.isUpgraded()) {


                if (previousLatestReceiptInfo != null && previousLatestReceiptInfo.getIsTrialPeriod() != null
                        && previousLatestReceiptInfo.getIsTrialPeriod().equals(true)) {
                    AirlyticsPurchaseEvent trialConverted = new AirlyticsPurchaseEvent();
                    setBasicFields(trialConverted, latestReceiptInfo, renewalsHistoricalSequence);
                    trialConverted.setName(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name());
                    trialConverted.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                    trialConverted.setRevenueUsd(getPrice(latestReceiptInfo));
                    airlyticsPurchaseEvents.add(trialConverted);
                } else {
                    // add renew before cancelled
                    AirlyticsPurchaseEvent renewalBeforeCancelAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
                    setBasicFields(renewalBeforeCancelAirlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);
                    renewalBeforeCancelAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWED.name());
                    renewalBeforeCancelAirlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                    renewalBeforeCancelAirlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
                    airlyticsPurchaseEvents.add(renewalBeforeCancelAirlyticsPurchaseEvent);
                }


                // add cancelled
                setBasicFields(airlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.CANCELED.name());
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getCancellationDateMs());
                airlyticsPurchaseEvent.setRevenueUsd(-getPrice(latestReceiptInfo));

                airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);
            } else if (previousLatestReceiptInfo != null && previousLatestReceiptInfo.getIsTrialPeriod() != null
                    && previousLatestReceiptInfo.getIsTrialPeriod().equals(true)) {

                setBasicFields(airlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name());
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
                airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);

            } else {
                setBasicFields(airlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWED.name());
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));

                airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);
            }

            previousLatestReceiptInfo = latestReceiptInfo;

        }

        // check the pending_renewal_info for expiration_intent field to determine if the sub is expired
        AirlyticsPurchaseEvent expirationEvent = getExpirationEventIfExits(renewalsHistoricalSequence);
        if (expirationEvent != null) {
            airlyticsPurchaseEvents.add(expirationEvent);
        }

        return airlyticsPurchaseEvents;
    }


    private long getPrice(LatestReceiptInfo latestReceiptInfo) {
        if (latestReceiptInfo.getIsInIntroOfferPeriod() == null ||
                latestReceiptInfo.getIsInIntroOfferPeriod() == false) {
            return inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getPriceUSDMicros();
        } else {
            return inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getIntroPriceUSDMicros();
        }
    }

    public List<AirlyticsPurchaseEvent> getPurchaseEventByNotification(RenewalsHistoricalSequence renewalsHistoricalSequence) {
        List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = new ArrayList<>();
        AirlyticsPurchaseEvent airlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
        AirlyticsPurchase airlyticsPurchase = renewalsHistoricalSequence.getAirlyticsPurchase();

        LatestReceiptInfo latestReceiptInfo = renewalsHistoricalSequence.getLatestReceiptInfos().last();
        LatestReceiptInfo firstLatestReceiptInfo = null;
        if (renewalsHistoricalSequence.getLatestReceiptInfos().size() == 2) {
            firstLatestReceiptInfo = renewalsHistoricalSequence.getLatestReceiptInfos().first();
        }
        setBasicFields(airlyticsPurchaseEvent,
                latestReceiptInfo,
                renewalsHistoricalSequence);

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.DID_RENEW.name())) {

            if (renewalsHistoricalSequence.getLatestReceiptInfos().size() == 1) {
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                if (latestReceiptInfo.getIsTrialPeriod()) {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name());
                } else {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PURCHASED.name());
                    airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
                }
            } else if (firstLatestReceiptInfo != null && firstLatestReceiptInfo.getIsTrialPeriod() != null &&
                    firstLatestReceiptInfo.getIsTrialPeriod().equals(true)) {
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name());
                airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
            } else {
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWED.name());
                airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
            }
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.INITIAL_BUY.name())) {
            airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
            if (latestReceiptInfo.getIsTrialPeriod()) {
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name());
            } else {
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PURCHASED.name());
                airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
            }
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.CANCEL.name())) {
            if (renewalsHistoricalSequence.getUpgradedFromProductReceipt() != null) {
                handleUpgradeNotification(renewalsHistoricalSequence, airlyticsPurchaseEvent, latestReceiptInfo, airlyticsPurchaseEvents);
            } else {
                airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.CANCELED.name());
                airlyticsPurchaseEvent.setCancellationReason(IOSPurchaseConsumer.CancellationReason.getNameByType(latestReceiptInfo.getCancellationReason(),
                        latestReceiptInfo.isUpgraded()));
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getCancellationDateMs() == null ? purchaseMetaData.getCancellationDateMs() :
                        latestReceiptInfo.getCancellationDateMs());

                if (latestReceiptInfo.getIsTrialPeriod() != null && latestReceiptInfo.getIsTrialPeriod() == true) {
                    airlyticsPurchaseEvent.setRevenueUsd(0L);
                } else {
                    airlyticsPurchaseEvent.setRevenueUsd(-getPrice(latestReceiptInfo));
                }
            }
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.DID_FAIL_TO_RENEW.name())) {
            PendingRenewalInfo pendingRenewalInfo = getPendingRenewalInfo(latestReceiptInfo);
            if (pendingRenewalInfo != null && pendingRenewalInfo.getAutoRenewProductId().equals(latestReceiptInfo.getProductId())) {

                airlyticsPurchaseEvent.setEventTime(System.currentTimeMillis());

                if (pendingRenewalInfo.getExpirationIntent() != null &&
                        pendingRenewalInfo.getExpirationIntent() == IOSPurchaseConsumer.ExpirationReason.BILLING_ISSUE.getType()) {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.BILLING_ISSUE.name());
                } else {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWAL_FAILED.name());
                }

                if (pendingRenewalInfo.getExpirationIntent() != null) {
                    airlyticsPurchaseEvent.setExpirationReason(IOSPurchaseConsumer.
                            ExpirationReason.getNameByType(pendingRenewalInfo.getExpirationIntent()));
                }
            } else {
                return Collections.emptyList();
            }
        }


        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.DID_RECOVER.name()) ||
                purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.RENEWAL.name())) {
            airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());

            AirlyticsPurchaseEvent recoveredAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
            setBasicFields(recoveredAirlyticsPurchaseEvent,
                    latestReceiptInfo,
                    renewalsHistoricalSequence);
            recoveredAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RECOVERED.name());
            recoveredAirlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
            airlyticsPurchaseEvents.add(recoveredAirlyticsPurchaseEvent);


            airlyticsPurchaseEvent.setName(airlyticsPurchase.getPeriods() == 1 &&
                    airlyticsPurchase.getTrialStartDate() != null ? PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name() :
                    PurchaseConsumer.PurchaseEventType.RENEWED.name());
            airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name())) {
            if (renewalsHistoricalSequence.getUpgradedFromProductReceipt() != null) {
                handleUpgradeNotification(renewalsHistoricalSequence, airlyticsPurchaseEvent, latestReceiptInfo, airlyticsPurchaseEvents);
            } else {
                airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getPurchaseDateMs());
                if (latestReceiptInfo.getIsTrialPeriod()) {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name());
                } else {
                    airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.PURCHASED.name());
                    airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));
                }
            }
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.DID_CHANGE_RENEWAL_PREF.name())) {
            airlyticsPurchaseEvent.setEventTime(System.currentTimeMillis());
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWAL_PLAN_CHANGED.name());
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.DID_CHANGE_RENEWAL_STATUS.name())) {
            airlyticsPurchaseEvent.setEventTime(purchaseMetaData.getAutoRenewStatusChangeDateMs());
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWAL_STATUS_CHANGED.name());
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.REFUND.name())) {
            airlyticsPurchaseEvent.setEventTime(latestReceiptInfo.getCancellationDateMs() == null ? purchaseMetaData.getCancellationDateMs() :
                    latestReceiptInfo.getCancellationDateMs());
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.CANCELED.name());
            airlyticsPurchaseEvent.setCancellationReason(IOSPurchaseConsumer.CancellationReason.getNameByType(latestReceiptInfo.getCancellationReason(),
                    latestReceiptInfo.isUpgraded()));


            airlyticsPurchaseEvent.setRevenueUsd(-getPrice(latestReceiptInfo));
        }

        if (purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.CONSUMPTION_REQUEST.name())) {
            airlyticsPurchaseEvent.setEventTime(System.currentTimeMillis());
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.CONSUMPTION_REQUEST.name());
        }

        airlyticsPurchaseEvents.add(airlyticsPurchaseEvent);
        // check the pending_renewal_info for expiration_intent field to determine if the sub is expired
        AirlyticsPurchaseEvent expirationEvent = getExpirationEventIfExits(renewalsHistoricalSequence);
        if (expirationEvent != null) {
            airlyticsPurchaseEvents.add(expirationEvent);
        }
        return airlyticsPurchaseEvents;
    }


    private void handleUpgradeNotification(RenewalsHistoricalSequence renewalsHistoricalSequence, AirlyticsPurchaseEvent airlyticsPurchaseEvent,
                                           LatestReceiptInfo latestReceiptInfo,
                                           List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents) {
        LatestReceiptInfo upgradedFromReceipt = renewalsHistoricalSequence.getUpgradedFromProductReceipt();
        // add upgrade of previous sub
        AirlyticsPurchaseEvent upgradedAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
        setBasicFields(upgradedAirlyticsPurchaseEvent, upgradedFromReceipt, upgradedFromReceipt.getTransactionId(), false);
        upgradedAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.UPGRADED.name());
        upgradedAirlyticsPurchaseEvent.setEventTime(upgradedFromReceipt.getCancellationDateMs() == null ? purchaseMetaData.getCancellationDateMs() :
                upgradedFromReceipt.getCancellationDateMs());
        upgradedAirlyticsPurchaseEvent.setRevenueUsd(-(long) (getPrice(upgradedFromReceipt) - (getPeriods(upgradedFromReceipt, purchaseMetaData))
                * getPrice(upgradedFromReceipt)));
        upgradedAirlyticsPurchaseEvent.setUpgradedTo(renewalsHistoricalSequence.getPurchaseTransactionId());
        airlyticsPurchaseEvents.add(upgradedAirlyticsPurchaseEvent);


        airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.UPGRADE_PURCHASED.name());
        setBasicFields(airlyticsPurchaseEvent, latestReceiptInfo, renewalsHistoricalSequence);

        airlyticsPurchaseEvent.setEventTime(upgradedFromReceipt.getCancellationDateMs() == null ? purchaseMetaData.getCancellationDateMs() :
                upgradedFromReceipt.getCancellationDateMs());
        airlyticsPurchaseEvent.setRevenueUsd(getPrice(latestReceiptInfo));

        airlyticsPurchaseEvent.setUpgradedFrom(upgradedFromReceipt.getTransactionId());
        // shouldn't be added twice
        renewalsHistoricalSequence.setUpgradedFromProductReceipt(null);
    }

    private PendingRenewalInfo getPendingRenewalInfo(LatestReceiptInfo latestReceiptInfo) {
        for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
            if (pendingRenewalInfo.getProductId().equals(latestReceiptInfo.getProductId())) {
                return pendingRenewalInfo;
            }
        }
        return null;
    }

    private AirlyticsPurchaseEvent getExpirationEventIfExits(RenewalsHistoricalSequence renewalsHistoricalSequence) {
        for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
            if (pendingRenewalInfo.getProductId().equals(renewalsHistoricalSequence.getProductId()) &&
                    pendingRenewalInfo.getExpirationIntent() != null &&
                    pendingRenewalInfo.getExpirationIntent() == IOSPurchaseConsumer.ExpirationReason.CANCELED.getType()) {
                AirlyticsPurchaseEvent airlyticsPurchaseExpirationEvent = new AirlyticsPurchaseEvent();
                setBasicFields(airlyticsPurchaseExpirationEvent,
                        renewalsHistoricalSequence.getLatestReceiptInfos().last(),
                        renewalsHistoricalSequence);

                if (renewalsHistoricalSequence.getLatestReceiptInfos().size() == 1) {
                    LatestReceiptInfo trialExpiredReceiptInfo = renewalsHistoricalSequence.getLatestReceiptInfos().last();
                    if (trialExpiredReceiptInfo.getIsTrialPeriod() != null && trialExpiredReceiptInfo.getCancellationReason() == null
                            && trialExpiredReceiptInfo.isUpgraded() == false &&
                            trialExpiredReceiptInfo.getIsTrialPeriod().equals(true)) {
                        AirlyticsPurchaseEvent trialExpiredAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
                        setBasicFields(trialExpiredAirlyticsPurchaseEvent, trialExpiredReceiptInfo, renewalsHistoricalSequence);
                        trialExpiredAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name());
                        trialExpiredAirlyticsPurchaseEvent.setEventTime(trialExpiredReceiptInfo.getExpiresDateMs());
                        return trialExpiredAirlyticsPurchaseEvent;
                    }
                }

                airlyticsPurchaseExpirationEvent.setExpirationReason(IOSPurchaseConsumer.ExpirationReason.getNameByType(pendingRenewalInfo.getExpirationIntent()));
                airlyticsPurchaseExpirationEvent.setName(PurchaseConsumer.PurchaseEventType.EXPIRED.name());
                airlyticsPurchaseExpirationEvent.setEventTime(renewalsHistoricalSequence.getLatestReceiptInfos().last().getExpiresDateMs());
                return airlyticsPurchaseExpirationEvent;
            }
        }

        if (renewalsHistoricalSequence.getLatestReceiptInfos().size() == 1) {
            LatestReceiptInfo trialExpiredReceiptInfo = renewalsHistoricalSequence.getLatestReceiptInfos().last();
            if (trialExpiredReceiptInfo.getIsTrialPeriod() != null && trialExpiredReceiptInfo.getCancellationReason() == null
                    && trialExpiredReceiptInfo.isUpgraded() == false &&
                    trialExpiredReceiptInfo.getIsTrialPeriod().equals(true) &&
                    trialExpiredReceiptInfo.getExpiresDateMs() + Duration.FOUR_DAYS_IN_MILLISECOND < System.currentTimeMillis()) {
                AirlyticsPurchaseEvent trialExpiredAirlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
                setBasicFields(trialExpiredAirlyticsPurchaseEvent, trialExpiredReceiptInfo, renewalsHistoricalSequence);
                trialExpiredAirlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name());
                trialExpiredAirlyticsPurchaseEvent.setEventTime(trialExpiredReceiptInfo.getExpiresDateMs());
                return trialExpiredAirlyticsPurchaseEvent;
            }
        }
        return null;
    }

    private double getPeriods(LatestReceiptInfo latestReceiptInfo, PurchaseConsumer.PurchaseMetaData purchaseMetaData) {

        long subscriptionDurationInMilliSeconds = Duration.durationToMilliSeconds(
                inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getSubscriptionPeriod());

        Long cancellationDateMs = latestReceiptInfo.getCancellationDateMs() == null ? purchaseMetaData.getCancellationDateMs() :
                latestReceiptInfo.getCancellationDateMs();
        // non renewal was cancelled the payment refunded
        if (nonRenewalProducts.containsKey(latestReceiptInfo.getProductId()) &&
                (cancellationDateMs != null || latestReceiptInfo.isUpgraded())) {
            return 0;
        }

        // the period maybe be partial if a purchase was upgraded.
        if (cancellationDateMs != null) {
            if (latestReceiptInfo.isUpgraded()) {
                // covers negative periods
                if (cancellationDateMs < latestReceiptInfo.getPurchaseDateMs()) {
                    return 0;
                }
                return MathUtils.roundOff((((double) (cancellationDateMs - latestReceiptInfo.getPurchaseDateMs())) /
                        (double) subscriptionDurationInMilliSeconds), 3);
            } else { // not upgraded meaning canceled
                return 0;
            }
        } else {
            return (int) java.lang.Math.round(((double) latestReceiptInfo.getExpiresDateMs()
                    - (double) latestReceiptInfo.getPurchaseDateMs()) / (double) subscriptionDurationInMilliSeconds);
        }
    }

}
