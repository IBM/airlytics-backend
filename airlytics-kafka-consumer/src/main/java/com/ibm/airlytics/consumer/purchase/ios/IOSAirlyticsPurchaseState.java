package com.ibm.airlytics.consumer.purchase.ios;

import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.utilities.Duration;
import com.ibm.airlytics.utilities.MathUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;


public class IOSAirlyticsPurchaseState {

    protected final PurchaseConsumer.PurchaseMetaData purchaseMetaData;
    protected List<LatestReceiptInfo> latestReceiptInfos;
    protected final Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts;
    protected final Map<String, AirlyticsInAppProduct> nonRenewalProducts;
    protected List<AirlyticsPurchaseEvent> interactiveRenewalEvents;

    @Nullable
    protected List<PendingRenewalInfo> pendingRenewalInfos;


    public IOSAirlyticsPurchaseState(IOSubscriptionPurchase ioSubscriptionPurchase,
                                     PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                     Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts,
                                     List<AirlyticsPurchaseEvent> interactiveRenewalEvents
    ) {
        this(purchaseMetaData, inAppAirlyticsProducts);
        this.interactiveRenewalEvents = interactiveRenewalEvents;
        this.latestReceiptInfos = ioSubscriptionPurchase.getLatestReceiptInfo();
        this.pendingRenewalInfos = ioSubscriptionPurchase.getPendingRenewalInfo();
        this.setNonRenewalTransactionsPeriod();
    }

    public IOSAirlyticsPurchaseState(IOSubscriptionPurchase ioSubscriptionPurchase,
                                     PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                     Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts) {
        this(purchaseMetaData, inAppAirlyticsProducts);
        this.latestReceiptInfos = ioSubscriptionPurchase.getLatestReceiptInfo();
        this.pendingRenewalInfos = ioSubscriptionPurchase.getPendingRenewalInfo();
        this.setNonRenewalTransactionsPeriod();
    }

    private IOSAirlyticsPurchaseState(PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                      Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts) {
        this.purchaseMetaData = purchaseMetaData;
        this.inAppAirlyticsProducts = inAppAirlyticsProducts;
        this.nonRenewalProducts = new Hashtable<>();
        this.findNonRenewalProduct();

    }

    // returns transaction id for non-renewal we use transaction_id for renewal web_order_line_item_id
    private String getTransactionId(LatestReceiptInfo latestReceiptInfo) {
        return latestReceiptInfo.getWebOrderLineItemId() == null ?
                latestReceiptInfo.getOriginalTransactionId() : latestReceiptInfo.getWebOrderLineItemId();
    }


    protected List<RenewalsHistoricalSequence> groupSubscriptionByRenewals() {
        // use the apple store order just traverse from the oldest to the recent
        Collections.reverse(latestReceiptInfos);


        List<RenewalsHistoricalSequence> renewalHistoricalSequences = new ArrayList<>();
        RenewalsHistoricalSequence renewalHistoricalSequence = null;
        LatestReceiptInfo previousReceiptInfo = null;
        RenewalsHistoricalSequence previousRenewalsHistoricalSequence = null;
        LatestReceiptInfo theMostRecentTransaction = latestReceiptInfos.size() > 0 ?
                latestReceiptInfos.get(latestReceiptInfos.size() - 1) : null;

        for (LatestReceiptInfo latestReceiptInfo : latestReceiptInfos) {

            Long purchaseStartDate = latestReceiptInfo.getPurchaseDateMs();
            String productId = latestReceiptInfo.getProductId();

            // create new sequence if
            //1. The text transaction has different product
            //2. The product is non renewal.
            //3. The last transaction is upgraded or cancelled
            //4. The current transaction is trial
            //5. The previous trial transaction has been expired
            //6. If the current notification is INTERACTIVE_RENEWAL - the repurchase the same sub after its expiration
            if (renewalHistoricalSequence == null || !renewalHistoricalSequence.getProductId().equals(productId) ||
                    !inAppAirlyticsProducts.get(productId).isAutoRenewing()
                    || previousReceiptInfo.getCancellationDateMs() != null || previousReceiptInfo.isUpgraded()
                    || !previousReceiptInfo.getOriginalPurchaseDateMs().equals(latestReceiptInfo.getOriginalPurchaseDateMs())
                    || (latestReceiptInfo.getIsTrialPeriod() != null && latestReceiptInfo.getIsTrialPeriod().equals(true)
                    || isPreviousTransactionExpiredTrial(previousReceiptInfo, latestReceiptInfo)
                    || (isTransactionIsInteractiveRenewalEvent(latestReceiptInfo))
                    || (isCurrentUpdateIsINTERACTIVE_RENEWAL_NOTIFICATION() &&
                    theMostRecentTransaction.getWebOrderLineItemId().equals(latestReceiptInfo.getWebOrderLineItemId()))
                    || isPotentialInteractiveRenewal(latestReceiptInfo, previousReceiptInfo))
            ) {


                renewalHistoricalSequence = new RenewalsHistoricalSequence(purchaseStartDate, getTransactionId(latestReceiptInfo), productId);

                if (previousRenewalsHistoricalSequence != null) {
                    previousRenewalsHistoricalSequence.setNextPurchaseId(getTransactionId(latestReceiptInfo));
                    renewalHistoricalSequence.setPreviousPurchaseId(previousRenewalsHistoricalSequence.purchaseTransactionId);
                }

                // if the last transaction is INTERACTIVE_RENEWAL_NOTIFICATION
                // update last payment action date
                if ((isCurrentUpdateIsINTERACTIVE_RENEWAL_NOTIFICATION() &&
                        theMostRecentTransaction.getWebOrderLineItemId().equals(latestReceiptInfo.getWebOrderLineItemId())) ||
                        isTransactionIsInteractiveRenewalEvent(latestReceiptInfo)) {
                    renewalHistoricalSequence.setLastPaymentActionDate(latestReceiptInfo.getPurchaseDateMs());
                }

                renewalHistoricalSequences.add(renewalHistoricalSequence);
            } else if (previousReceiptInfo != null && previousReceiptInfo.getIsTrialPeriod()) {
                // if the previous transaction is trial set the period start date to be the expiration of of the trial transaction.
                // the previousReceiptInfo points to trial purchase
                latestReceiptInfo.setStartPeriodDateAfterTrial(previousReceiptInfo.getExpiresDateMs());
            }

            if (latestReceiptInfo.getIsTrialPeriod()) {
                renewalHistoricalSequence.setTrialStartDate(latestReceiptInfo.getPurchaseDateMs());
                renewalHistoricalSequence.setTrialEndDate(latestReceiptInfo.getExpiresDateMs());
            }

            previousReceiptInfo = latestReceiptInfo;
            previousRenewalsHistoricalSequence = renewalHistoricalSequence;
            latestReceiptInfo.setTransactionId(renewalHistoricalSequence.purchaseTransactionId);
            renewalHistoricalSequence.add(latestReceiptInfo);
        }


        // mark the last RenewalsHistoricalSequence to be the  most recent renewalsHistoricalSequence
        if (!renewalHistoricalSequences.isEmpty()) {
            renewalHistoricalSequences.get(renewalHistoricalSequences.size() - 1).
                    setMostRecentRenewalsHistoricalSequence(true);
        }


        // walk through the updated purchases and look for the not cancelled upgraded transaction.
        // if there such transaction set its actual and cancellation to the to purchase date of the following transaction
        for (int i = 0; i < renewalHistoricalSequences.size(); i++) {
            RenewalsHistoricalSequence airlyticsPurchaseHistory = renewalHistoricalSequences.get(i);

            if (airlyticsPurchaseHistory.getLatestReceiptInfos().last().isUpgraded() && (i + 1) < renewalHistoricalSequences.size()) {
                RenewalsHistoricalSequence nextHistoricalSequences = renewalHistoricalSequences.get(i + 1);
                if (airlyticsPurchaseHistory.getLatestReceiptInfos().last().getCancellationDateMs() == null) {
                    airlyticsPurchaseHistory.getLatestReceiptInfos().last().setCancellationDateMs(nextHistoricalSequences.getLatestReceiptInfos().first().getPurchaseDateMs());
                }
                airlyticsPurchaseHistory.setUpgradedToProductReceipt(nextHistoricalSequences.getLatestReceiptInfos().first());
                nextHistoricalSequences.setUpgradedFromProductReceipt(airlyticsPurchaseHistory.getLatestReceiptInfos().last());
            }
        }

        return renewalHistoricalSequences;
    }


    // if the next period start is older than the previous end date more then by one week, is treated as a new purchase
    private boolean isPotentialInteractiveRenewal(LatestReceiptInfo latestReceiptInfo, LatestReceiptInfo previousReceiptInfo) {
        if (latestReceiptInfo.getPurchaseDateMs() - previousReceiptInfo.getExpiresDateMs() >
                Duration.ONE_WEEKS_IN_MILLISECOND) {

            return true;
        }
        return false;
    }

    private boolean isTransactionIsInteractiveRenewalEvent(LatestReceiptInfo latestReceiptInfo) {
        return this.interactiveRenewalEvents != null && this.interactiveRenewalEvents.stream().
                anyMatch(t -> t.getEventTime().equals(latestReceiptInfo.getPurchaseDateMs()));
    }


    private boolean isCurrentUpdateIsINTERACTIVE_RENEWAL_NOTIFICATION() {
        return purchaseMetaData.getType() == PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION
                && purchaseMetaData.getNotificationType() != null
                && purchaseMetaData.getNotificationType().equals(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());
    }

    private boolean isPreviousTransactionExpiredTrial(LatestReceiptInfo previous, LatestReceiptInfo current) {
        if (previous.getIsTrialPeriod() != null && previous.getIsTrialPeriod().equals(true) &&
                previous.getExpiresDateMs() + Duration.FOUR_DAYS_IN_MILLISECOND < current.getPurchaseDateMs()) {
            return true;

        }
        return false;
    }

    public List<AirlyticsPurchase> getUpdatedAirlyticsPurchase() {

        List<AirlyticsPurchase> updatedAirlyticsPurchases = new ArrayList<>();
        List<RenewalsHistoricalSequence> groupSubscriptionByRenewals = groupSubscriptionByRenewals();
        for (RenewalsHistoricalSequence currentRenewalHistoricalSequence : groupSubscriptionByRenewals) {
            updatedAirlyticsPurchases.add(currentRenewalHistoricalSequence.getAirlyticsPurchase());
        }

        return updatedAirlyticsPurchases;
    }


    private void findNonRenewalProduct() {
        for (String airlyticsInAppProductName : this.inAppAirlyticsProducts.keySet()) {
            if (!this.inAppAirlyticsProducts.get(airlyticsInAppProductName).isAutoRenewing()) {
                nonRenewalProducts.put(airlyticsInAppProductName, this.inAppAirlyticsProducts.get(airlyticsInAppProductName));
            }
        }
    }

    protected void setNonRenewalTransactionsPeriod() {
        // walk through all non-renewal product and lookup for its transaction.
        for (AirlyticsInAppProduct airlyticsInAppProduct : this.nonRenewalProducts.values()) {

            // walk through all transaction and put them into separated list
            List<LatestReceiptInfo> nonRenewalTransactions = new ArrayList<>();
            for (LatestReceiptInfo latestReceiptInfo : this.latestReceiptInfos) {
                if (latestReceiptInfo.getProductId().equals(airlyticsInAppProduct.getProductId())) {
                    nonRenewalTransactions.add(latestReceiptInfo);
                }
            }

            //sort the non renewal transaction by start date
            List<LatestReceiptInfo> sortedNonRenewalTransaction = nonRenewalTransactions.stream().
                    sorted(Comparator.comparingLong(LatestReceiptInfo::getPurchaseDateMs)).collect(Collectors.toList());

            // walk through sorted transaction and set start and expiration date in milliseconds
            long periodInMilliseconds = Duration.durationToMilliSeconds(airlyticsInAppProduct.getSubscriptionPeriod());
            LatestReceiptInfo previousLatestReceiptInfo = null;
            for (LatestReceiptInfo latestReceiptInfo : sortedNonRenewalTransaction) {
                if (previousLatestReceiptInfo == null) {
                    previousLatestReceiptInfo = latestReceiptInfo;
                    latestReceiptInfo.setExpiresDateMs(latestReceiptInfo.getPurchaseDateMs() + periodInMilliseconds);
                } else {
                    if (previousLatestReceiptInfo.getExpiresDateMs() > latestReceiptInfo.getPurchaseDateMs()) {
                        latestReceiptInfo.setPurchaseDateMs(previousLatestReceiptInfo.getExpiresDateMs());
                        latestReceiptInfo.setExpiresDateMs(previousLatestReceiptInfo.getExpiresDateMs() + periodInMilliseconds);
                    } else {
                        latestReceiptInfo.setExpiresDateMs(latestReceiptInfo.getPurchaseDateMs() + periodInMilliseconds);
                    }
                    previousLatestReceiptInfo = latestReceiptInfo;
                }
            }
        }
    }

    @SuppressWarnings({"unused", "unchecked"})
    protected class RenewalsHistoricalSequence {
        private Long purchaseStartDate;
        private Long trialStartDate;
        private Long trialEndDate;
        private boolean isMostRecentRenewalsHistoricalSequence;
        private final String purchaseTransactionId;
        private String productId;
        private final SortedSet<LatestReceiptInfo> latestReceiptInfos;
        private Long lastPaymentActionDate;
        private final AirlyticsPurchase updatedPurchase;
        private LatestReceiptInfo upgradedToProductReceipt;
        private LatestReceiptInfo upgradedFromProductReceipt;

        // these two field are used to hold a reference to initial
        // subscription transaction id that we use for purchase id
        private String nextPurchaseId;
        private String previousPurchaseId;


        RenewalsHistoricalSequence(Long purchaseStartDate, String purchaseTransactionId, String productId) {
            this.purchaseStartDate = purchaseStartDate;
            this.purchaseTransactionId = purchaseTransactionId;
            this.productId = productId;
            this.latestReceiptInfos = new TreeSet((Comparator<LatestReceiptInfo>)
                    (o1, o2) -> (int) (o1.getPurchaseDateMs().compareTo(o2.getPurchaseDateMs())));
            this.updatedPurchase = new AirlyticsPurchase();
        }

        public Long getLastPaymentActionDate() {
            return lastPaymentActionDate;
        }

        public void setLastPaymentActionDate(Long lastPaymentActionDate) {
            this.lastPaymentActionDate = lastPaymentActionDate;
        }

        public String getNextPurchaseId() {
            return nextPurchaseId;
        }

        public void setNextPurchaseId(String nextPurchaseId) {
            this.nextPurchaseId = nextPurchaseId;
        }

        public String getPreviousPurchaseId() {
            return previousPurchaseId;
        }

        public void setPreviousPurchaseId(String previousPurchaseId) {
            this.previousPurchaseId = previousPurchaseId;
        }

        public boolean getMostRecentRenewalsHistoricalSequence() {
            return isMostRecentRenewalsHistoricalSequence;
        }

        public void setMostRecentRenewalsHistoricalSequence(boolean mostRecentRenewalsHistoricalSequence) {
            isMostRecentRenewalsHistoricalSequence = mostRecentRenewalsHistoricalSequence;
        }

        public boolean isRenewableProduct() {
            return !nonRenewalProducts.containsKey(productId);
        }

        public String getPurchaseTransactionId() {
            return purchaseTransactionId;
        }

        public AirlyticsPurchase getUpdatedPurchase() {
            return updatedPurchase;
        }

        public LatestReceiptInfo getUpgradedToProductReceipt() {
            return upgradedToProductReceipt;
        }

        public LatestReceiptInfo getUpgradedFromProductReceipt() {
            return upgradedFromProductReceipt;
        }

        public void setUpgradedFromProductReceipt(LatestReceiptInfo upgradedFromProductReceipt) {
            this.upgradedFromProductReceipt = upgradedFromProductReceipt;
        }

        public void setUpgradedToProductReceipt(LatestReceiptInfo upgradedToProductReceipt) {
            this.upgradedToProductReceipt = upgradedToProductReceipt;
        }

        public SortedSet<LatestReceiptInfo> getLatestReceiptInfos() {
            return latestReceiptInfos;
        }

        public void setPurchaseStartDate(Long purchaseStartDate) {
            this.purchaseStartDate = purchaseStartDate;
        }

        public Long getTrialStartDate() {
            return trialStartDate;
        }

        public void setTrialStartDate(Long trialStartDate) {
            this.trialStartDate = trialStartDate;
        }

        void add(LatestReceiptInfo latestReceiptInfo) {
            latestReceiptInfos.add(latestReceiptInfo);
        }

        public Long getTrialEndDate() {
            return trialEndDate;
        }

        public void setTrialEndDate(Long trialEndDate) {
            this.trialEndDate = trialEndDate;
        }

        String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public Long getPurchaseStartDate() {
            return purchaseStartDate;
        }

        public AirlyticsPurchase getAirlyticsPurchase() {

            updateWithBasicAttributes();
            // put all renewal history to separate  and to replicate subscription-renewed events sequence
            for (LatestReceiptInfo latestReceiptInfo : this.latestReceiptInfos) {
                updateWithCalculatedAttribute(latestReceiptInfo, purchaseStartDate);
                if (!nonRenewalProducts.containsKey(productId)
                        && (updatedPurchase.getPeriods() > 1 || updatedPurchase.getPeriods() == 1 && getTrialStartDate() != null)) {

                    AirlyticsPurchase renewal = updatedPurchase.clone();
                    renewal.setStartDate(latestReceiptInfo.getPurchaseDateMs());
                    renewal.setExpirationDate(latestReceiptInfo.getExpiresDateMs());
                    updatedPurchase.addRenewalToHistory(renewal);
                }
            }

            // set expiration reason
            if (pendingRenewalInfos != null) {
                for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
                    if (pendingRenewalInfo.getProductId().equals(productId) && pendingRenewalInfo.getExpirationIntent() != null) {
                        updatedPurchase.setExpirationReason(IOSPurchaseConsumer.ExpirationReason.getNameByType(pendingRenewalInfo.getExpirationIntent()));
                    }
                }
            }

            return this.updatedPurchase;
        }

        private void updateWithBasicAttributes() {
            updatedPurchase.setId(purchaseTransactionId);
            updatedPurchase.setPlatform(purchaseMetaData.getPlatform());
            updatedPurchase.setProduct(productId);
            updatedPurchase.setReceipt(purchaseMetaData.getReceipt());
            updatedPurchase.setStartDate(purchaseStartDate);
            updatedPurchase.setLastModifiedDate(System.currentTimeMillis());
            updatedPurchase.setDuration(Duration.Periodicity.getNameByType(inAppAirlyticsProducts.get(productId).getSubscriptionPeriod()));
            updatedPurchase.setUpgradedFromProduct(upgradedFromProductReceipt);
            updatedPurchase.setUpgradedToProductActive(upgradedToProductReceipt == null ? false :
                    isLatestReceiptInfoActive(upgradedToProductReceipt));
            updatedPurchase.setPurchaseIdUpgradedFrom(previousPurchaseId);
        }

        private void updateWithCalculatedAttribute(LatestReceiptInfo latestReceiptInfo, Long purchaseStartDate) {

            updatedPurchase.setExpirationDate(latestReceiptInfo.getExpiresDateMs());
            updatedPurchase.setIntroPricing(latestReceiptInfo.getIsInIntroOfferPeriod() == null ? false : latestReceiptInfo.getIsInIntroOfferPeriod());
            updatedPurchase.setOriginalTransactionId(latestReceiptInfo.getOriginalTransactionId());
            updatedPurchase.setLastPaymentActionDate(this.lastPaymentActionDate == null ?
                    latestReceiptInfo.getOriginalPurchaseDateMs() : this.lastPaymentActionDate);
            updatedPurchase.setTrialStartDate(trialStartDate);
            updatedPurchase.setTrialEndDate(trialEndDate);

            updateAutoRenewStatus(updatedPurchase, latestReceiptInfo);
            updateIsActive(updatedPurchase, latestReceiptInfo);
            updatePeriodStartDate(updatedPurchase, latestReceiptInfo);
            updateActualEndDate(updatedPurchase, latestReceiptInfo);
            updateIsTrial(updatedPurchase, latestReceiptInfo);
            updatePeriods(updatedPurchase, latestReceiptInfo);
            updateIsGrace(updatedPurchase, latestReceiptInfo);
            updateIsUpgraded(updatedPurchase, latestReceiptInfo);
            updatedModifierSource(updatedPurchase, latestReceiptInfo);
            updateRevenueUsd(updatedPurchase, latestReceiptInfo);
            updateCancellationReason(updatedPurchase, latestReceiptInfo);
            setAutoRenewStatusChangeDate(updatedPurchase, latestReceiptInfo);


        }

        // set the period start date be equal to the purchase date unless previously it was trial
        // all next renewal will update the period start date.
        private void updatePeriodStartDate(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setPeriodStartDate(latestReceiptInfo.getPurchaseDateMs());
        }

        private void updatedModifierSource(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            if (purchaseMetaData.getType() == PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION) {
                if (pendingRenewalInfos != null) {
                    for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
                        if (pendingRenewalInfo.getProductId().equals(productId) &&
                                pendingRenewalInfo.getOriginalTransactionId().equals(latestReceiptInfo.getOriginalTransactionId())) {
                            updatedPurchase.setModifierSource(purchaseMetaData.getNotificationType());
                        }
                    }
                }
            } else if (purchaseMetaData.getType() != PurchaseConsumer.PurchaseMetaData.Type.RENEWAL_CHECK) {
                updatedPurchase.setModifierSource(purchaseMetaData.getType().name());
            }
        }

        private void updateCancellationReason(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            if (latestReceiptInfo.getCancellationReason() != null) {
                updatedPurchase.setCancellationReason(IOSPurchaseConsumer.CancellationReason.getNameByType(latestReceiptInfo.getCancellationReason(),
                        latestReceiptInfo.isUpgraded()));
            } else {
                updatedPurchase.setCancellationReason(null);
            }
            updatedPurchase.setCancellationDate(latestReceiptInfo.getCancellationDateMs());

            if (latestReceiptInfo.getCancellationDateMs() != null && !latestReceiptInfo.isUpgraded()) {
                updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.CANCELED.getStatus());
            }
        }

        private void updateIsUpgraded(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setUpgraded(latestReceiptInfo.isUpgraded());
            updatedPurchase.setUpgradedToProduct(this.getUpgradedToProductReceipt());
            if (latestReceiptInfo.isUpgraded()) {
                updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
            }
        }

        private long getActualEndDate(LatestReceiptInfo latestReceiptInfo) {
            if (latestReceiptInfo.getCancellationDateMs() != null) {
                return latestReceiptInfo.getCancellationDateMs();
            } else {
                return latestReceiptInfo.getExpiresDateMs();
            }
        }

        private void updateActualEndDate(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setActualEndDate(getActualEndDate(latestReceiptInfo));
        }

        private double getPeriods(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {

            if (latestReceiptInfo.getIsTrialPeriod()) {
                return 0;
            }

            long subscriptionDurationInMilliSeconds = Duration.durationToMilliSeconds(
                    inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getSubscriptionPeriod());

            // non renewal was cancelled the payment refunded
            if (nonRenewalProducts.containsKey(updatedPurchase.getProduct()) &&
                    (latestReceiptInfo.getCancellationDateMs() != null || latestReceiptInfo.isUpgraded())) {
                return 0;
            }

            // the period maybe be partial if a purchase was upgraded.
            if (latestReceiptInfo.getCancellationDateMs() != null) {
                if (latestReceiptInfo.isUpgraded()) {
                    // covers negative periods
                    if (latestReceiptInfo.getCancellationDateMs() < latestReceiptInfo.getPurchaseDateMs()) {
                        return 0;
                    }
                    return MathUtils.roundOff((((double) (latestReceiptInfo.getCancellationDateMs() - latestReceiptInfo.getPurchaseDateMs())) /
                            (double) subscriptionDurationInMilliSeconds), 3);
                } else { // not upgraded meaning canceled
                    return 0;
                }
            } else {
                return (int) java.lang.Math.round(((double) latestReceiptInfo.getExpiresDateMs()
                        - (double) latestReceiptInfo.getPurchaseDateMs()) / (double) subscriptionDurationInMilliSeconds);
            }
        }

        protected void updatePeriods(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setPeriods(updatedPurchase.getPeriods() + getPeriods(updatedPurchase, latestReceiptInfo));
        }


        protected boolean getAutoRenewStatus(LatestReceiptInfo latestReceiptInfo) {
            if (!nonRenewalProducts.containsKey(latestReceiptInfo.getProductId()) && isMostRecentRenewalsHistoricalSequence) {
                if (pendingRenewalInfos != null) {
                    // for sub with multiple purchase transactions set AutoRenewStatus only for the last
                    for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
                        if (pendingRenewalInfo.getProductId().equals(productId) &&
                                pendingRenewalInfo.getOriginalTransactionId().equals(latestReceiptInfo.getOriginalTransactionId())) {
                            return pendingRenewalInfo.isAutoRenewStatus();
                        }
                    }
                }
            }
            return false;
        }

        protected void updateAutoRenewStatus(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setAutoRenewStatus(getAutoRenewStatus(latestReceiptInfo));
        }


        private boolean isInGracePeriod(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            if (pendingRenewalInfos != null && isMostRecentRenewalsHistoricalSequence) {
                for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
                    if (pendingRenewalInfo.getProductId().equals(productId) &&
                            pendingRenewalInfo.getOriginalTransactionId().equals(latestReceiptInfo.getOriginalTransactionId())) {

                        // should check the expiration intent to determine if a purchase in grace period
                        if (pendingRenewalInfo.getExpirationIntent() != null &&
                                pendingRenewalInfo.getExpirationIntent() == IOSPurchaseConsumer.ExpirationReason.BILLING_ISSUE.getType()
                                && pendingRenewalInfo.getGracePeriodExpiresDateMs() != null) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        protected void updateIsGrace(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setGrace(isInGracePeriod(updatedPurchase, latestReceiptInfo));
        }

        private boolean isLatestReceiptInfoActive(LatestReceiptInfo latestReceiptInfo) {
            boolean isActive = getActualEndDate(latestReceiptInfo) > System.currentTimeMillis() &&
                    purchaseStartDate < System.currentTimeMillis();

            if (nonRenewalProducts.containsKey(latestReceiptInfo.getProductId())) {
                // if there pendingRenewalInfos section, meaning the non renewable purchase should be not active.
                if (pendingRenewalInfos != null) {
                    return false;
                } else {
                    return isActive;
                }
                // if transaction is upgraded it should be not active
            } else if (isActive) {
                return !latestReceiptInfo.isUpgraded();
            }
            return isActive;
        }


        protected boolean getIsActive(LatestReceiptInfo latestReceiptInfo) {
            boolean isActive = getActualEndDate(latestReceiptInfo) > System.currentTimeMillis() &&
                    purchaseStartDate < System.currentTimeMillis();

            if (nonRenewalProducts.containsKey(latestReceiptInfo.getProductId())) {
                // if there pendingRenewalInfos section, meaning the non renewable purchase should be not active.
                if (pendingRenewalInfos != null) {
                    return false;
                } else {
                    return isActive;
                }
            } else {
                return isActive ? !latestReceiptInfo.isUpgraded() : false;
            }
        }

        /**
         * The active column calculation is true if  is start-date < current-data < expiration date otherwise false
         */
        void updateIsActive(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {

            boolean isActive = getActualEndDate(latestReceiptInfo) > System.currentTimeMillis() &&
                    purchaseStartDate < System.currentTimeMillis();

            if (nonRenewalProducts.containsKey(latestReceiptInfo.getProductId())) {
                // if there pendingRenewalInfos section, meaning the non renewable purchase should be not active.
                if (pendingRenewalInfos != null) {
                    updatedPurchase.setActive(false);
                    updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
                    return;
                } else {
                    updatedPurchase.setActive(isActive);
                    updatedPurchase.setSubscriptionStatus(isActive ? PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus() :
                            PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
                    return;
                }
            } else {
                updatedPurchase.setActive(isActive);
            }

            // if transaction is upgraded it should be not active
            if (isActive) {
                updatedPurchase.setActive(!latestReceiptInfo.isUpgraded());
            }

            // set subscription state based on the active value
            if (updatedPurchase.getActive()) {
                updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus());
            } else {
                updatedPurchase.setSubscriptionStatus(PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            }
        }

        void updateIsTrial(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            updatedPurchase.setTrial(latestReceiptInfo.getIsTrialPeriod());
        }

        void updateRevenueUsd(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {

            if (latestReceiptInfo.getIsInIntroOfferPeriod() != null && latestReceiptInfo.getIsInIntroOfferPeriod() &&
                    inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getIntroPriceUSDMicros() != null) {
                updatedPurchase.setRevenueUsd(updatedPurchase.getRevenueUsd() +
                        inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getIntroPriceUSDMicros());
            } else {
                updatedPurchase.setRevenueUsd((long) (updatedPurchase.getRevenueUsd() +
                        (inAppAirlyticsProducts.get(latestReceiptInfo.getProductId()).getPriceUSDMicros()
                                * getPeriods(updatedPurchase, latestReceiptInfo))));
            }
        }

        // autoRenewStatusChange Date is taken only from the notification, it's not a part of LatestReceiptInfo
        void setAutoRenewStatusChangeDate(AirlyticsPurchase updatedPurchase, LatestReceiptInfo latestReceiptInfo) {
            if (purchaseMetaData.getType() == PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION) {
                if (pendingRenewalInfos != null) {
                    for (PendingRenewalInfo pendingRenewalInfo : pendingRenewalInfos) {
                        if (pendingRenewalInfo.getProductId().equals(productId) &&
                                pendingRenewalInfo.getOriginalTransactionId().equals(latestReceiptInfo.getOriginalTransactionId())
                                && purchaseMetaData.getAutoRenewStatusChangeDateMs() != null) {
                            updatedPurchase.setAutoRenewStatusChangeDate(purchaseMetaData.getAutoRenewStatusChangeDateMs());
                        }
                    }
                }
            }
        }
    }
}
