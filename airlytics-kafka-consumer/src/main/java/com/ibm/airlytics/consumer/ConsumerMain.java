package com.ibm.airlytics.consumer;

import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeForwardingConsumer;
import com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeForwardingConsumerConfig;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeTransformationConsumer;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeTransformationConsumerConfig;
import com.ibm.airlytics.consumer.braze.currents.BrazeCurrentsConsumer;
import com.ibm.airlytics.consumer.braze.currents.BrazeCurrentsConsumerConfig;
import com.ibm.airlytics.consumer.braze.forwarding.BrazeForwardingConsumer;
import com.ibm.airlytics.consumer.braze.forwarding.BrazeForwardingConsumerConfig;
import com.ibm.airlytics.consumer.braze.transformation.BrazeTransformationConsumer;
import com.ibm.airlytics.consumer.braze.transformation.BrazeTransformationConsumerConfig;
import com.ibm.airlytics.consumer.cloning.CloningConsumer;
import com.ibm.airlytics.consumer.cloning.config.CloningConsumerConfig;
import com.ibm.airlytics.consumer.cohorts.amplitude.AmplitudeCohortsConsumer;
import com.ibm.airlytics.consumer.cohorts.amplitude.AmplitudeCohortsConsumerConfig;
import com.ibm.airlytics.consumer.cohorts.braze.BrazeCohortsConsumer;
import com.ibm.airlytics.consumer.cohorts.braze.BrazeCohortsConsumerConfig;
import com.ibm.airlytics.consumer.cohorts.localytics.LocalyticsCohortsConsumer;
import com.ibm.airlytics.consumer.cohorts.localytics.LocalyticsCohortsConsumerConfig;
import com.ibm.airlytics.consumer.cohorts.mparticle.MparticleCohortsConsumer;
import com.ibm.airlytics.consumer.cohorts.mparticle.MparticleCohortsConsumerConfig;
import com.ibm.airlytics.consumer.compaction.CompactionConsumer;
import com.ibm.airlytics.consumer.compaction.CompactionConsumerConfig;
import com.ibm.airlytics.consumer.dsr.DSRConsumer;
import com.ibm.airlytics.consumer.dsr.DSRConsumerConfig;
import com.ibm.airlytics.consumer.kafkacloning.KafkaCloningConsumer;
import com.ibm.airlytics.consumer.kafkacloning.KafkaCloningConsumerConfig;
import com.ibm.airlytics.consumer.ltvProcess.LTVProcessConsumer;
import com.ibm.airlytics.consumer.ltvProcess.LTVProcessConsumerConfig;
import com.ibm.airlytics.consumer.mparticle.forwarding.MparticleForwardingConsumer;
import com.ibm.airlytics.consumer.mparticle.forwarding.MparticleForwardingConsumerConfig;
import com.ibm.airlytics.consumer.mparticle.transformation.MparticleTransformationConsumer;
import com.ibm.airlytics.consumer.mparticle.transformation.MparticleTransformationConsumerConfig;
import com.ibm.airlytics.consumer.persistence.PersistenceConsumer;
import com.ibm.airlytics.consumer.persistence.PersistenceConsumerConfig;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumerConfig;
import com.ibm.airlytics.consumer.purchase.android.AndroidPurchaseConsumer;
import com.ibm.airlytics.consumer.purchase.data.AndroidDataRepairer;
import com.ibm.airlytics.consumer.purchase.data.IOSDataRepairer;
import com.ibm.airlytics.consumer.purchase.ios.IOSPurchaseConsumer;
import com.ibm.airlytics.consumer.rawData.RawDataConsumer;
import com.ibm.airlytics.consumer.rawData.RawDataConsumerConfig;
import com.ibm.airlytics.consumer.realTimeData.RealTimeDataConsumer;
import com.ibm.airlytics.consumer.realTimeData.RealTimeDataConsumerConfig;
import com.ibm.airlytics.consumer.segment.SegmentConsumer;
import com.ibm.airlytics.consumer.segment.SegmentConsumerConfig;
import com.ibm.airlytics.consumer.transformation.TransformationConsumer;
import com.ibm.airlytics.consumer.transformation.TransformationConsumerConfig;
import com.ibm.airlytics.consumer.userdb.UserDBConsumer;
import com.ibm.airlytics.consumer.userdb.UserDBConsumerConfig;
import com.ibm.airlytics.monitoring.MonitoringServer;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.Products;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.log4j.*;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerMain {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(ConsumerMain.class.getName());

    private static final String METRICS_PORT_ENV_VARIABLE = "METRICS_PORT";
    private static final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) {

        MonitoringServer monitoringServer = null;

        try {
            Appender appender = new ConsoleAppender(new PatternLayout("%d [%t] %p %c %x - %m%n"));// default: "%r [%t] %p %c %x - %m%n"
            org.apache.log4j.BasicConfigurator.configure(appender);
            Logger.getRootLogger().setLevel(Level.INFO);
            boolean invalidInput = args.length < 1;

            if (!invalidInput) {

                switch (args[0]) {
                    case "userdb":
                    case "persistence":
                    case "compaction":
                    case "dsr":
                    case "rawdata":
                    case "realtime":
                    case "cloning":
                    case "kafka_cloning":
                    case "amplitude_transform":
                    case "amplitude_forward":
                    case "braze_transform":
                    case "braze_forward":
                    case "braze_currents":
                    case "transformation":
                    case "cohorts_localytics":
                    case "cohorts_ups":
                    case "cohorts_braze":
                    case "cohorts_amplitude":
                    case "cohorts_mparticle":
                    case "android_purchase":
                    case "ios_purchase":
                    case "ltv_process":
                    case "mparticle_transform":
                    case "mparticle_forward":
                    case "data_repairer":
                        break;
                    default:
                        invalidInput = true;
                }
            }

            if (invalidInput) {
                System.out.println(
                        "Argument missing or incorrect: consumer type (currently supported: " +
                                "persistence, userdb, dsr, compaction, cloning, kafka_cloning, amplitude_transform, amplitude_forward, " +
                                "mparticle_transform, mparticle_forward, " +
                                "braze_transform, braze_forward, braze_currents, cohorts_localytics, cohorts_ups, cohorts_braze, cohorts_amplitude, cohorts_mpartcile, " +
                                "realtime, ios_purchase, android_purchase, ltv_process or rawdata)");
                System.exit(1);
            }

            int monitoringPort = 8084;
            //String monitoringPortEnv = System.getenv(METRICS_PORT_ENV_VARIABLE);
            String monitoringPortEnv = Environment.getNumericEnv(METRICS_PORT_ENV_VARIABLE, true);
            if (monitoringPortEnv != null) {
                try {
                    monitoringPort = Integer.parseUnsignedInt(monitoringPortEnv);
                } catch (NumberFormatException e) {
                    System.out.println("Metrics port not a valid unsigned int. Defaulting to 8084");
                }
            }

            AirlockManager.getInstance(args[0]);
            Products.newConfigurationAvailable();
            try (final AirlyticsConsumer consumer = createConsumer(args[0])) {
                if (consumer != null) {
                    monitoringServer = new MonitoringServer(consumer);
                    monitoringServer.start(monitoringPort);

                    executor.scheduleAtFixedRate(() -> refreshAirlockConfiguration(consumer),
                            600, 600, TimeUnit.SECONDS);

                    consumer.runConsumer();
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        } finally {
            executor.shutdown();
            AirlockManager.close();
            if (monitoringServer != null)
                monitoringServer.stop();
        }
    }

    private static void refreshAirlockConfiguration(AirlyticsConsumer consumer) {
        if (AirlockManager.getInstance().refreshConfiguration()) {
            consumer.newConfigurationAvailable();
            Products.newConfigurationAvailable();
        }
    }

    private static AirlyticsConsumer createConsumer(String consumerName) {
        AirlyticsConsumer newConsumer = null;

        try {
            switch (consumerName.toLowerCase()) {
                case "persistence":
                    PersistenceConsumerConfig perConfig = new PersistenceConsumerConfig();
                    perConfig.initWithAirlock();
                    newConsumer = new PersistenceConsumer(perConfig);
                    break;
                case "userdb":
                    UserDBConsumerConfig config = new UserDBConsumerConfig();
                    config.initWithAirlock();
                    newConsumer = new UserDBConsumer(config);
                    break;
                case "compaction":
                    CompactionConsumerConfig compConfig = new CompactionConsumerConfig();
                    compConfig.initWithAirlock();
                    newConsumer = new CompactionConsumer(compConfig);
                    break;
                case "segment":
                    SegmentConsumerConfig segmentConfig = new SegmentConsumerConfig();
                    segmentConfig.initWithAirlock();
                    newConsumer = new SegmentConsumer(segmentConfig);
                    break;
                case "dsr":
                    DSRConsumerConfig dsrConfig = new DSRConsumerConfig();
                    dsrConfig.initWithAirlock();
                    newConsumer = new DSRConsumer(dsrConfig);
                    break;
                case "rawdata":
                    RawDataConsumerConfig rawDataConfig = new RawDataConsumerConfig();
                    rawDataConfig.initWithAirlock();
                    newConsumer = new RawDataConsumer(rawDataConfig);
                    break;
                case "realtime":
                    RealTimeDataConsumerConfig realtimeConfig = new RealTimeDataConsumerConfig();
                    realtimeConfig.initWithAirlock();
                    newConsumer = new RealTimeDataConsumer(realtimeConfig);
                    break;
                case "cloning":
                    CloningConsumerConfig cloningConfig = new CloningConsumerConfig();
                    cloningConfig.initWithAirlock();
                    newConsumer = new CloningConsumer(cloningConfig);
                    break;
                case "kafka_cloning":
                    KafkaCloningConsumerConfig kafkaCloningConfig = new KafkaCloningConsumerConfig();
                    kafkaCloningConfig.initWithAirlock();
                    newConsumer = new KafkaCloningConsumer(kafkaCloningConfig);
                    break;
                case "android_purchase":
                    PurchaseConsumerConfig purchaseAttributeConsumerConfig = new PurchaseConsumerConfig();
                    purchaseAttributeConsumerConfig.initWithAirlock();
                    newConsumer = new AndroidPurchaseConsumer(purchaseAttributeConsumerConfig);
                    break;
                case "ios_purchase":
                    PurchaseConsumerConfig iOSPurchaseAttributeConsumerConfig = new PurchaseConsumerConfig();
                    iOSPurchaseAttributeConsumerConfig.initWithAirlock();
                    newConsumer = new IOSPurchaseConsumer(iOSPurchaseAttributeConsumerConfig);
                    break;
                case "transformation":
                    TransformationConsumerConfig tcConfig = new TransformationConsumerConfig();
                    tcConfig.initWithAirlock();
                    newConsumer = new TransformationConsumer(tcConfig);
                    break;
                case "amplitude_transform":
                    AmplitudeTransformationConsumerConfig atcConfig = new AmplitudeTransformationConsumerConfig();
                    atcConfig.initWithAirlock();
                    newConsumer = new AmplitudeTransformationConsumer(atcConfig);
                    break;
                case "amplitude_forward":
                    AmplitudeForwardingConsumerConfig afcConfig = new AmplitudeForwardingConsumerConfig();
                    afcConfig.initWithAirlock();
                    newConsumer = new AmplitudeForwardingConsumer(afcConfig);
                    break;
                case "braze_transform":
                    BrazeTransformationConsumerConfig btcConfig = new BrazeTransformationConsumerConfig();
                    btcConfig.initWithAirlock();
                    newConsumer = new BrazeTransformationConsumer(btcConfig);
                    break;
                case "braze_forward":
                    BrazeForwardingConsumerConfig bfcConfig = new BrazeForwardingConsumerConfig();
                    bfcConfig.initWithAirlock();
                    newConsumer = new BrazeForwardingConsumer(bfcConfig);
                    break;
                case "braze_currents":
                    BrazeCurrentsConsumerConfig bccConfig = new BrazeCurrentsConsumerConfig();
                    bccConfig.initWithAirlock();
                    newConsumer = new BrazeCurrentsConsumer(bccConfig);
                    break;
                case "cohorts_localytics":
                    LocalyticsCohortsConsumerConfig lccConfig = new LocalyticsCohortsConsumerConfig();
                    lccConfig.initWithAirlock();
                    newConsumer = new LocalyticsCohortsConsumer(lccConfig);
                    break;
                case "cohorts_braze":
                    BrazeCohortsConsumerConfig brazeConfig = new BrazeCohortsConsumerConfig();
                    brazeConfig.initWithAirlock();
                    newConsumer = new BrazeCohortsConsumer(brazeConfig);
                    break;
                case "cohorts_amplitude":
                    AmplitudeCohortsConsumerConfig amplitudeConfig = new AmplitudeCohortsConsumerConfig();
                    amplitudeConfig.initWithAirlock();
                    newConsumer = new AmplitudeCohortsConsumer(amplitudeConfig);
                    break;
                case "cohorts_mparticle":
                    MparticleCohortsConsumerConfig mparticleConfig = new MparticleCohortsConsumerConfig();
                    mparticleConfig.initWithAirlock();
                    newConsumer = new MparticleCohortsConsumer(mparticleConfig);
                    break;
                case "ltv_process":
                    LTVProcessConsumerConfig ltvpConfig = new LTVProcessConsumerConfig();
                    ltvpConfig.initWithAirlock();
                    newConsumer = new LTVProcessConsumer(ltvpConfig);
                    break;
                case "mparticle_transform":
                    MparticleTransformationConsumerConfig mtcConfig = new MparticleTransformationConsumerConfig();
                    mtcConfig.initWithAirlock();
                    newConsumer = new MparticleTransformationConsumer(mtcConfig);
                    break;
                case "mparticle_forward":
                    MparticleForwardingConsumerConfig mfcConfig = new MparticleForwardingConsumerConfig();
                    mfcConfig.initWithAirlock();
                    newConsumer = new MparticleForwardingConsumer(mfcConfig);
                    break;
                case "data_repairer":
                    PurchaseConsumerConfig iOSDataRepairerConsumerConfig = new PurchaseConsumerConfig();
                    iOSDataRepairerConsumerConfig.initWithAirlock();
                    newConsumer = new AndroidDataRepairer(iOSDataRepairerConsumerConfig);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }

        if (newConsumer != null) {
            newConsumer.setConsumerType(consumerName);
        }
        return newConsumer;
    }
}
