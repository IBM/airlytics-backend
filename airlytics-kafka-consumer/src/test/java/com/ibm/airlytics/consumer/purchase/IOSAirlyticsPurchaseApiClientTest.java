package com.ibm.airlytics.consumer.purchase;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.ios.IOSPurchaseApiClient;
import com.ibm.airlytics.consumer.purchase.ios.IOSubscriptionPurchase;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;


public class IOSAirlyticsPurchaseApiClientTest {

    private static String testReceiptData = "ewoJInNpZ25hdHVyZSIgPSAiQTZQc3RMaDNPOStPMjJ5TXEwZkpwVG1MZko4SFZsb21PUitZK2c2QXNwVjFPNjNCb2RiOWxwUVJDRURqRW90V1g0N1hUWU5WclVOYTJjclZ1UjdiNlpyRDVDVnpuSnUybHlxRmxXbFRCdXZXU05FODc3WEx2alYvVFliREJTNXhpVzMxWVJicnZvcWhmTEJPSlcyd1NUODljcG5YVFQvSC9OVUZ0TzUyYVBuWTZCbkF2bVhzUmRNaVdoNWl4WFV0MWp6UU43dFNZSnpPVnhLeVpuOENTUGJUdlZ1VXJ5QVozeTNXcWg0REJDWDJmeEk5Sjc2NEY3VUNQbG1HYkZhdm1vQ2sxak1aWmdzZm9oWVdGcFZrOWZ0Y0pucGUrYXp1NlE1QnFpVW5wNUd5ci9XTjlhVHhUMVlKQXl5MGZsTTgzZHdpSUNSQjdrdzBMbWo4TTR1aENuTUFBQVdBTUlJRmZEQ0NCR1NnQXdJQkFnSUlEdXRYaCtlZUNZMHdEUVlKS29aSWh2Y05BUUVGQlFBd2daWXhDekFKQmdOVkJBWVRBbFZUTVJNd0VRWURWUVFLREFwQmNIQnNaU0JKYm1NdU1Td3dLZ1lEVlFRTERDTkJjSEJzWlNCWGIzSnNaSGRwWkdVZ1JHVjJaV3h2Y0dWeUlGSmxiR0YwYVc5dWN6RkVNRUlHQTFVRUF3dzdRWEJ3YkdVZ1YyOXliR1IzYVdSbElFUmxkbVZzYjNCbGNpQlNaV3hoZEdsdmJuTWdRMlZ5ZEdsbWFXTmhkR2x2YmlCQmRYUm9iM0pwZEhrd0hoY05NVFV4TVRFek1ESXhOVEE1V2hjTk1qTXdNakEzTWpFME9EUTNXakNCaVRFM01EVUdBMVVFQXd3dVRXRmpJRUZ3Y0NCVGRHOXlaU0JoYm1RZ2FWUjFibVZ6SUZOMGIzSmxJRkpsWTJWcGNIUWdVMmxuYm1sdVp6RXNNQ29HQTFVRUN3d2pRWEJ3YkdVZ1YyOXliR1IzYVdSbElFUmxkbVZzYjNCbGNpQlNaV3hoZEdsdmJuTXhFekFSQmdOVkJBb01Da0Z3Y0d4bElFbHVZeTR4Q3pBSkJnTlZCQVlUQWxWVE1JSUJJakFOQmdrcWhraUc5dzBCQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBcGMrQi9TV2lnVnZXaCswajJqTWNqdUlqd0tYRUpzczl4cC9zU2cxVmh2K2tBdGVYeWpsVWJYMS9zbFFZbmNRc1VuR09aSHVDem9tNlNkWUk1YlNJY2M4L1cwWXV4c1FkdUFPcFdLSUVQaUY0MWR1MzBJNFNqWU5NV3lwb041UEM4cjBleE5LaERFcFlVcXNTNCszZEg1Z1ZrRFV0d3N3U3lvMUlnZmRZZUZScjZJd3hOaDlLQmd4SFZQTTNrTGl5a29sOVg2U0ZTdUhBbk9DNnBMdUNsMlAwSzVQQi9UNXZ5c0gxUEttUFVockFKUXAyRHQ3K21mNy93bXYxVzE2c2MxRkpDRmFKekVPUXpJNkJBdENnbDdaY3NhRnBhWWVRRUdnbUpqbTRIUkJ6c0FwZHhYUFEzM1k3MkMzWmlCN2o3QWZQNG83UTAvb21WWUh2NGdOSkl3SURBUUFCbzRJQjF6Q0NBZE13UHdZSUt3WUJCUVVIQVFFRU16QXhNQzhHQ0NzR0FRVUZCekFCaGlOb2RIUndPaTh2YjJOemNDNWhjSEJzWlM1amIyMHZiMk56Y0RBekxYZDNaSEl3TkRBZEJnTlZIUTRFRmdRVWthU2MvTVIydDUrZ2l2Uk45WTgyWGUwckJJVXdEQVlEVlIwVEFRSC9CQUl3QURBZkJnTlZIU01FR0RBV2dCU0lKeGNKcWJZWVlJdnM2N3IyUjFuRlVsU2p0ekNDQVI0R0ExVWRJQVNDQVJVd2dnRVJNSUlCRFFZS0tvWklodmRqWkFVR0FUQ0IvakNCd3dZSUt3WUJCUVVIQWdJd2diWU1nYk5TWld4cFlXNWpaU0J2YmlCMGFHbHpJR05sY25ScFptbGpZWFJsSUdKNUlHRnVlU0J3WVhKMGVTQmhjM04xYldWeklHRmpZMlZ3ZEdGdVkyVWdiMllnZEdobElIUm9aVzRnWVhCd2JHbGpZV0pzWlNCemRHRnVaR0Z5WkNCMFpYSnRjeUJoYm1RZ1kyOXVaR2wwYVc5dWN5QnZaaUIxYzJVc0lHTmxjblJwWm1sallYUmxJSEJ2YkdsamVTQmhibVFnWTJWeWRHbG1hV05oZEdsdmJpQndjbUZqZEdsalpTQnpkR0YwWlcxbGJuUnpMakEyQmdnckJnRUZCUWNDQVJZcWFIUjBjRG92TDNkM2R5NWhjSEJzWlM1amIyMHZZMlZ5ZEdsbWFXTmhkR1ZoZFhSb2IzSnBkSGt2TUE0R0ExVWREd0VCL3dRRUF3SUhnREFRQmdvcWhraUc5Mk5rQmdzQkJBSUZBREFOQmdrcWhraUc5dzBCQVFVRkFBT0NBUUVBRGFZYjB5NDk0MXNyQjI1Q2xtelQ2SXhETUlKZjRGelJqYjY5RDcwYS9DV1MyNHlGdzRCWjMrUGkxeTRGRkt3TjI3YTQvdncxTG56THJSZHJqbjhmNUhlNXNXZVZ0Qk5lcGhtR2R2aGFJSlhuWTR3UGMvem83Y1lmcnBuNFpVaGNvT0FvT3NBUU55MjVvQVE1SDNPNXlBWDk4dDUvR2lvcWJpc0IvS0FnWE5ucmZTZW1NL2oxbU9DK1JOdXhUR2Y4YmdwUHllSUdxTktYODZlT2ExR2lXb1IxWmRFV0JHTGp3Vi8xQ0tuUGFObVNBTW5CakxQNGpRQmt1bGhnd0h5dmozWEthYmxiS3RZZGFHNllRdlZNcHpjWm04dzdISG9aUS9PamJiOUlZQVlNTnBJcjdONFl0UkhhTFNQUWp2eWdhWndYRzU2QWV6bEhSVEJoTDhjVHFBPT0iOwoJInB1cmNoYXNlLWluZm8iID0gImV3b0pJbTl5YVdkcGJtRnNMWEIxY21Ob1lYTmxMV1JoZEdVdGNITjBJaUE5SUNJeU1ERTVMVEEyTFRBNElEQXhPakk1T2pReElFRnRaWEpwWTJFdlRHOXpYMEZ1WjJWc1pYTWlPd29KSW5GMVlXNTBhWFI1SWlBOUlDSXhJanNLQ1NKemRXSnpZM0pwY0hScGIyNHRaM0p2ZFhBdGFXUmxiblJwWm1sbGNpSWdQU0FpTWpBeU1qVXlNVEFpT3dvSkltOXlhV2RwYm1Gc0xYQjFjbU5vWVhObExXUmhkR1V0YlhNaUlEMGdJakUxTlRrNU9ESTFPREV3TURBaU93b0pJbVY0Y0dseVpYTXRaR0YwWlMxbWIzSnRZWFIwWldRaUlEMGdJakl3TWpBdE1EY3RNVFVnTVRrNk16RTZNemdnUlhSakwwZE5WQ0k3Q2draWFYTXRhVzR0YVc1MGNtOHRiMlptWlhJdGNHVnlhVzlrSWlBOUlDSm1ZV3h6WlNJN0Nna2ljSFZ5WTJoaGMyVXRaR0YwWlMxdGN5SWdQU0FpTVRVNU1qSTBPVFE1T0RBd01DSTdDZ2tpWlhod2FYSmxjeTFrWVhSbExXWnZjbTFoZEhSbFpDMXdjM1FpSUQwZ0lqSXdNakF0TURjdE1UVWdNVEk2TXpFNk16Z2dRVzFsY21sallTOU1iM05mUVc1blpXeGxjeUk3Q2draWFYTXRkSEpwWVd3dGNHVnlhVzlrSWlBOUlDSm1ZV3h6WlNJN0Nna2lhWFJsYlMxcFpDSWdQU0FpTVRRMU1qVTNNVEk1TXlJN0Nna2lkVzVwY1hWbExXbGtaVzUwYVdacFpYSWlJRDBnSWpBd01EQTRNREl3TFRBd01UVXpRME5ETXpaRE1UQXdNa1VpT3dvSkltOXlhV2RwYm1Gc0xYUnlZVzV6WVdOMGFXOXVMV2xrSWlBOUlDSXlNREF3TURBMk16YzVNakEyTnpNaU93b0pJbVY0Y0dseVpYTXRaR0YwWlNJZ1BTQWlNVFU1TkRnME1UUTVPREF3TUNJN0Nna2lZWEJ3TFdsMFpXMHRhV1FpSUQwZ0lqSTVOVFkwTmpRMk1TSTdDZ2tpZEhKaGJuTmhZM1JwYjI0dGFXUWlJRDBnSWpJd01EQXdNRGd3TlRRME1EWXhNQ0k3Q2draVluWnljeUlnUFNBaU5ESTBORFkwSWpzS0NTSjNaV0l0YjNKa1pYSXRiR2x1WlMxcGRHVnRMV2xrSWlBOUlDSXlNREF3TURBeU9ERTRNamMyTkRjaU93b0pJblpsY25OcGIyNHRaWGgwWlhKdVlXd3RhV1JsYm5ScFptbGxjaUlnUFNBaU9ETXhORGN5TVRBNUlqc0tDU0ppYVdRaUlEMGdJbU52YlM1M1pXRjBhR1Z5TGxSWFF5STdDZ2tpY0hKdlpIVmpkQzFwWkNJZ1BTQWlZMjl0TG5kbFlYUm9aWEl1VkZkRExtbGhjQzV5Wlc1bGQybHVaeTR4Ylc5dWRHZ3VNU0k3Q2draWNIVnlZMmhoYzJVdFpHRjBaU0lnUFNBaU1qQXlNQzB3TmkweE5TQXhPVG96TVRvek9DQkZkR012UjAxVUlqc0tDU0p3ZFhKamFHRnpaUzFrWVhSbExYQnpkQ0lnUFNBaU1qQXlNQzB3TmkweE5TQXhNam96TVRvek9DQkJiV1Z5YVdOaEwweHZjMTlCYm1kbGJHVnpJanNLQ1NKdmNtbG5hVzVoYkMxd2RYSmphR0Z6WlMxa1lYUmxJaUE5SUNJeU1ERTVMVEEyTFRBNElEQTRPakk1T2pReElFVjBZeTlIVFZRaU93cDkiOwoJInBvZCIgPSAiMjAiOwoJInNpZ25pbmctc3RhdHVzIiA9ICIwIjsKfQ";
    private static String testIOSubscriptionPurchase = "{\n" +
            "    \"status\": 0,\n" +
            "    \"environment\": \"Production\",\n" +
            "    \"receipt\": {\n" +
            "        \"receipt_type\": \"Production\",\n" +
            "        \"adam_id\": 295646461,\n" +
            "        \"app_item_id\": 295646461,\n" +
            "        \"bundle_id\": \"com.product\",\n" +
            "        \"application_version\": \"424641\",\n" +
            "        \"download_id\": null,\n" +
            "        \"version_external_identifier\": 836465002,\n" +
            "        \"receipt_creation_date\": \"2020-06-22 20:33:00 Etc/GMT\",\n" +
            "        \"receipt_creation_date_ms\": \"1592857980000\",\n" +
            "        \"receipt_creation_date_pst\": \"2020-06-22 13:33:00 America/Los_Angeles\",\n" +
            "        \"request_date\": \"2020-07-05 20:05:24 Etc/GMT\",\n" +
            "        \"request_date_ms\": \"1593979524404\",\n" +
            "        \"request_date_pst\": \"2020-07-05 13:05:24 America/Los_Angeles\",\n" +
            "        \"original_purchase_date\": \"2020-06-22 20:32:54 Etc/GMT\",\n" +
            "        \"original_purchase_date_ms\": \"1592857974000\",\n" +
            "        \"original_purchase_date_pst\": \"2020-06-22 13:32:54 America/Los_Angeles\",\n" +
            "        \"in_app\": [\n" +
            "            {\n" +
            "                \"quantity\": \"1\",\n" +
            "                \"product_id\": \"com.product.inapp.sub.1year.1\",\n" +
            "                \"transaction_id\": \"170000290429417\",\n" +
            "                \"original_transaction_id\": \"170000290429417\",\n" +
            "                \"purchase_date\": \"2016-11-20 14:08:50 Etc/GMT\",\n" +
            "                \"purchase_date_ms\": \"1479650930000\",\n" +
            "                \"purchase_date_pst\": \"2016-11-20 06:08:50 America/Los_Angeles\",\n" +
            "                \"original_purchase_date\": \"2016-11-20 14:08:50 Etc/GMT\",\n" +
            "                \"original_purchase_date_ms\": \"1479650930000\",\n" +
            "                \"original_purchase_date_pst\": \"2016-11-20 06:08:50 America/Los_Angeles\",\n" +
            "                \"is_trial_period\": \"false\"\n" +
            "            },\n" +
            "            {\n" +
            "                \"quantity\": \"1\",\n" +
            "                \"product_id\": \"com.product.inapp.sub.1year.1\",\n" +
            "                \"transaction_id\": \"170000329551726\",\n" +
            "                \"original_transaction_id\": \"170000329551726\",\n" +
            "                \"purchase_date\": \"2017-04-17 00:43:18 Etc/GMT\",\n" +
            "                \"purchase_date_ms\": \"1492389798000\",\n" +
            "                \"purchase_date_pst\": \"2017-04-16 17:43:18 America/Los_Angeles\",\n" +
            "                \"original_purchase_date\": \"2017-04-17 00:43:18 Etc/GMT\",\n" +
            "                \"original_purchase_date_ms\": \"1492389798000\",\n" +
            "                \"original_purchase_date_pst\": \"2017-04-16 17:43:18 America/Los_Angeles\",\n" +
            "                \"is_trial_period\": \"false\"\n" +
            "            },\n" +
            "            {\n" +
            "                \"quantity\": \"1\",\n" +
            "                \"product_id\": \"com.product.iap.renewing.1year.pro\",\n" +
            "                \"transaction_id\": \"170000796308323\",\n" +
            "                \"original_transaction_id\": \"170000796308323\",\n" +
            "                \"purchase_date\": \"2020-06-22 20:32:54 Etc/GMT\",\n" +
            "                \"purchase_date_ms\": \"1592857974000\",\n" +
            "                \"purchase_date_pst\": \"2020-06-22 13:32:54 America/Los_Angeles\",\n" +
            "                \"original_purchase_date\": \"2020-06-22 20:32:55 Etc/GMT\",\n" +
            "                \"original_purchase_date_ms\": \"1592857975000\",\n" +
            "                \"original_purchase_date_pst\": \"2020-06-22 13:32:55 America/Los_Angeles\",\n" +
            "                \"expires_date\": \"2020-06-25 20:32:54 Etc/GMT\",\n" +
            "                \"expires_date_ms\": \"1593117174000\",\n" +
            "                \"expires_date_pst\": \"2020-06-25 13:32:54 America/Los_Angeles\",\n" +
            "                \"web_order_line_item_id\": \"170000299465370\",\n" +
            "                \"is_trial_period\": \"true\",\n" +
            "                \"is_in_intro_offer_period\": \"false\"\n" +
            "            }\n" +
            "        ]\n" +
            "    },\n" +
            "    \"latest_receipt_info\": [\n" +
            "        {\n" +
            "            \"quantity\": \"1\",\n" +
            "            \"product_id\": \"com.product.iap.renewing.1year.pro\",\n" +
            "            \"transaction_id\": \"170000797912210\",\n" +
            "            \"original_transaction_id\": \"170000796308323\",\n" +
            "            \"purchase_date\": \"2020-06-25 20:32:54 Etc/GMT\",\n" +
            "            \"purchase_date_ms\": \"1593117174000\",\n" +
            "            \"purchase_date_pst\": \"2020-06-25 13:32:54 America/Los_Angeles\",\n" +
            "            \"original_purchase_date\": \"2020-06-22 20:32:55 Etc/GMT\",\n" +
            "            \"original_purchase_date_ms\": \"1592857975000\",\n" +
            "            \"original_purchase_date_pst\": \"2020-06-22 13:32:55 America/Los_Angeles\",\n" +
            "            \"expires_date\": \"2021-06-25 20:32:54 Etc/GMT\",\n" +
            "            \"expires_date_ms\": \"1624653174000\",\n" +
            "            \"expires_date_pst\": \"2021-06-25 13:32:54 America/Los_Angeles\",\n" +
            "            \"web_order_line_item_id\": \"170000299465371\",\n" +
            "            \"is_trial_period\": \"false\",\n" +
            "            \"is_in_intro_offer_period\": \"false\",\n" +
            "            \"subscription_group_identifier\": \"20225210\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"quantity\": \"1\",\n" +
            "            \"product_id\": \"com.product.inapp.sub.1year.1\",\n" +
            "            \"transaction_id\": \"170000329551726\",\n" +
            "            \"original_transaction_id\": \"170000329551726\",\n" +
            "            \"purchase_date\": \"2017-04-17 00:43:18 Etc/GMT\",\n" +
            "            \"purchase_date_ms\": \"1492389798000\",\n" +
            "            \"purchase_date_pst\": \"2017-04-16 17:43:18 America/Los_Angeles\",\n" +
            "            \"original_purchase_date\": \"2017-04-17 00:43:18 Etc/GMT\",\n" +
            "            \"original_purchase_date_ms\": \"1492389798000\",\n" +
            "            \"original_purchase_date_pst\": \"2017-04-16 17:43:18 America/Los_Angeles\",\n" +
            "            \"is_trial_period\": \"false\"\n" +
            "        }\n" +
            "    ],\n" +
            "    \"latest_receipt\": \"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\",\n" +
            "    \"pending_renewal_info\": [\n" +
            "        {\n" +
            "            \"auto_renew_product_id\": \"com.product.iap.renewing.1year.pro\",\n" +
            "            \"original_transaction_id\": \"170000796308323\",\n" +
            "            \"product_id\": \"com.product.iap.renewing.1year.pro\",\n" +
            "            \"auto_renew_status\": \"1\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    @Test
    public void post() {
        IOSPurchaseApiClient iosPurchaseApiClient = new IOSPurchaseApiClient();
        try {
            String response = iosPurchaseApiClient.post(testReceiptData);
            Assert.assertNotNull(response);
            JSONObject jsonObject = new JSONObject(response);
            Assert.assertNotNull(jsonObject);
            Assert.assertEquals(1, jsonObject.getInt("auto_renew_status"));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void createIOSubscriptionPurchase() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(testIOSubscriptionPurchase, IOSubscriptionPurchase.class);
            Assert.assertNotNull(ioSubscriptionPurchase);
            Assert.assertEquals("Production", ioSubscriptionPurchase.getEnvironment());
        } catch (JsonProcessingException e) {
            Assert.fail(e.getMessage());
        }
    }
}
