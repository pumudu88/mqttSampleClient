/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.sample.mqtt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * If MQTT Retain enabled broker should keep the retain enabled message for future subscribers.
 * This samples demonstrates how MQTT retain feature works.
 */
public class Main {

    private static final Log log = LogFactory.getLog(Main.class);

    /**
     * Java temporary directory location
     */
    private static final String JAVA_TMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * The MQTT broker URL
     */
    private static String brokerURL = "tcp://127.0.0.1:1883";

    /**
     * topic name for subscriber and publisher
     */
    private static String topic = "simpleTopic";

    /**
     * retain state for published topic
     */
    private static boolean retained  = false;

    /**
     *
     */
    private static String clientType = "SUB";

    /**
     *
     */
    private static int numberOfMessages = 1;

    /**
     *
     */
    private static int QOSLevel = 0;

    /**
     *
     */
    private static String clientId = "default_client";

    /**
     * The main method which runs the sample.
     *
     * @param args Commandline arguments
     */
    public static void main(String[] args) throws InterruptedException, IOException {

        // buffer reader for read console inputs
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));

        log.info("MQTT Sample");
        getUserInputs(bufferReader);

        try {
            if(clientType.equalsIgnoreCase("SUB")) {
                createSubscriber();
            }
            if(clientType.equalsIgnoreCase("PUB")) {
                createPublisher();
            }
        } catch (MqttException e) {
            log.error("Error running the sample", e);
        } finally {
            bufferReader.close();
        }
    }


    /**
     *
     * @throws MqttException
     */
    private static void createPublisher() throws MqttException {


        log.info("======= Publisher ========");
        log.info(" broker URL : " + brokerURL);
        log.info(" client id  : " + clientId );
        log.info(" QOS Level  : " + QOSLevel );
        log.info(" retain flag: " + retained );
        log.info(" Num of msgs: " + numberOfMessages );
        log.info("===========================");

        // Creating mqtt publisher client
        MqttClient mqttPublisherClient = getNewMqttClient(clientId);

        for (int i = 1; i <= numberOfMessages; i++) {

            // default payload
            byte[] payload = ("sample message payload. "+ "client id : " + clientId + ". message number : " + i).getBytes();

//            byte[] payload = "".getBytes();

            // Publishing to mqtt topic "simpleTopic"
            mqttPublisherClient.publish(topic, payload, QOSLevel, retained);
        }

        mqttPublisherClient.disconnect();

    }

    /**
     *
     * @throws MqttException
     */
    private static void createSubscriber() throws MqttException {

        log.info("======= Subscriber ========");
        log.info(" broker URL : " + brokerURL);
        log.info(" client id  : " + clientId );
        log.info(" QOS Level  : " + QOSLevel );
        log.info("===========================");

        // Creating mqtt subscriber client
        MqttClient mqttSubscriberClient = getNewMqttClient(clientId);

        // Subscribing to mqtt topic
        mqttSubscriberClient.subscribe(topic, QOSLevel);

        while (true) {

        }

    }

    /**
     * Read user inputs and set to relevant parameters
     *
     * @param bufferReader buffer text from character input stream
     * @throws java.io.IOException
     */
    private static void getUserInputs(BufferedReader bufferReader) throws IOException {

        String lineSeparator = System.getProperty("line.separator");

        log.info("Enter Client ID : ");
        String bufferReaderString = bufferReader.readLine();
        if(!bufferReaderString.isEmpty()) {
            clientId = bufferReaderString;
        }


        log.info("Client type [SUB/PUB] : ");
        bufferReaderString = bufferReader.readLine();
        if(!bufferReaderString.isEmpty()) {
            clientType = bufferReaderString;
        }

        log.info("Enter topic name : ");
        bufferReaderString = bufferReader.readLine();
        if (!bufferReaderString.isEmpty()) {
            topic = bufferReaderString;
        }

        log.info("Enter  QOS level : ");
        bufferReaderString = bufferReader.readLine();
        if (!bufferReaderString.isEmpty()) {
            QOSLevel = Integer.parseInt(bufferReaderString);
        }

        log.info("Enter MQTT broker url with port (ex: tcp://10.100.5.165:1883 )");
        bufferReaderString = bufferReader.readLine();
        if (!bufferReaderString.isEmpty()) {
            brokerURL = bufferReaderString;
        }


        if(clientType.equalsIgnoreCase("PUB")) {
            log.info("Number of Messages to publish (integer) : ");
            bufferReaderString = bufferReader.readLine();
            if (!bufferReaderString.isEmpty()) {
                numberOfMessages = Integer.parseInt(bufferReaderString);
            }

            log.info("Set retain flag [Y/N] : ");
            bufferReaderString = bufferReader.readLine();
            if (bufferReaderString.equalsIgnoreCase("Y")) {
                // set retain enable
                retained = true;
            } else if (bufferReaderString.equalsIgnoreCase("N")) {
                // set retain disable
                retained = false;
            }
        }

        log.info(lineSeparator + "Enter Y to continue" + lineSeparator +
                 "Enter N to revise parameters [Y/N] : ");
        bufferReaderString = bufferReader.readLine();
        if (bufferReaderString.equalsIgnoreCase("N")) {
            getUserInputs(bufferReader);
        }



    }

    /**
     * Crate a new MQTT client and connect it to the server.
     *
     * @param clientId The unique mqtt client Id
     * @return Connected MQTT client
     * @throws MqttException
     */
    private static MqttClient getNewMqttClient(String clientId) throws MqttException {
        //Store messages until server fetches them
        MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(JAVA_TMP_DIR + "/" + clientId);

        MqttClient mqttClient = new MqttClient(brokerURL, clientId, dataStore);
        SimpleMQTTCallback callback = new SimpleMQTTCallback();
        mqttClient.setCallback(callback);

        MqttConnectOptions connectOptions = new MqttConnectOptions();

        connectOptions.setUserName("admin");
        connectOptions.setPassword("admin".toCharArray());
        connectOptions.setCleanSession(true);
        mqttClient.connect(connectOptions);

        return mqttClient;
    }

}
