/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.Set;
import java.lang.Math;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;

public class CoordinateUtil {

    public static double calculateDistance(NetworkCoordinate coordinateA, NetworkCoordinate coordinateB) {
        double sumsq = 0.0;
        double[] coordinateVectorA = coordinateA.getCoordinateVector();
        double[] coordinateVectorB = coordinateB.getCoordinateVector();
        for(int i = 0; i < coordinateVectorA.length; i++) {
            double diff = coordinateVectorA[i] - coordinateVectorB[i];
            sumsq += diff*diff;
        }
        
        System.out.println("Vector diff = "+sumsq);
        if (Math.sqrt(sumsq) < 1E-6)
            return 0;

        double rtt = Math.sqrt(sumsq) + coordinateA.getHeight() + coordinateB.getHeight();

        System.out.println("RTT after height addition = "+ rtt);

        double adjusted = rtt + coordinateA.getAdjustment() + coordinateB.getAdjustment();

        System.out.println("adjustment = "+ adjusted);
        if(adjusted > 0.0) {
            rtt = adjusted;
        }

        return rtt;
    }
}
