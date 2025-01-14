/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.janusgraph.kggraph.json;


/**
 * The set of available GraphSON versions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum TrsGraphSONVersion{
    TRS(TrsGraphSONModule.GraphSONModuleV3d0.build(), "TRS");

    private final TrsGraphSONModule.GraphSONModuleBuilder builder;
    private final String versionNumber;

    TrsGraphSONVersion(final TrsGraphSONModule.GraphSONModuleBuilder builder, final String versionNumber) {
        this.builder = builder;
        this.versionNumber = versionNumber;
    }

    public TrsGraphSONModule.GraphSONModuleBuilder getBuilder() {
        return builder;
    }

    public String getVersion() {
        return versionNumber;
    }
}
