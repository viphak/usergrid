/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.usergrid.persistence.core.guice;


import java.io.IOException;

import org.apache.usergrid.persistence.core.cassandra.CassandraService;
import org.apache.usergrid.persistence.core.migration.MigrationException;
import org.apache.usergrid.persistence.core.migration.MigrationManager;

import com.google.guiceberry.GuiceBerryEnvMain;
import com.google.guiceberry.GuiceBerryModule;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.netflix.config.ConfigurationManager;


public class GuiceBerryITModule extends AbstractModule {


    @Override
    protected void configure() {
        //stupid, but neccessary for this env
        install( new GuiceBerryModule() );
        bind( GuiceBerryEnvMain.class ).to( CollectionModuleRuntime.class );
    }


    private static class CollectionModuleRuntime implements GuiceBerryEnvMain {

        private final MigrationManager migrationManager;


        @Inject
        private CollectionModuleRuntime( final MigrationManager migrationManager ) {
            this.migrationManager = migrationManager;
        }


        @Override
        public void run() {

            try {
                CassandraService.INSTANCE.start();
            }
            catch ( Throwable throwable ) {
                throw new RuntimeException( "Cannot start rule", throwable );
            }

            try {
                migrationManager.migrate();
            }
            catch ( MigrationException e ) {
                throw new RuntimeException( "Cannot migrate", e );
            }
        }
    }


    /**
     * This super sucks, but our lifecycle down't work properly otherwise
     */
    static {
        //set up the property wiring
        System.setProperty( "archaius.deployment.environment", "UNIT" );
        try {
            //load up the properties
            ConfigurationManager.loadCascadedPropertiesFromResources( "usergrid" );
        }
        catch ( IOException e ) {
            throw new RuntimeException( "Cannot do much without properly loading our configuration.", e );
        }
    }
}
