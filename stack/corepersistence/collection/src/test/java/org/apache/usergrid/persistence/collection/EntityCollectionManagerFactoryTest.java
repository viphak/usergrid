package org.apache.usergrid.persistence.collection;


import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.usergrid.persistence.collection.guice.TestCollectionModule;
import org.apache.usergrid.persistence.collection.impl.CollectionScopeImpl;
import org.apache.usergrid.persistence.model.entity.SimpleId;

import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;

import static org.junit.Assert.assertNotNull;


/**
 * Basic tests
 *
 * @author tnine
 */

public class EntityCollectionManagerFactoryTest {

    @Rule
    public GuiceBerryRule guiceBerry = new GuiceBerryRule(TestCollectionModule.class);

    @Inject
    private EntityCollectionManagerFactory entityCollectionManagerFactory;




    @Test
    public void validInput() {

        CollectionScopeImpl context = new CollectionScopeImpl(new SimpleId( "organization" ), new SimpleId( "test" ), "test" );

        EntityCollectionManager entityCollectionManager =
                entityCollectionManagerFactory.createCollectionManager( context );

        assertNotNull( "A collection manager must be returned", entityCollectionManager );
    }


    @Test(expected = ProvisionException.class)
    public void nullInput() {
        entityCollectionManagerFactory.createCollectionManager( null );
    }

}
