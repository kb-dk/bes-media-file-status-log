package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.mock;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;
import javax.naming.Reference;

import org.hibernate.HibernateException;
import org.hibernate.Interceptor;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.classic.Session;
import org.hibernate.engine.FilterDefinition;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metadata.CollectionMetadata;
import org.hibernate.stat.Statistics;

public class SessionFactoryMock implements SessionFactory {

    @Override
    public Reference getReference() throws NamingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evict(Class arg0) throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evict(Class arg0, Serializable arg1) throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evictCollection(String arg0) throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evictCollection(String arg0, Serializable arg1)
            throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evictEntity(String arg0) throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evictEntity(String arg0, Serializable arg1)
            throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evictQueries() throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void evictQueries(String arg0) throws HibernateException {
        // TODO Auto-generated method stub

    }

    @Override
    public Map getAllClassMetadata() throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map getAllCollectionMetadata() throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClassMetadata getClassMetadata(Class arg0) throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClassMetadata getClassMetadata(String arg0)
            throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CollectionMetadata getCollectionMetadata(String arg0)
            throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Session getCurrentSession() throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set getDefinedFilterNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FilterDefinition getFilterDefinition(String arg0)
            throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statistics getStatistics() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Session openSession() throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Session openSession(Connection arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Session openSession(Interceptor arg0) throws HibernateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Session openSession(Connection arg0, Interceptor arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StatelessSession openStatelessSession() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StatelessSession openStatelessSession(Connection arg0) {
        // TODO Auto-generated method stub
        return null;
    }

}
