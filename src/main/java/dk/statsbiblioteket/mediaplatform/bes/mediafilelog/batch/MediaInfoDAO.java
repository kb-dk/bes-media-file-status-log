package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.classic.Session;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Job;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Metadata;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.PreviewMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.ProgramMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.SnapshotMediaInfo;

public class MediaInfoDAO {

    private Logger logger = Logger.getLogger(MediaInfoDAO.class);
    private SessionFactory hibernateSessionFactory;

    public MediaInfoDAO(SessionFactory hibernateSessionFactory) {
        this.hibernateSessionFactory = hibernateSessionFactory;
    }

    public void create(Metadata metadata, 
            ProgramMediaInfo programMediaInfo,
            PreviewMediaInfo previewMediaInfo,
            List<SnapshotMediaInfo> snapshotsMediaInfo) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            hibernateSession.beginTransaction();
            hibernateSession.save(metadata);
            logger.debug("Saved object: " + metadata);
            hibernateSession.save(programMediaInfo);
            logger.debug("Saved object: " + programMediaInfo);
            hibernateSession.save(previewMediaInfo);
            logger.debug("Saved object: " + previewMediaInfo);
            for (SnapshotMediaInfo snapshotMediaInfo : snapshotsMediaInfo) {
                hibernateSession.save(snapshotMediaInfo);
                logger.debug("Saved object: " + snapshotMediaInfo);
            }
            hibernateSession.getTransaction().commit();
            logger.debug("Commited succesfully");
        } finally {
            hibernateSession.close();
        }
    }
    
    @SuppressWarnings("unchecked")
    public List<Metadata> readMetadata(String shardUuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            Query query = hibernateSession.createQuery("FROM Metadata WHERE " +
                    " (shardUuid = :shardUuid)"
                    ).setString("shardUuid", shardUuid);
            return (List<Metadata>) query.list();
        } finally {
            hibernateSession.close();
        }
    }
    
    @SuppressWarnings("unchecked")
    protected List<ProgramMediaInfo> readProgramMediaInfo(String shardUuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            Query query = hibernateSession.createQuery("FROM ProgramMediaInfo WHERE " +
                    " (shardUuid = :shardUuid)"
                    ).setString("shardUuid", shardUuid);
            return (List<ProgramMediaInfo>) query.list();
        } finally {
            hibernateSession.close();
        }
    }
    
    @SuppressWarnings("unchecked")
    protected List<PreviewMediaInfo> readPreviewMediaInfo(String shardUuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            Query query = hibernateSession.createQuery("FROM PreviewMediaInfo WHERE " +
                    " (shardUuid = :shardUuid)"
                    ).setString("shardUuid", shardUuid);
            return (List<PreviewMediaInfo>) query.list();
        } finally {
            hibernateSession.close();
        }
    }
    
    @SuppressWarnings("unchecked")
    protected List<SnapshotMediaInfo> readSnapshotMediaInfo(String shardUuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            Query query = hibernateSession.createQuery("FROM SnapshotMediaInfo WHERE " +
                    " (shardUuid = :shardUuid)"
                    ).setString("shardUuid", shardUuid);
            return (List<SnapshotMediaInfo>) query.list();
        } finally {
            hibernateSession.close();
        }
    }
}
