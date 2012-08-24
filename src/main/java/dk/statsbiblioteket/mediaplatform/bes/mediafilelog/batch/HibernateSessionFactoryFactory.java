package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.io.File;

import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Job;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.ProgramMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Metadata;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.PreviewMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.SnapshotMediaInfo;

public class HibernateSessionFactoryFactory {

    private static Logger logger = Logger.getLogger(HibernateSessionFactoryFactory.class);
    
    public static SessionFactory create(String hibernateConfigFilePath) {
        File file = new File(hibernateConfigFilePath);
        logger.debug("Looking for config file in: " + file.getAbsolutePath());
        AnnotationConfiguration configure = (new AnnotationConfiguration()).configure(file);
        configure.addAnnotatedClass(Job.class);
        configure.addAnnotatedClass(Metadata.class);
        configure.addAnnotatedClass(ProgramMediaInfo.class);
        configure.addAnnotatedClass(PreviewMediaInfo.class);
        configure.addAnnotatedClass(SnapshotMediaInfo.class);
        SessionFactory hibernateSessionFactory = configure.buildSessionFactory();
        return hibernateSessionFactory;
    }

}
