package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.util.List;

import org.apache.log4j.Logger;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Job;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.ProgramMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Metadata;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.PreviewMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.SnapshotMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.exception.JobAlreadyStartedException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionConnectToDOMSException;

public class JobService {

    private final Logger logger = Logger.getLogger(JobService.class);
    private final JobDAO jobDAO;
    private final MediaInfoService mediaInfoService;

    public JobService(JobDAO jobDAO, MediaInfoService mediaInfoService) {
        this.jobDAO = jobDAO;
        this.mediaInfoService = mediaInfoService;
    }
    
    public void addJobs(List<String> shardPids) {
        jobDAO.addJobs(shardPids);
    }

    public void addNonExistingJobs(List<String> shardPids) {
        jobDAO.addNonExistingJobs(shardPids);
    }
    
    public boolean execute(Job job) throws JobAlreadyStartedException, DOMSMetadataExtractionConnectToDOMSException {
        boolean doneSuccesfully = false;
        logger.info("Started job: " + job);
        Metadata metadata = mediaInfoService.retrieveMetadata(job.getUuid());
        ProgramMediaInfo programMediaInfo = mediaInfoService.retrieveProgramMediaInfo(job.getUuid());
        PreviewMediaInfo previewMediaInfo = mediaInfoService.retrievePreviewMediaInfo(job.getUuid());
        List<SnapshotMediaInfo> snapshotsMediaInfo = mediaInfoService.retrieveSnapshotMediaInfo(job.getUuid());
        mediaInfoService.save(metadata, programMediaInfo, previewMediaInfo, snapshotsMediaInfo);
        jobDAO.finishJob(job);
        doneSuccesfully = true;
        logger.info("Finished job: " + job); 
        return doneSuccesfully;
    }

    public void executeJobs() throws DOMSMetadataExtractionConnectToDOMSException {
        int numberOfJobsInStateToDoAtLastLoop = Integer.MAX_VALUE;
        int numberOfJobsInStateToDoAtCurrentLoop = jobDAO.getNumberOfJobsInStateToDo();
        logger.info("Number of jobs in state todo: " + numberOfJobsInStateToDoAtCurrentLoop);
        while((numberOfJobsInStateToDoAtCurrentLoop > 0)
                && (numberOfJobsInStateToDoAtCurrentLoop < numberOfJobsInStateToDoAtLastLoop)) {
            Job job = jobDAO.getAJobInStateTodo();
            try {
                execute(job);
            } catch (JobAlreadyStartedException e) {
                Job startedJob = jobDAO.getJob(job.getUuid());
                logger.warn("Job started before execution. Ignoring job: " + job + ". Most recent state was: " + startedJob);
            }
            logger.debug("Number of jobs in last loop   : " + numberOfJobsInStateToDoAtLastLoop);
            logger.debug("Number of jobs in current loop: " + numberOfJobsInStateToDoAtCurrentLoop);
            numberOfJobsInStateToDoAtLastLoop = numberOfJobsInStateToDoAtCurrentLoop;
            numberOfJobsInStateToDoAtCurrentLoop = jobDAO.getNumberOfJobsInStateToDo();
            logger.info("Number of jobs in state todo: " + numberOfJobsInStateToDoAtCurrentLoop);
        }
    }
}
