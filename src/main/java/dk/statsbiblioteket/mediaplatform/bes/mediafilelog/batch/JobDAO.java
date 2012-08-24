package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.classic.Session;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Job;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.exception.JobAlreadyStartedException;


public class JobDAO {

    private static Logger logger = Logger.getLogger(JobDAO.class);

    private final String stateNameTodo = "Todo";
    private final String stateNameWIP = "WIP";
    private final String stateNameDone = "Done";

    private SessionFactory hibernateSessionFactory;

    public JobDAO(SessionFactory hibernateSessionFactory) {
        this.hibernateSessionFactory = hibernateSessionFactory;
    }

    public int getNumberOfJobsInStateToDo() {
        Session hibernateSession = hibernateSessionFactory.openSession();
        int count = -1;
        try {
            count = ((Long) hibernateSession.createQuery(
                    "SELECT count(*) " +
                    "FROM Job " +
                    "WHERE (status = :todoStateName)")
                    .setString("todoStateName", stateNameTodo)
                    .uniqueResult()).intValue();
        } finally {
            hibernateSession.close();
        }
        return count;
    }

    /**
     * @return number of jobs or -1 if something goes wrong
     */
    public int getNumberOfAllJobs() {
        Session hibernateSession = hibernateSessionFactory.openSession();
        int count = -1;
        try {
            count = ((Long) hibernateSession.createQuery(
                    "SELECT count(*) " +
                    "FROM Job").uniqueResult()).intValue();
        } finally {
            hibernateSession.close();
        }
        return count;
    }

    public void addJobs(List<String> shardPids) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            hibernateSession.beginTransaction();
            for (String uuid : shardPids) {
                Job job = getJob(hibernateSession, uuid);
                if (job == null) {
                    Job newJob = new Job(uuid, stateNameTodo, new Date());
                    logger.info("Adding job: " + newJob);
                    hibernateSession.save(newJob);
                } else {
                    logger.info("Job with uuid " + job.getUuid() + " already exists: " + job + ". " +
                    		"Switch state to " + stateNameTodo);
                    job.setStatus(stateNameTodo);
                    job.setLastTouched(new Date());
                    hibernateSession.update(job);
                }
            }
            hibernateSession.getTransaction().commit();
        } finally {
            hibernateSession.close();
        }
    }

    public void addNonExistingJobs(List<String> uuids) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            hibernateSession.beginTransaction();
            for (String uuid : uuids) {
                Job job = getJob(hibernateSession, uuid);
                if (job == null) {
                    Job newJob = new Job(uuid, stateNameTodo, new Date());
                    hibernateSession.save(newJob);
                } else {
                    logger.info("Job with uuid " + job.getUuid() + " already exists: " + job);
                }
            }
            hibernateSession.getTransaction().commit();
        } finally {
            hibernateSession.close();
        }

    }

    /**
     * @return uuid or null if no job could be started
     */
    public Job startAJob() {
        Session hibernateSession = hibernateSessionFactory.openSession();
        hibernateSession.beginTransaction();
        Job job = getAJobInStateTodo(hibernateSession);
        if (job!=null) {
            job.setStatus(stateNameWIP);
            job.setLastTouched(new Date());
            hibernateSession.update(job);
            hibernateSession.getTransaction().commit();
        }
        if (hibernateSession.getTransaction().wasRolledBack()) {
            job = null;
        }
        hibernateSession.close();
        return job;
    }

    public void startJob(Job job) throws JobAlreadyStartedException {
        Session hibernateSession = hibernateSessionFactory.openSession();
        hibernateSession.beginTransaction();
        try {
            if (job==null) {
                throw new RuntimeException("Job was null. Could not start.");
            }
            Job existingJob = getJob(hibernateSession, job.getUuid());
            if (!existingJob.getStatus().equals(stateNameTodo)) {
                throw new JobAlreadyStartedException("Job was already started. State: " + existingJob);
            }
            existingJob.setStatus(stateNameWIP);
            existingJob.setLastTouched(new Date());
            hibernateSession.update(existingJob);
            hibernateSession.getTransaction().commit();
        } finally {
            hibernateSession.close();
        }
    }

    public Job getAJobInStateTodo() {
        Session hibernateSession = hibernateSessionFactory.openSession();
        Job job = null;
        try {
            job = getAJobInStateTodo(hibernateSession);
        } finally {
            hibernateSession.close();
        }
        return job;
    }

    protected Job getAJobInStateTodo(Session hibernateSession) {
        Job job = null;
        Query query = hibernateSession.createQuery("FROM Job WHERE " +
                " (status = :todoStateName)"
                ).setString("todoStateName", stateNameTodo);
        if (query.list().size() > 0) {
            job = (Job) query.list().get(0);
        }
        return job;
    }

    @Deprecated
    public String getJobStatus(String uuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        String jobStatus = null;
        try {
            Job job = getJob(hibernateSession, uuid);
            if (job!=null) {
                jobStatus = job.getStatus();
            }
        } finally {
            hibernateSession.close();
        }
        return jobStatus;
    }

    public Date getJobChangedDate(String uuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        Date jobChanged = null;
        try {
            Job job = getJob(hibernateSession, uuid);
            if (job!=null) {
                jobChanged = job.getLastTouched();
            }
        } finally {
            hibernateSession.close();
        }
        return jobChanged;
    }

    public Job getJob(String uuid) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        Job job = null;
        try {
            job = getJob(hibernateSession, uuid);
        } finally {
            hibernateSession.close();
        }
        return job;
    }

    protected Job getJob(Session hibernateSession, String uuid) {
        Job job = null;
        Query query = hibernateSession.createQuery(
                "FROM Job " +
                        "WHERE (uuid = :uuid)"
                ).setString("uuid", uuid);
        if (query.list().size() > 0) {
            job = (Job) query.list().get(0);
        }
        return job;
    }

    public void finishJob(Job job) {
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            if (job!=null) {
                hibernateSession.beginTransaction();
                job.setStatus(stateNameDone);
                job.setLastTouched(new Date());
                hibernateSession.update(job);
                hibernateSession.getTransaction().commit();
            }
        } finally {
            hibernateSession.close();
        }
    }

    /**
     * @return list of all job entries in the db
     */
    @SuppressWarnings("unchecked")
    public List<Job> getAllJobs() {
        List<Job> jobs = null;
        Session hibernateSession = hibernateSessionFactory.openSession();
        try {
            Query query = hibernateSession.createQuery("FROM Job");
            jobs = (List<Job>) query.list();
        } finally {
            hibernateSession.close();
        }
        return jobs;
    }
}
