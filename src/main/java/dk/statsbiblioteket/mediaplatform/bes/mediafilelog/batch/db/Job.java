package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Job {

    private String uuid;
    private String status;
    private Date lastTouched;

    public Job() {
        super();
    }
    
    public Job(String uuid, String status, Date lastTouched) {
        super();
        this.uuid = uuid;
        this.status = status;
        this.lastTouched = lastTouched;
    }
    
    @Id
    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getLastTouched() {
        return lastTouched;
    }

    public void setLastTouched(Date lastTouched) {
        this.lastTouched = lastTouched;
    }

    @Override
    public String toString() {
        return "Job [uuid=" + uuid + ", status=" + status + ", lastTouched="
                + lastTouched + "]";
    }
    
}
