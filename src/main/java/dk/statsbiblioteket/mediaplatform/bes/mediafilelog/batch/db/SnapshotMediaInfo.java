package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class SnapshotMediaInfo {

    private long id;
    private Date lastTouched;
    private String shardUuid;
    private boolean fileExists;
    private String filename;
    private long fileSizeByte;
    private Date fileTimestamp;
    private int snapshotTime;
    private String transcodeCommandLine;
    private String note;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public Date getLastTouched() {
        return lastTouched;
    }
    public void setLastTouched(Date lastTouched) {
        this.lastTouched = lastTouched;
    }
    public String getShardUuid() {
        return shardUuid;
    }
    public void setShardUuid(String shardUuid) {
        this.shardUuid = shardUuid;
    }
    public boolean isFileExists() {
        return fileExists;
    }
    public void setFileExists(boolean fileExists) {
        this.fileExists = fileExists;
    }
    public String getFilename() {
        return filename;
    }
    public void setFilename(String filename) {
        this.filename = filename;
    }
    public long getFileSizeByte() {
        return fileSizeByte;
    }
    public void setFileSizeByte(long fileSizeByte) {
        this.fileSizeByte = fileSizeByte;
    }
    public Date getFileTimestamp() {
        return fileTimestamp;
    }
    public void setFileTimestamp(Date fileTimestamp) {
        this.fileTimestamp = fileTimestamp;
    }
    public int getSnapshotTime() {
        return snapshotTime;
    }
    public void setSnapshotTime(int snapshotTime) {
        this.snapshotTime = snapshotTime;
    }
    public String getTranscodeCommandLine() {
        return transcodeCommandLine;
    }
    public void setTranscodeCommandLine(String transcodeCommandLine) {
        this.transcodeCommandLine = transcodeCommandLine;
    }
    public String getNote() {
        return note;
    }
    public void setNote(String note) {
        this.note = note;
    }
    @Override
    public String toString() {
        return "SnapshotMediaInfo [id=" + id + ", lastTouched=" + lastTouched
                + ", shardUuid=" + shardUuid + ", fileExists=" + fileExists
                + ", filename=" + filename + ", fileSizeByte=" + fileSizeByte
                + ", fileTimestamp=" + fileTimestamp + ", snapshotTime="
                + snapshotTime + ", transcodeCommandLine="
                + transcodeCommandLine + ", note=" + note + "]";
    }
    
}
