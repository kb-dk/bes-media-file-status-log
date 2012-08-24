package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class Metadata {

    private long id;
    private Date lastChangedDate;
    private String shardUuid;
    private String programUuid;
    private String sbChannelID;
    private String channelID;
    private String programTitle;
    private Date ritzauStartTime;
    private Date ritzauEndTime;
    private Date tvmeterStartTime;
    private Date tvmeterEndTime;
    private String mediaType;
    private String Note;
    
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public Date getLastChangedDate() {
        return lastChangedDate;
    }
    public void setLastChangedDate(Date lastChangedDate) {
        this.lastChangedDate = lastChangedDate;
    }
    public String getShardUuid() {
        return shardUuid;
    }
    public void setShardUuid(String uuid) {
        this.shardUuid = uuid;
    }
    public String getProgramUuid() {
        return programUuid;
    }
    public void setProgramUuid(String programUuid) {
        this.programUuid = programUuid;
    }
    public String getSbChannelID() {
        return sbChannelID;
    }
    public void setSbChannelID(String sbChannelID) {
        this.sbChannelID = sbChannelID;
    }
    public String getChannelID() {
        return channelID;
    }
    public void setChannelID(String channelID) {
        this.channelID = channelID;
    }
    public String getProgramTitle() {
        return programTitle;
    }
    public void setProgramTitle(String programTitle) {
        this.programTitle = programTitle;
    }
    public Date getRitzauStartTime() {
        return ritzauStartTime;
    }
    public void setRitzauStartTime(Date ritzauStartTime) {
        this.ritzauStartTime = ritzauStartTime;
    }
    public Date getRitzauEndTime() {
        return ritzauEndTime;
    }
    public void setRitzauEndTime(Date ritzauEndTime) {
        this.ritzauEndTime = ritzauEndTime;
    }
    public Date getTvmeterStartTime() {
        return tvmeterStartTime;
    }
    public void setTvmeterStartTime(Date tvmeterStartTime) {
        this.tvmeterStartTime = tvmeterStartTime;
    }
    public Date getTvmeterEndTime() {
        return tvmeterEndTime;
    }
    public void setTvmeterEndTime(Date tvmeterEndTime) {
        this.tvmeterEndTime = tvmeterEndTime;
    }
    public String getMediaType() {
        return mediaType;
    }
    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }
    public String getNote() {
        return Note;
    }
    public void setNote(String note) {
        Note = note;
    }
    @Override
    public String toString() {
        return "Metadata [id=" + id + ", lastChangedDate=" + lastChangedDate
                + ", shardUuid=" + shardUuid + ", programUuid=" + programUuid
                + ", sbChannelID=" + sbChannelID + ", programTitle="
                + programTitle + ", ritzauStartTime=" + ritzauStartTime
                + ", ritzauEndTime=" + ritzauEndTime + ", tvmeterStartTime="
                + tvmeterStartTime + ", tvmeterEndTime=" + tvmeterEndTime
                + ", mediaType=" + mediaType + ", Note=" + Note + "]";
    }
}
