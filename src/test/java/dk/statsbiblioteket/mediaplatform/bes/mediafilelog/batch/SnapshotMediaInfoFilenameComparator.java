package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.util.Comparator;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.SnapshotMediaInfo;

public class SnapshotMediaInfoFilenameComparator implements Comparator<SnapshotMediaInfo> {

    @Override
    public int compare(SnapshotMediaInfo o1, SnapshotMediaInfo o2) {
        if (o1==o2) {
            return 0;
        } else if (o1 == null) {
            return -1;
        } else if (o2 == null) {
            return 1;
        } else {
            return o1.getFilename().compareTo(o2.getFilename());
        }
    }
}
