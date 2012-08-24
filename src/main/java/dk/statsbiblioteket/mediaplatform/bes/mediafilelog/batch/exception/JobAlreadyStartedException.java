package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.exception;

@SuppressWarnings("serial")
public class JobAlreadyStartedException extends Exception {

    public JobAlreadyStartedException(String msg) {
        super(msg);
    }

}
