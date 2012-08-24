package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception;

@SuppressWarnings("serial")
public class DOMSMetadataExtractionException extends Exception {

	public DOMSMetadataExtractionException(String msg, Throwable t) {
		super(msg, t);
	}

	public DOMSMetadataExtractionException(String msg) {
		super(msg);
	}

}
