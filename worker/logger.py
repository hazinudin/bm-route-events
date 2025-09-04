import logging

# Common log format
LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"

class JobAdapter(logging.LoggerAdapter):
    """
    Adapter to add Job ID into log messages.
    """
    def process(self, msg, kwargs):
        job_id = self.extra.get("job_id", "N/A")
        return f"[job_id={job_id}] {msg}", kwargs

def setup_logger(name="app", level=logging.INFO):
    """
    Setup a logger for general logging.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:  # Prevent duplicate handlers
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter(LOG_FORMAT)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger

def get_job_logger(job_id):
    """
    Get a logger for a specific job.
    """
    base_logger = setup_logger("jobs")
    return JobAdapter(base_logger, {"job_id": job_id})
