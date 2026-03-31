import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

JOB_QUEUE_KEY = "job_queue"
ASSIGNED_JOBS_KEY = "assigned_jobs"
NODE_SESSION_KEY = "node_session_tokens"


def enqueue_job(job_id: str) -> None:
    r.lrem(JOB_QUEUE_KEY, 0, job_id)
    if not r.hexists(ASSIGNED_JOBS_KEY, job_id):
        r.lpush(JOB_QUEUE_KEY, job_id)


def remove_job_from_queue(job_id: str) -> None:
    r.lrem(JOB_QUEUE_KEY, 0, job_id)


def set_node_session(node_id: str, session_token: str) -> None:
    r.hset(NODE_SESSION_KEY, node_id, session_token)


def get_node_session(node_id: str) -> str | None:
    return r.hget(NODE_SESSION_KEY, node_id)
