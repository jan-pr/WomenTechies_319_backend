from datetime import datetime
from fastapi import FastAPI
import uuid
from scheduler.scheduler import assign_job_to_node
app = FastAPI()

# In-memory storage
nodes: dict[str, dict] = {}
jobs: dict = {}
job_queue: list = []

@app.get("/")
def root():
    return {"message": "Backend running"}

@app.get("/health")
def health():
    return {"status": "OK"}

@app.post("/register-node")
def register_node(node_id: str):
    nodes[node_id] = {
        "id": node_id,
        "status": "idle",
        "availability": "idle",

        "metrics": {
            "success_rate": 0.9,
            "uptime": 0.95,
            "speed": 1.0
        },

        "carbon_intensity": 300,

        "cpu": 0,
        "last_seen": str(datetime.utcnow())
    }

    return {"message": f"Node {node_id} registered"}

@app.get("/debug")
def debug():
    return {
        "nodes": nodes,
        "jobs": jobs,
        "queue": job_queue
    }

@app.get("/nodes")
def get_nodes():
    return nodes

@app.post("/submit-job")
def submit_job(task_type: str = "demo_task"):
    import uuid

    job_id = str(uuid.uuid4())

    job = {
        "id": job_id,
        "task_type": task_type,
        "status": "queued"
    }

    jobs[job_id] = job
    job_queue.append(job)

    return {
        "message": "Job submitted",
        "job_id": job_id,
        "task_type": task_type
    }

@app.post("/assign-job")
def assign_job():
    if not job_queue:
        return {"message": "No jobs in queue"}

    # peek first job without removing
    job = job_queue[0]

    # convert nodes dict → list
    node_list = list(nodes.values())

    # call scheduler
    result = assign_job_to_node(job, node_list)

    if not result["assigned"]:
        return result

    node_id = result["node_id"]

    # update job
    jobs[job["id"]]["status"] = "running"
    jobs[job["id"]]["assigned_node"] = node_id

    # update node
    nodes[node_id]["status"] = "busy"
    nodes[node_id]["availability"] = "busy"

    # lightly improve node metrics after successful assignment
    nodes[node_id]["metrics"]["success_rate"] = min(
        1.0, nodes[node_id]["metrics"].get("success_rate", 0) + 0.01
    )
    nodes[node_id]["metrics"]["uptime"] = min(
        1.0, nodes[node_id]["metrics"].get("uptime", 0) + 0.005
    )

    # remove job from queue only after successful assignment
    job_queue.pop(0)

    return result