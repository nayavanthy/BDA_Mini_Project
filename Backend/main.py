from fastapi import FastAPI
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import json
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with allowed frontend origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the directory where the images are saved
image_dir = "/home/captain/Desktop/BDA/Backend/DashBoard"
app.mount("/static", StaticFiles(directory=image_dir), name="static")

# Define request model
class ActionRequest(BaseModel):
    action: str

action_map = {
    "Data Collection": ["python3", "/home/captain/Desktop/BDA/Backend/Data_Collection/economic_indicator_collection.py"],
    "Load Data": ["python3", "/home/captain/Desktop/BDA/Backend/Data_Load/start_load.py"],
    "Data Processing": ["python3", "/home/captain/Desktop/BDA/Backend/Data_Processing/run_data_preprocessing.py"],
    "Modelling": ["python3", "/home/captain/Desktop/BDA/Backend/Model/run_modelling.py"],
    "Inference": ["python3", "/home/captain/Desktop/BDA/Backend/Model/run_inference.py"],
}

def run_command_with_logs(command):
    """Runs a command and streams its output."""
    try:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in iter(process.stdout.readline, ""):
            yield line
        process.stdout.close()
        process.wait()
    except Exception as e:
        yield f"Error: {str(e)}"

@app.post("/process")
async def process_action(request: ActionRequest):
    """Handles frontend requests and returns either logs or a plot image."""
    command = action_map.get(request.action)
    
    if not command:
        return JSONResponse(content={"status": "failed", "message": "Invalid action"}, status_code=400)

    return StreamingResponse(run_command_with_logs(command), media_type="text/plain")
