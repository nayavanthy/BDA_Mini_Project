from fastapi import FastAPI
from pydantic import BaseModel

from Data_Collection import economic_indicator_collection, stock_data_collection
from Data_Load import start_load
from Data_Processing import run_data_preprocessing
from Model import run_modelling, run_inference

app = FastAPI()

# Define request model
class ActionRequest(BaseModel):
    action: str

@app.post("/process")
async def process_action(request: ActionRequest):
    """Handles button click actions from the frontend."""
    
    action_map = {
        "Data Collection": 0,
        "Load Data": 1,
        "Data Processing": 2,
        "Modelling": 3,
        "Inference": 4,
    }
    
    response = action_map.get(request.action, -1)

    if response == -1:
        return {"status": "failed", "message": "Invalid action"}

    try:
        if response == 0:
            economic_indicator_collection.run()
            stock_data_collection.run()
            return {"status": "success", "message": "Data collection completed"}

        elif response == 1:
            start_load.run()
            return {"status": "success", "message": "Data loaded successfully"}

        elif response == 2:
            run_data_preprocessing.run()
            return {"status": "success", "message": "Data processing completed"}

        elif response == 3:
            run_modelling.run()
            return {"status": "success", "message": "Model training completed"}

        elif response == 4:
            run_inference.run()
            return {"status": "success", "message": "Inference completed"}

    except Exception as e:
        return {"status": "error", "message": f"Error: {str(e)}"}

