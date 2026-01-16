from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI(title="Babel Briefings Data API")

CURATED_BASE_PATH = "/data/curated"

@app.get("/")
def read_root():
    return {"message": "Babel Briefings API is active. Use /quarterly/{year}/{quarter} to fetch data."}

@app.get("/quarterly/{year}/{quarter}")
def get_quarterly_data(year: int, quarter: int):
    """Fetch quarterly aggregated NLP data for ML consumption"""
    path = f"{CURATED_BASE_PATH}/year={year}/quarter={quarter}"
    
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Data for Year {year} Quarter {quarter} not found.")
    
    try:
        df = pd.read_parquet(path)
        results = df.to_dict(orient="records")
        return {
            "year": year,
            "quarter": quarter,
            "count": len(results),
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/datasets")
def list_datasets():
    """List available curated datasets"""
    datasets = []
    if os.path.exists(CURATED_BASE_PATH):
        for year_dir in os.listdir(CURATED_BASE_PATH):
            year_path = os.path.join(CURATED_BASE_PATH, year_dir)
            if os.path.isdir(year_path):
                for quarter_dir in os.listdir(year_path):
                    datasets.append(f"{year_dir}/{quarter_dir}")
    return {"datasets": datasets}

@app.get("/health")
def health():
    return {"status": "healthy"}