"""
FastAPI stub — Day 1.
Expands significantly on Days 4 and 5.
"""

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="ConstraintMesh API",
    description="EU AI Act Article 12 governance layer",
    version="0.1.0",
)


@app.get("/health")
def health():
    return {"status": "ok", "service": "constraintmesh-api", "version": "0.1.0"}


@app.get("/")
def root():
    return {
        "project": "ConstraintMesh",
        "eu_ai_act_articles": ["9", "10", "11", "12", "14", "26"],
        "status": "day-1-scaffold",
    }
