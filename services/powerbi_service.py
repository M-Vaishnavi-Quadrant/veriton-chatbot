# app/services/powerbi_service.py

import logging
import os
import requests
import numpy as np
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ========================= CONFIG =========================
# Base URL of your already deployed PowerBI API
POWERBI_API_BASE_URL = "https://api.veriton.ai/api/service2"  # e.g. https://your-deployed-api.com

# ===================== HELPERS =====================
def sanitize_for_json(obj):
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return 0.0
        return obj
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        val = float(obj)
        return 0.0 if np.isnan(val) or np.isinf(val) else val
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    return obj

# ===================== CALL DEPLOYED APIS =====================
def call_discover_kpis(csv_blob: str) -> Dict[str, Any]:
    """Calls your already deployed /discover_kpis API"""
    url = f"{POWERBI_API_BASE_URL}/discover_kpis"
    response = requests.post(url, json={"csv_blob": csv_blob}, timeout=120)
    response.raise_for_status()
    return response.json()


def call_compute_kpis(csv_blob: str, selected_kpi_names: List[str]) -> Dict[str, Any]:
    """Calls your already deployed /compute_kpis API"""
    url = f"{POWERBI_API_BASE_URL}/compute_kpis"
    response = requests.post(url, json={
        "csv_blob": csv_blob,
        "selected_kpi_names": selected_kpi_names
    }, timeout=120)
    response.raise_for_status()
    return response.json()


def call_generate_visuals(csv_blob: str, computed_kpis: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calls your already deployed /generate_visuals API"""
    url = f"{POWERBI_API_BASE_URL}/generate_visuals"
    response = requests.post(url, json={
        "csv_blob": csv_blob,
        "computed_kpis": computed_kpis
    }, timeout=120)
    response.raise_for_status()
    return response.json()


# ===================== SINGLE FUNCTION =====================
def generate_powerbi_dashboard(csv_blob: str, user_prompt: str) -> Dict[str, Any]:
    """
    Calls your 3 already deployed APIs in sequence.
    No logic rewritten — just orchestrates the existing endpoints.
    """

    # ── STEP 1: Call deployed /discover_kpis ─────────────────
    logging.info("Step 1: Calling deployed /discover_kpis...")
    kpi_result = call_discover_kpis(csv_blob)
    all_kpis = kpi_result.get("available_kpis", [])

    if not all_kpis:
        raise ValueError("No KPIs discovered from dataset")

    # ── STEP 2: Auto-select top 7 KPIs ───────────────────────
    logging.info("Step 2: Auto-selecting top 7 KPIs...")
    selected_kpi_names = [kpi["kpi_name"] for kpi in all_kpis[:7]]

    # ── STEP 3: Call deployed /compute_kpis ──────────────────
    logging.info("Step 3: Calling deployed /compute_kpis...")
    computed_result = call_compute_kpis(csv_blob, selected_kpi_names)

    # Normalize — single KPI returns dict, multiple returns {"selected_kpis": [...]}
    if "selected_kpis" in computed_result:
        computed_kpis = computed_result["selected_kpis"]
    else:
        computed_kpis = [computed_result]

    # ── STEP 4: Call deployed /generate_visuals ───────────────
    logging.info("Step 4: Calling deployed /generate_visuals...")
    visuals_result = call_generate_visuals(csv_blob, computed_kpis)

    # ── FINAL: Return complete dashboard ──────────────────────
    return {
        "status": "success",
        "user_prompt": user_prompt,
        "total_kpis_discovered": len(all_kpis),
        "selected_kpi_names": selected_kpi_names,
        "computed_kpis": computed_kpis,
        "visuals": visuals_result.get("visuals", []),
        "total_visuals": visuals_result.get("total_visuals_generated", 0),
        "output_blob_path": visuals_result.get("output_blob_path", ""),
        "storage_status": visuals_result.get("storage_status", "unknown")
    }