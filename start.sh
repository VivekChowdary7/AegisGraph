#!/usr/bin/env bash
# AegisGraph — One-command startup script
set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  AegisGraph — FalkorDB Triple-Crown Multi-Agent Engine"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Resolve python binary
if command -v python3 &>/dev/null; then
  PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
  PYTHON_CMD="python"
else
  echo "❌ Error: Python 3 is required but not found in PATH."
  exit 1
fi

# Step 1: Start FalkorDB
echo ""
echo "▶ Step 1/4  Starting FalkorDB via Docker..."
docker compose up -d
echo "  Waiting for FalkorDB to be healthy..."
sleep 4

# Step 2: Install Python deps
echo ""
echo "▶ Step 2/4  Installing Python dependencies..."
$PYTHON_CMD -m pip install -r requirements.txt -q

# Step 3: Seed graphs
echo ""
echo "▶ Step 3/4  Seeding enterprise brain + agent memory graphs..."
$PYTHON_CMD seed_graph.py

# Step 4: Launch the server
echo ""
echo "▶ Step 4/4  Launching FastAPI server..."
echo ""
echo "  🌐  Mission Control UI   →  http://localhost:8000"
echo "  📊  FalkorDB Browser     →  http://localhost:3000"
echo "  📖  API Docs (Swagger)   →  http://localhost:8000/docs"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
$PYTHON_CMD -m uvicorn main:app --reload --port 8000 --host 0.0.0.0
