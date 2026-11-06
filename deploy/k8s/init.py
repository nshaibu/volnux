"""
init.py — Volnux Project Bootstrap
====================================

This file is the entry point for the Volnux engine. It is loaded by:
  - `volnux dev` (development server)
  - `volnux serve` (production API and worker)
  - `volnux workflow run` (manual workflow execution)
  - `volnux validate` (workflow validation)
  - `volnux migrate` (database migrations)

It initialises the engine by:
  1. Loading the project configuration (config.py or settings.py)
  2. Discovering workflow packages in WORKFLOWS_DIR
  3. Registering persistence backends
  4. Returning the initialised WorkflowEngine

The engine object is used by the CLI and the REST API server.
You do not normally need to modify this file.
"""

from pathlib import Path

from volnux.setup import initialise_workflows

# Absolute path to this project directory.
# All relative paths in config.py are resolved from here.
project_dir = Path(__file__).parent

# Initialise and return the Volnux engine.
# This call:
#   - Loads config.py (or settings.py if mounted via K8s ConfigMap)
#   - Discovers all WorkflowConfig subclasses in workflows/
#   - Registers persistence backends (PostgreSQL, Redis, SQLite)
#   - Connects to EventHub and resolves WorkflowSource packages
#   - Compiles all Pointy-Lang .pty files
#   - Registers all triggers via each workflow's ready() method
#   - Returns the initialised engine ready for serving
engine = initialise_workflows(project_dir)
