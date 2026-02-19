# Meta --------------------------------------------------------------------

## Title:         HC Atlas Pricing Pipeline
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Orchestrator script. Sources all processing scripts in order.


# Preliminaries -----------------------------------------------------------

source("code/0-setup.R")
source("code/code-lists.R")


# Extract data from Oria DuckDB (skip if already extracted) ---------------

if (!file.exists("data/output/oria-hospital.csv") ||
    !file.exists("data/output/oria-rates-drg.csv") ||
    !file.exists("data/output/oria-rates-cpt.csv")) {
  source("code/0-oria-extract.R")
} else {
  message("Oria extracts already exist, skipping extraction.")
}


# Call individual code files ----------------------------------------------

source("code/1-hospitals.R")
source("code/2-rates-clean.R")
source("code/3-payer-harmonize.R")
source("code/4-panel.R")

message("\nBuild complete.")
