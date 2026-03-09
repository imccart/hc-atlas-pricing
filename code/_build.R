# Meta --------------------------------------------------------------------

## Title:         HC Atlas Pricing Pipeline
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Orchestrator script. Sources all processing scripts in order.

source("code/0-setup.R")
source("code/code-lists.R")

source("code/1-oria-extract.R")
source("code/2-rand-extract.R")
source("code/3-hospitals.R")
source("code/4-rates-clean.R")
source("code/5-payer-harmonize.R")
source("code/6-panel.R")

message("\nBuild complete.")
