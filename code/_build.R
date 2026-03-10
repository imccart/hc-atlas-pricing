# Meta --------------------------------------------------------------------

## Title:         HC Atlas Pricing Pipeline
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Orchestrator script. Sources all processing scripts in order.

source("code/0-setup.R")
source("code/code-lists.R")

source("code/1-hcris.R")
source("code/2-rand.R")
source("code/3-oria.R")
source("code/4-panel.R")

message("\nBuild complete.")
