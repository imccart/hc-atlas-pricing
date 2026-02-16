# Meta --------------------------------------------------------------------

## Title:         HC Atlas Pricing Setup
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Loads packages at fixed versions via groundhog for
##                reproducibility. Source this file at the top of every script.

# Groundhog date ---------------------------------------------------------
# All packages are pinned to the CRAN snapshot on this date.

options(repos = c(CRAN = "https://cran.r-project.org/"))

if (!require("groundhog", quietly = TRUE)) install.packages("groundhog")
library(groundhog)

ghog_date <- "2025-04-01"

# Packages ----------------------------------------------------------------

pkgs <- c("tidyverse", "data.table")
suppressWarnings(groundhog.library(pkgs, ghog_date))
