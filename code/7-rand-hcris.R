# Meta --------------------------------------------------------------------

## Title:         RAND-HCRIS Join
## Author:        Ian McCarthy
## Date Created:  2026-03-08
## Description:   Joins RAND hospital-level commercial price ratios to HCRIS
##                cost report variables. Each RAND round is matched to every
##                HCRIS year in its coverage period (same RAND price, annual HCRIS).

# Round-to-year mapping
round_years <- tribble(
  ~round, ~year,
  2, 2015, 2, 2016, 2, 2017,
  3, 2016, 3, 2017, 3, 2018,
  4, 2018, 4, 2019, 4, 2020,
  5, 2020, 5, 2021, 5, 2022
)

# 1. Read RAND prices -------------------------------------------------------

rand <- read_csv("data/output/rand-hospitals.csv", show_col_types = FALSE)
message("RAND: ", format(nrow(rand), big.mark = ","), " hospital-rounds, ",
        n_distinct(rand$ccn), " unique CCNs")

# 2. Read HCRIS and select predictor variables -------------------------------

hcris_raw <- fread("data/input/hcris/HCRIS_Data.txt") %>%
  as_tibble() %>%
  rename(ccn = provider_number) %>%
  filter(year >= 2015, year <= 2022)

message("HCRIS: ", format(nrow(hcris_raw), big.mark = ","), " hospital-years (",
        n_distinct(hcris_raw$ccn), " CCNs, ", min(hcris_raw$year), "-", max(hcris_raw$year), ")")

# Construct predictor variables
hcris <- hcris_raw %>%
  mutate(
    ccn = as.character(ccn),
    mcare_share = mcare_discharges / tot_discharges,
    mcaid_share = mcaid_discharges / tot_discharges,
    op_share = pps_op_charges / (pps_ip_charges + pps_op_charges),
    operating_margin = (net_pat_rev - tot_operating_exp) / net_pat_rev,
    charge_per_discharge = tot_charges / tot_discharges,
    uncomp_share = uncomp_care / net_pat_rev,
    ip_charge_per_discharge = ip_charges / tot_discharges
  ) %>%
  select(ccn, year, beds, tot_discharges, cost_to_charge,
         mcare_share, mcaid_share, op_share, operating_margin,
         charge_per_discharge, uncomp_share, tot_charges, net_pat_rev,
         tot_operating_exp, ip_charge_per_discharge)

# 3. Expand RAND to year level and join to HCRIS ----------------------------

rand_expanded <- rand %>%
  mutate(ccn = as.character(ccn)) %>%
  inner_join(round_years, by = "round", relationship = "many-to-many")

message("RAND expanded: ", format(nrow(rand_expanded), big.mark = ","), " CCN-round-years")

# 4. Join RAND to HCRIS -----------------------------------------------------

rand_hcris <- rand_expanded %>%
  inner_join(hcris, by = c("ccn", "year"))

matched <- n_distinct(rand_hcris$ccn)
total <- n_distinct(rand$ccn)
message("Joined: ", format(nrow(rand_hcris), big.mark = ","), " rows, ",
        matched, " / ", total, " RAND CCNs matched (",
        round(100 * matched / total, 1), "%)")

# 5. Diagnostics ------------------------------------------------------------

# Quick check: relative price vs key predictors
message("\nRelative price (combined) summary:")
message("  Mean: ", round(mean(rand_hcris$rp_combined, na.rm = TRUE), 2),
        "  Median: ", round(median(rand_hcris$rp_combined, na.rm = TRUE), 2),
        "  SD: ", round(sd(rand_hcris$rp_combined, na.rm = TRUE), 2))

message("  Correlation with cost_to_charge: ",
        round(cor(rand_hcris$rp_combined, rand_hcris$cost_to_charge, use = "complete.obs"), 3))
message("  Correlation with beds: ",
        round(cor(rand_hcris$rp_combined, rand_hcris$beds, use = "complete.obs"), 3))
message("  Correlation with mcare_share: ",
        round(cor(rand_hcris$rp_combined, rand_hcris$mcare_share, use = "complete.obs"), 3))

write_csv(rand_hcris, "data/output/rand-hcris.csv")
message("\nWrote data/output/rand-hcris.csv")
