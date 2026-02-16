# Meta --------------------------------------------------------------------

## Title:         Hospital Processing
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Reads Dolthub hospital table, joins to hc-atlas crosswalk,
##                flags hospitals with MRF rate data, and outputs hospital panel.

# Read hospital table -------------------------------------------------------

hosp_raw <- fread("data/input/dolthub/hospital.csv")
message("Hospital table: ", nrow(hosp_raw), " rows, ", ncol(hosp_raw), " cols")

hosp <- hosp_raw %>%
  as_tibble() %>%
  rename(ccn = id) %>%
  select(ccn, name, address, city, state, zip_code,
         last_updated_on, mrf_url)

message("  Columns kept: ", paste(names(hosp), collapse = ", "))

# Join to hc-atlas crosswalk ------------------------------------------------

crosswalk <- fread("../hc-atlas/data/output/npi-ccn-crosswalk-enriched.csv") %>%
  as_tibble()

message("Crosswalk: ", nrow(crosswalk), " rows")

# Deduplicate to one row per CCN (crosswalk has multiple NPIs per CCN)
crosswalk_ccn <- crosswalk %>%
  select(ccn, aha_id, sysid) %>%
  filter(!is.na(ccn), ccn != "") %>%
  distinct(ccn, .keep_all = TRUE)

message("  Unique CCNs in crosswalk: ", nrow(crosswalk_ccn))

hosp <- hosp %>%
  left_join(crosswalk_ccn, by = "ccn")

matched <- sum(!is.na(hosp$aha_id))
message("  Crosswalk match rate: ", matched, " / ", nrow(hosp),
        " (", round(100 * matched / nrow(hosp), 1), "%)")

# Flag hospitals with MRF rate data -----------------------------------------

rates_drg <- fread("data/input/dolthub/rates-drg.csv", select = "hospital_id")
rates_cpt <- fread("data/input/dolthub/rates-cpt.csv", select = "hospital_id")

ccns_with_rates <- unique(c(rates_drg$hospital_id, rates_cpt$hospital_id))
message("  Unique CCNs with rate data: ", length(ccns_with_rates))

hosp <- hosp %>%
  mutate(has_mrf_data = ccn %in% ccns_with_rates)

message("  Hospitals with MRF data: ", sum(hosp$has_mrf_data), " / ", nrow(hosp))

# Write output --------------------------------------------------------------

write_csv(hosp, "data/output/hospitals.csv")
message("Wrote data/output/hospitals.csv: ", nrow(hosp), " rows")
