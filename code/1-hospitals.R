# Meta --------------------------------------------------------------------

## Title:         Hospital Processing
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Reads Oria hospital table, joins to hc-atlas crosswalk via EIN,
##                flags hospitals with MRF rate data, and outputs hospital panel.

# Read hospital table -------------------------------------------------------

hosp_raw <- fread("data/output/oria-hospital.csv")
message("Hospital table: ", nrow(hosp_raw), " rows, ", ncol(hosp_raw), " cols")
message("  Columns: ", paste(names(hosp_raw), collapse = ", "))

hosp <- hosp_raw %>%
  as_tibble() %>%
  rename(
    name = hospital_name,
    addr = hospital_address,
    city = hospital_city,
    state = hospital_state
  ) %>%
  mutate(ein = as.character(ein)) %>%
  select(hospital_id, name, addr, city, state, ein)

message("  Columns kept: ", paste(names(hosp), collapse = ", "))

# Join to hc-atlas crosswalk via EIN --------------------------------------

crosswalk <- fread("../hc-atlas/data/output/npi-ccn-crosswalk-enriched.csv") %>%
  as_tibble()

message("Crosswalk: ", nrow(crosswalk), " rows")

# Crosswalk EINs may be 8 or 9 digits; pad to 9 for matching
crosswalk_ein <- crosswalk %>%
  filter(!is.na(ein), ein != "") %>%
  mutate(ein = str_pad(ein, width = 9, side = "left", pad = "0")) %>%
  select(ein, ccn, aha_id, sysid) %>%
  distinct(ein, .keep_all = TRUE)

message("  Unique EINs in crosswalk: ", nrow(crosswalk_ein))

hosp <- hosp %>%
  left_join(crosswalk_ein, by = "ein")

matched <- sum(!is.na(hosp$aha_id))
message("  Crosswalk match rate (by EIN): ", matched, " / ", nrow(hosp),
        " (", round(100 * matched / nrow(hosp), 1), "%)")

# Flag hospitals with MRF rate data -----------------------------------------

rates_drg <- fread("data/output/oria-rates-drg.csv", select = "hospital_id")
rates_cpt <- fread("data/output/oria-rates-cpt.csv", select = "hospital_id")

ids_with_rates <- unique(c(rates_drg$hospital_id, rates_cpt$hospital_id))
message("  Unique hospital IDs with rate data: ", length(ids_with_rates))

hosp <- hosp %>%
  mutate(has_mrf_data = hospital_id %in% ids_with_rates)

message("  Hospitals with MRF data: ", sum(hosp$has_mrf_data), " / ", nrow(hosp))

# Write output --------------------------------------------------------------

write_csv(hosp, "data/output/hospitals.csv")
message("Wrote data/output/hospitals.csv: ", nrow(hosp), " rows")
