# Meta --------------------------------------------------------------------

## Title:         Hospital Processing
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Reads Oria hospital table, joins to hc-atlas crosswalk via EIN,
##                flags hospitals with MRF rate data, and outputs hospital panel.

# Read hospital table ------------------------------------------------------

hosp <- fread("data/output/oria-hospital.csv") %>%
  as_tibble() %>%
  rename(name = hospital_name, addr = hospital_address,
         city = hospital_city, state = hospital_state) %>%
  mutate(ein = as.character(ein)) %>%
  select(hospital_id, name, addr, city, state, ein)

# Join to hc-atlas crosswalk via EIN --------------------------------------

crosswalk_ein <- fread("../hc-atlas/data/output/npi-ccn-crosswalk-enriched.csv") %>%
  as_tibble() %>%
  filter(!is.na(ein), ein != "") %>%
  mutate(ein = str_pad(ein, width = 9, side = "left", pad = "0")) %>%
  select(ein, ccn, aha_id, sysid) %>%
  distinct(ein, .keep_all = TRUE)

hosp <- hosp %>% left_join(crosswalk_ein, by = "ein")

matched <- sum(!is.na(hosp$aha_id))
message("Crosswalk match: ", matched, " / ", nrow(hosp),
        " (", round(100 * matched / nrow(hosp), 1), "%)")

# Flag hospitals with MRF rate data ----------------------------------------

rates_drg <- fread("data/output/oria-rates-drg.csv", select = "hospital_id")
rates_cpt <- fread("data/output/oria-rates-cpt.csv", select = "hospital_id")
ids_with_rates <- unique(c(rates_drg$hospital_id, rates_cpt$hospital_id))

hosp <- hosp %>%
  mutate(has_mrf_data = hospital_id %in% ids_with_rates)

message("Hospitals with MRF data: ", sum(hosp$has_mrf_data), " / ", nrow(hosp))

# Write output -------------------------------------------------------------

write_csv(hosp, "data/output/hospitals.csv")
message("Wrote data/output/hospitals.csv: ", nrow(hosp), " rows")
