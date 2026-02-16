# Meta --------------------------------------------------------------------

## Title:         Rate Cleaning
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Reads and combines DRG/CPT rate extracts, standardizes codes,
##                joins to target code list, and drops invalid charges.

# Read rate extracts --------------------------------------------------------

rates_drg <- fread("data/input/dolthub/rates-drg.csv") %>% as_tibble()
rates_cpt <- fread("data/input/dolthub/rates-cpt.csv") %>% as_tibble()

message("DRG rates: ", nrow(rates_drg), " rows")
message("CPT rates: ", nrow(rates_cpt), " rows")

# Combine and standardize ---------------------------------------------------

rates <- bind_rows(rates_drg, rates_cpt) %>%
  rename(ccn = hospital_id)

message("Combined rates: ", nrow(rates), " rows")

# Create unified code + code_type columns
rates <- rates %>%
  mutate(
    code = case_when(
      !is.na(ms_drg) & ms_drg != "" ~ as.character(ms_drg),
      !is.na(hcpcs_cpt) & hcpcs_cpt != "" ~ as.character(hcpcs_cpt),
      TRUE ~ NA_character_
    ),
    code_type = case_when(
      !is.na(ms_drg) & ms_drg != "" ~ "MS-DRG",
      !is.na(hcpcs_cpt) & hcpcs_cpt != "" ~ "CPT/HCPCS",
      TRUE ~ NA_character_
    )
  )

# Join to target codes for labels, keep only our codes ----------------------

rates <- rates %>%
  inner_join(target_codes %>% select(code, code_type, label),
             by = c("code", "code_type"))

message("After filtering to target codes: ", nrow(rates), " rows")

# Validate charges ----------------------------------------------------------

n_before <- nrow(rates)

rates <- rates %>%
  filter(!is.na(standard_charge),
         is.finite(standard_charge),
         standard_charge > 0)

n_dropped <- n_before - nrow(rates)
message("Dropped ", n_dropped, " rows with NA/zero/negative/infinite charges (",
        round(100 * n_dropped / n_before, 1), "%)")

# Diagnostics ---------------------------------------------------------------

message("\nRow counts by code:")
rates %>%
  count(code_type, code, label, name = "n") %>%
  arrange(code_type, code) %>%
  mutate(msg = paste0("  ", code_type, " ", code, " (", label, "): ", n)) %>%
  pull(msg) %>%
  walk(message)

zero_codes <- target_codes %>%
  anti_join(rates %>% distinct(code, code_type), by = c("code", "code_type"))

if (nrow(zero_codes) > 0) {
  message("\nWARNING: ", nrow(zero_codes), " target codes have zero rows:")
  walk(zero_codes$code, ~ message("  ", .x))
}

message("\nRow counts by rate_category:")
rates %>%
  count(rate_category, name = "n") %>%
  arrange(desc(n)) %>%
  mutate(msg = paste0("  ", rate_category, ": ", n)) %>%
  pull(msg) %>%
  walk(message)

# Write output --------------------------------------------------------------

write_csv(rates, "data/output/rates-clean.csv")
message("\nWrote data/output/rates-clean.csv: ", nrow(rates), " rows")
