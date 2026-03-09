# Meta --------------------------------------------------------------------

## Title:         Rate Cleaning
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Combines DRG/CPT rate extracts, standardizes codes,
##                joins to target code list, and drops invalid charges.

# Read and combine rate extracts -------------------------------------------

rates <- bind_rows(
  fread("data/output/oria-rates-drg.csv") %>% as_tibble(),
  fread("data/output/oria-rates-cpt.csv") %>% as_tibble()
) %>%
  rename(payer = payer_name, plan = plan_name) %>%
  mutate(standard_charge = as.numeric(standard_charge))

message("Combined rates: ", format(nrow(rates), big.mark = ","), " rows")

# Create unified code + code_type columns ---------------------------------

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

# Keep only target codes ---------------------------------------------------

rates <- rates %>%
  inner_join(target_codes %>% select(code, code_type, label),
             by = c("code", "code_type"))

# Drop invalid charges -----------------------------------------------------

n_before <- nrow(rates)
rates <- rates %>%
  filter(!is.na(standard_charge), is.finite(standard_charge), standard_charge > 0)

message("Dropped ", n_before - nrow(rates), " invalid charges, ",
        format(nrow(rates), big.mark = ","), " remaining")

# Write output -------------------------------------------------------------

write_csv(rates, "data/output/rates-clean.csv")
message("Wrote data/output/rates-clean.csv")
