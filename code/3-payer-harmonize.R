# Meta --------------------------------------------------------------------

## Title:         Payer Harmonization
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Classifies payer names into standardized categories using
##                regex matching. Non-negotiated rate types get payer_category NA.

# Read cleaned rates --------------------------------------------------------

rates <- fread("data/output/rates-clean.csv") %>% as_tibble()
message("Input rates: ", nrow(rates), " rows")

# Payer classification ------------------------------------------------------
# Regex cascade for major payer categories. Order matters: more specific
# patterns first, catch-all "Other" last.

classify_payer <- function(payer_name) {
  payer_upper <- toupper(payer_name)

  case_when(
    is.na(payer_name) | payer_name == "" ~ NA_character_,
    str_detect(payer_upper, "BLUE\\s*CROSS|BLUE\\s*SHIELD|BCBS|ANTHEM|CAREFIRST|HIGHMARK|PREMERA|REGENCE|WELLMARK|HORIZON|INDEPENDENCE|EXCELLUS") ~ "BCBS",
    str_detect(payer_upper, "UNITED\\s*HEALTH|UHC|OPTUM|UHCSR|UNITED\\s*BEHAVIORAL") ~ "UHC",
    str_detect(payer_upper, "AETNA|CVS\\s*HEALTH") ~ "Aetna",
    str_detect(payer_upper, "CIGNA|EVERNORTH") ~ "Cigna",
    str_detect(payer_upper, "HUMANA") ~ "Humana",
    str_detect(payer_upper, "KAISER") ~ "Kaiser",
    str_detect(payer_upper, "CENTENE|WELLCARE|AMBETTER|HEALTH\\s*NET|FIDELIS|PEACH\\s*STATE|SUNSHINE\\s*HEALTH") ~ "Centene",
    str_detect(payer_upper, "MOLINA") ~ "Molina",
    str_detect(payer_upper, "MEDICARE\\s*ADV|MA-PD|MAPD") ~ "Medicare Advantage",
    str_detect(payer_upper, "MEDICAID|MEDI-CAL|CHIP") ~ "Medicaid MCO",
    str_detect(payer_upper, "MEDICARE") ~ "Medicare",
    str_detect(payer_upper, "TRICARE") ~ "Tricare",
    TRUE ~ "Other"
  )
}

# Apply to negotiated rates only; non-negotiated get NA ---------------------

rates <- rates %>%
  mutate(
    payer_category = if_else(
      rate_category == "negotiated",
      classify_payer(payer),
      NA_character_
    )
  )

# Diagnostics ---------------------------------------------------------------

message("\nPayer category distribution (negotiated rates only):")
rates %>%
  filter(rate_category == "negotiated") %>%
  count(payer_category, name = "n") %>%
  mutate(pct = round(100 * n / sum(n), 1)) %>%
  arrange(desc(n)) %>%
  mutate(msg = paste0("  ", payer_category, ": ", n, " (", pct, "%)")) %>%
  pull(msg) %>%
  walk(message)

classified <- rates %>%
  filter(rate_category == "negotiated", payer_category != "Other") %>%
  nrow()
total_neg <- rates %>%
  filter(rate_category == "negotiated") %>%
  nrow()

if (total_neg > 0) {
  message("\nPayer classification rate: ", classified, " / ", total_neg,
          " (", round(100 * classified / total_neg, 1), "%) classified to named payer")
}

# Top unmatched payer names
message("\nTop 20 unmatched payer names (classified as 'Other'):")
rates %>%
  filter(rate_category == "negotiated", payer_category == "Other") %>%
  count(payer, name = "n") %>%
  arrange(desc(n)) %>%
  slice_head(n = 20) %>%
  mutate(msg = paste0("  ", payer, ": ", n)) %>%
  pull(msg) %>%
  walk(message)

message("\nRate category breakdown:")
rates %>%
  count(rate_category, name = "n") %>%
  arrange(desc(n)) %>%
  mutate(msg = paste0("  ", rate_category, ": ", n)) %>%
  pull(msg) %>%
  walk(message)

# Write output --------------------------------------------------------------

write_csv(rates, "data/output/rates-payer.csv")
message("\nWrote data/output/rates-payer.csv: ", nrow(rates), " rows")
