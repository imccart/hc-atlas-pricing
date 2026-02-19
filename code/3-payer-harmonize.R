# Meta --------------------------------------------------------------------

## Title:         Payer Harmonization
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Classifies payer names into standardized categories using
##                regex matching. Non-negotiated rate types get payer_category NA.

# Read cleaned rates -------------------------------------------------------

rates <- fread("data/output/rates-clean.csv") %>% as_tibble()

# Payer classification -----------------------------------------------------

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

rates <- rates %>%
  mutate(payer_category = if_else(
    rate_category == "negotiated",
    classify_payer(payer),
    NA_character_
  ))

# Diagnostics --------------------------------------------------------------

neg <- rates %>% filter(rate_category == "negotiated")
classified <- sum(neg$payer_category != "Other")
message("Payer classification: ", classified, " / ", nrow(neg),
        " (", round(100 * classified / nrow(neg), 1), "%) matched")

# Write output -------------------------------------------------------------

write_csv(rates, "data/output/rates-payer.csv")
message("Wrote data/output/rates-payer.csv")
