# Meta --------------------------------------------------------------------

## Title:         Price Panel Assembly
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Joins payer-harmonized rates to hospital attributes to produce
##                the final hospital x service x payer price panel.

# Join rates to hospitals --------------------------------------------------

rates <- fread("data/output/rates-payer.csv") %>% as_tibble()
hospitals <- read_csv("data/output/hospitals.csv", show_col_types = FALSE)

panel <- rates %>%
  inner_join(
    hospitals %>% select(hospital_id, name, state, ccn, aha_id, sysid),
    by = "hospital_id"
  ) %>%
  select(
    hospital_id, ccn, name, state, aha_id, sysid,
    code, code_type, label,
    rate_category, standard_charge,
    payer, plan, payer_category
  )

# Write output -------------------------------------------------------------

write_csv(panel, "data/output/price-panel.csv")

message("Wrote data/output/price-panel.csv: ", format(nrow(panel), big.mark = ","), " rows, ",
        n_distinct(panel$hospital_id), " hospitals, ",
        n_distinct(panel$code), " codes, ",
        n_distinct(panel$state), " states")
