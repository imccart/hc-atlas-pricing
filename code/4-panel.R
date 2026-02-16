# Meta --------------------------------------------------------------------

## Title:         Price Panel Assembly
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Joins payer-harmonized rates to hospital attributes to produce
##                the final hospital x service x payer price panel.

# Read inputs ---------------------------------------------------------------

rates <- fread("data/output/rates-payer.csv") %>% as_tibble()
hospitals <- read_csv("data/output/hospitals.csv", show_col_types = FALSE)

message("Rates: ", nrow(rates), " rows")
message("Hospitals: ", nrow(hospitals), " rows")

# Join rates to hospitals ---------------------------------------------------

panel <- rates %>%
  inner_join(
    hospitals %>% select(ccn, name, state, aha_id, sysid),
    by = "ccn"
  )

message("After joining to hospitals: ", nrow(panel), " rows")
message("  Unique hospitals in panel: ", n_distinct(panel$ccn))

# Select final columns ------------------------------------------------------

panel <- panel %>%
  select(
    # Hospital identifiers
    ccn, name, state, aha_id, sysid,
    # Service
    code, code_type, label,
    # Rate
    rate_category, standard_charge,
    # Payer
    payer, plan, payer_category
  )

# Write output --------------------------------------------------------------

write_csv(panel, "data/output/price-panel.csv")
message("\nWrote data/output/price-panel.csv: ", nrow(panel), " rows")

# Summary statistics --------------------------------------------------------

message("\n--- Panel Summary ---")
message("Rows: ", nrow(panel))
message("Unique hospitals: ", n_distinct(panel$ccn))
message("Unique codes: ", n_distinct(panel$code))
message("States: ", n_distinct(panel$state))

message("\nRows by code x rate_category:")
panel %>%
  count(code_type, code, label, rate_category, name = "n") %>%
  arrange(code_type, code, rate_category) %>%
  mutate(msg = paste0("  ", code, " (", label, ") - ", rate_category, ": ", n)) %>%
  pull(msg) %>%
  walk(message)

message("\nPrice summary by rate_category:")
panel %>%
  group_by(rate_category) %>%
  summarize(
    n = n(),
    mean = round(mean(standard_charge), 2),
    median = round(median(standard_charge), 2),
    p10 = round(quantile(standard_charge, 0.10), 2),
    p90 = round(quantile(standard_charge, 0.90), 2),
    .groups = "drop"
  ) %>%
  mutate(msg = paste0("  ", rate_category, ": n=", n,
                       ", mean=$", mean, ", median=$", median,
                       ", p10=$", p10, ", p90=$", p90)) %>%
  pull(msg) %>%
  walk(message)
