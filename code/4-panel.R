# Meta --------------------------------------------------------------------

## Title:         Hospital Price Panel Assembly
## Author:        Ian McCarthy
## Date Created:  2026-03-09
## Description:   Assembles the final hospital-year panel by joining HCRIS
##                (spine), RAND prices, and Oria MRF summaries on CCN.

# 1. Read HCRIS spine --------------------------------------------------------

panel <- read_csv("data/output/hcris-panel.csv", show_col_types = FALSE) %>%
  mutate(ccn = as.character(ccn))
message("HCRIS spine: ", format(nrow(panel), big.mark = ","), " rows, ",
        n_distinct(panel$ccn), " hospitals")

# 2. Join RAND prices --------------------------------------------------------

rand <- read_csv("data/output/rand-hospitals.csv", show_col_types = FALSE) %>%
  mutate(ccn = as.character(ccn))

round_years <- tribble(
  ~round, ~year,
  2, 2015, 2, 2016, 2, 2017,
  3, 2016, 3, 2017, 3, 2018,
  4, 2018, 4, 2019, 4, 2020,
  5, 2020, 5, 2021, 5, 2022
)

rand_annual <- rand %>%
  select(ccn, round, rp_op, rp_ip, rp_combined) %>%
  inner_join(round_years, by = "round", relationship = "many-to-many") %>%
  group_by(ccn, year) %>%
  summarize(
    rand_rp_op = mean(rp_op, na.rm = TRUE),
    rand_rp_ip = mean(rp_ip, na.rm = TRUE),
    rand_rp_combined = mean(rp_combined, na.rm = TRUE),
    rand_n_rounds = n(),
    .groups = "drop"
  )

panel <- panel %>%
  left_join(rand_annual, by = c("ccn", "year"))

n_rand <- sum(!is.na(panel$rand_rp_combined))
message("RAND joined: ", format(n_rand, big.mark = ","), " rows with RAND data")

# 3. Join Oria summary -------------------------------------------------------

oria <- read_csv("data/output/oria-hospital-summary.csv", show_col_types = FALSE) %>%
  mutate(ccn = as.character(ccn))

# Oria is cross-sectional (no year dimension), so it attaches to all years
panel <- panel %>%
  left_join(oria, by = "ccn")

n_oria <- sum(!is.na(panel$oria_median_rate))
message("Oria joined: ", format(n_oria, big.mark = ","), " rows with Oria data (",
        n_distinct(panel$ccn[!is.na(panel$oria_median_rate)]), " hospitals)")

# 4. Summary and write -------------------------------------------------------

message("\nFinal panel: ", format(nrow(panel), big.mark = ","), " rows, ",
        n_distinct(panel$ccn), " hospitals, ",
        min(panel$year), "-", max(panel$year))
message("  With HCRIS price: ", format(sum(!is.na(panel$hcris_price)), big.mark = ","))
message("  With RAND price:  ", format(sum(!is.na(panel$rand_rp_combined)), big.mark = ","))
message("  With Oria rates:  ", format(sum(!is.na(panel$oria_median_rate)), big.mark = ","))
message("  With all three:   ", format(sum(!is.na(panel$hcris_price) &
                                            !is.na(panel$rand_rp_combined) &
                                            !is.na(panel$oria_median_rate)), big.mark = ","))

write_csv(panel, "data/output/hospital-price-panel.csv")
message("\nWrote data/output/hospital-price-panel.csv")
