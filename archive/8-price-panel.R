# Meta --------------------------------------------------------------------

## Title:         Hospital Price Panel
## Author:        Ian McCarthy
## Date Created:  2026-03-09
## Description:   Assembles a hospital-year panel (2010+) with HCRIS-based
##                price (Dafny formula), RAND commercial-to-Medicare ratios
##                (where observed), HRR market structure, and key hospital
##                characteristics. All price measures kept as separate columns.

# 1. Read full HCRIS panel (2010+, all provider types) -----------------------

hcris <- fread("data/input/hcris/HCRIS_Data.txt") %>%
  as_tibble() %>%
  rename(ccn = provider_number) %>%
  filter(year >= 2010) %>%
  mutate(
    ccn = as.character(ccn),
    zip5 = str_sub(as.character(zip), 1, 5),
    ccn_prefix = as.integer(str_sub(ccn, 3, 4)),
    acute = (ccn_prefix >= 0 & ccn_prefix <= 8)
  ) %>%
  select(-ccn_prefix)

message("Full HCRIS (2010+): ", format(nrow(hcris), big.mark = ","), " rows, ",
        n_distinct(hcris$ccn), " hospitals")
message("  Acute: ", format(sum(hcris$acute), big.mark = ","),
        "  Non-acute: ", format(sum(!hcris$acute), big.mark = ","))

# 2. HCRIS-based price (Dafny formula, acute only) ---------------------------

hcris <- hcris %>%
  mutate(
    discount_factor = ifelse(acute & tot_charges > 0,
                             1 - abs(tot_discounts) / tot_charges, NA_real_),
    hcris_price_num = ifelse(acute,
                             (ip_charges + icu_charges + ancillary_charges) * discount_factor - tot_mcare_payment,
                             NA_real_),
    hcris_price_denom = ifelse(acute, tot_discharges - mcare_discharges, NA_real_),
    hcris_price = ifelse(acute & hcris_price_denom > 100 & hcris_price_num > 0,
                         hcris_price_num / hcris_price_denom, NA_real_)
  ) %>%
  # Cap outliers
  mutate(hcris_price = ifelse(!is.na(hcris_price) & hcris_price > 100000, NA_real_, hcris_price)) %>%
  select(-discount_factor, -hcris_price_num, -hcris_price_denom)

n_price <- sum(!is.na(hcris$hcris_price))
message("\nHCRIS price computed: ", format(n_price, big.mark = ","), " / ",
        format(sum(hcris$acute), big.mark = ","), " acute rows")

# 3. HRR crosswalk and market structure --------------------------------------

zip_hrr <- read_xls("D:/research-data/geography/ZipHsaHrr15.xls") %>%
  mutate(zip5 = str_pad(as.character(as.integer(zipcode15)), 5, pad = "0")) %>%
  select(zip5, hrrnum, hrrcity, hrrstate) %>%
  distinct(zip5, .keep_all = TRUE)

hcris <- hcris %>%
  left_join(zip_hrr, by = "zip5")

matched_hrr <- sum(!is.na(hcris$hrrnum))
message("HRR match: ", format(matched_hrr, big.mark = ","), " / ",
        format(nrow(hcris), big.mark = ","), " (",
        round(100 * matched_hrr / nrow(hcris), 1), "%)")

# Count hospitals per HRR-year (all types)
hrr_counts <- hcris %>%
  filter(!is.na(hrrnum)) %>%
  group_by(hrrnum, year) %>%
  summarize(n_hosp_hrr = n(), .groups = "drop")

# Count acute hospitals per HRR-year
hrr_counts_acute <- hcris %>%
  filter(!is.na(hrrnum), acute) %>%
  group_by(hrrnum, year) %>%
  summarize(n_acute_hrr = n(), .groups = "drop")

hcris <- hcris %>%
  left_join(hrr_counts, by = c("hrrnum", "year")) %>%
  left_join(hrr_counts_acute, by = c("hrrnum", "year"))

# 4. RAND prices -------------------------------------------------------------

rand <- read_csv("data/output/rand-hospitals.csv", show_col_types = FALSE) %>%
  mutate(ccn = as.character(ccn))

round_years <- tribble(
  ~round, ~year,
  2, 2015, 2, 2016, 2, 2017,
  3, 2016, 3, 2017, 3, 2018,
  4, 2018, 4, 2019, 4, 2020,
  5, 2020, 5, 2021, 5, 2022
)

rand_expanded <- rand %>%
  select(ccn, round, rp_op, rp_ip, rp_combined) %>%
  inner_join(round_years, by = "round", relationship = "many-to-many")

# Average across rounds when years overlap (e.g., 2018 in rounds 3+4)
rand_annual <- rand_expanded %>%
  group_by(ccn, year) %>%
  summarize(
    rand_rp_op = mean(rp_op, na.rm = TRUE),
    rand_rp_ip = mean(rp_ip, na.rm = TRUE),
    rand_rp_combined = mean(rp_combined, na.rm = TRUE),
    rand_n_rounds = n(),
    .groups = "drop"
  )

message("\nRAND annual: ", format(nrow(rand_annual), big.mark = ","), " CCN-years, ",
        n_distinct(rand_annual$ccn), " hospitals")

hcris <- hcris %>%
  left_join(rand_annual, by = c("ccn", "year"))

n_rand <- sum(!is.na(hcris$rand_rp_combined))
message("RAND matched to HCRIS: ", format(n_rand, big.mark = ","), " rows")

# 5. Hospital characteristics ------------------------------------------------

panel <- hcris %>%
  mutate(
    mcaid_share = ifelse(tot_discharges > 0, mcaid_discharges / tot_discharges, NA_real_),
    mcare_share = ifelse(tot_discharges > 0, mcare_discharges / tot_discharges, NA_real_),
    operating_margin = ifelse(net_pat_rev > 0,
                              (net_pat_rev - tot_operating_exp) / net_pat_rev, NA_real_)
  ) %>%
  select(
    ccn, year, name, state, zip5, acute,
    # HCRIS price
    hcris_price,
    # RAND prices
    rand_rp_op, rand_rp_ip, rand_rp_combined, rand_n_rounds,
    # Hospital characteristics
    beds, tot_discharges, mcare_discharges, mcaid_discharges,
    mcare_share, mcaid_share,
    tot_charges, net_pat_rev, tot_operating_exp, operating_margin,
    cost_to_charge,
    # Market structure
    hrrnum, hrrcity, hrrstate, n_hosp_hrr, n_acute_hrr
  )

message("\nFinal panel: ", format(nrow(panel), big.mark = ","), " rows, ",
        n_distinct(panel$ccn), " hospitals, ",
        min(panel$year), "-", max(panel$year))
message("  With HCRIS price: ", format(sum(!is.na(panel$hcris_price)), big.mark = ","))
message("  With RAND price:  ", format(sum(!is.na(panel$rand_rp_combined)), big.mark = ","))
message("  With both:        ", format(sum(!is.na(panel$hcris_price) & !is.na(panel$rand_rp_combined)), big.mark = ","))

write_csv(panel, "data/output/hospital-price-panel.csv")
message("\nWrote data/output/hospital-price-panel.csv")
