# Meta --------------------------------------------------------------------

## Title:         HCRIS Extract
## Author:        Ian McCarthy
## Date Created:  2026-03-09
## Description:   Reads HCRIS cost reports (2010+), constructs Dafny price for
##                acute hospitals, adds HRR market structure. Outputs the full
##                hospital-year panel (all provider types) as the spine for
##                downstream joins.

# 1. Read full HCRIS panel ---------------------------------------------------

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

# 2. Dafny price (acute only) ------------------------------------------------

hcris <- hcris %>%
  mutate(
    discount_factor = ifelse(acute & tot_charges > 0,
                             1 - abs(tot_discounts) / tot_charges, NA_real_),
    hcris_price_num = ifelse(acute,
                             (ip_charges + icu_charges + ancillary_charges) * discount_factor - tot_mcare_payment,
                             NA_real_),
    hcris_price_denom = ifelse(acute, tot_discharges - mcare_discharges, NA_real_),
    hcris_price = ifelse(acute & hcris_price_denom > 100 & hcris_price_num > 0,
                         hcris_price_num / hcris_price_denom, NA_real_),
    hcris_price = ifelse(!is.na(hcris_price) & hcris_price > 100000, NA_real_, hcris_price)
  ) %>%
  select(-discount_factor, -hcris_price_num, -hcris_price_denom)

n_price <- sum(!is.na(hcris$hcris_price))
message("HCRIS price computed: ", format(n_price, big.mark = ","), " / ",
        format(sum(hcris$acute), big.mark = ","), " acute rows")

# 3. HRR crosswalk -----------------------------------------------------------

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

# 4. Market structure (hospital counts per HRR-year) -------------------------

hrr_counts <- hcris %>%
  filter(!is.na(hrrnum)) %>%
  group_by(hrrnum, year) %>%
  summarize(n_hosp_hrr = n(), .groups = "drop")

hrr_counts_acute <- hcris %>%
  filter(!is.na(hrrnum), acute) %>%
  group_by(hrrnum, year) %>%
  summarize(n_acute_hrr = n(), .groups = "drop")

hcris <- hcris %>%
  left_join(hrr_counts, by = c("hrrnum", "year")) %>%
  left_join(hrr_counts_acute, by = c("hrrnum", "year"))

# 5. Hospital characteristics ------------------------------------------------

hcris <- hcris %>%
  mutate(
    mcare_share = ifelse(tot_discharges > 0, mcare_discharges / tot_discharges, NA_real_),
    mcaid_share = ifelse(tot_discharges > 0, mcaid_discharges / tot_discharges, NA_real_),
    operating_margin = ifelse(net_pat_rev > 0,
                              (net_pat_rev - tot_operating_exp) / net_pat_rev, NA_real_)
  )

# 6. Select and write --------------------------------------------------------

hcris_out <- hcris %>%
  select(
    ccn, year, name, city, state, zip5, acute,
    hcris_price,
    beds, tot_discharges, mcare_discharges, mcaid_discharges,
    mcare_share, mcaid_share,
    tot_charges, net_pat_rev, tot_operating_exp, operating_margin,
    cost_to_charge,
    hrrnum, hrrcity, hrrstate, n_hosp_hrr, n_acute_hrr
  )

write_csv(hcris_out, "data/output/hcris-panel.csv")
message("\nWrote data/output/hcris-panel.csv: ", format(nrow(hcris_out), big.mark = ","),
        " rows, ", n_distinct(hcris_out$ccn), " hospitals, ",
        min(hcris_out$year), "-", max(hcris_out$year))
