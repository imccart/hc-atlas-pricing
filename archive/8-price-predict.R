# Meta --------------------------------------------------------------------

## Title:         Commercial Price Prediction
## Author:        Ian McCarthy
## Date Created:  2026-03-09
## Description:   Constructs HCRIS-based price (Dafny formula) for short-term
##                acute hospitals, residualizes via state-year FEs to purge
##                Medicaid variation, adds market structure from HRR hospital
##                counts, then learns an adjustment factor from RAND overlap
##                to predict commercial price ratios. Output includes the full
##                HCRIS panel (2010+, all provider types) with rp_hat populated
##                only for acute hospitals that pass the Dafny filters.

# 1. Read full HCRIS panel (2010+, all provider types) -----------------------

hcris_all <- fread("data/input/hcris/HCRIS_Data.txt") %>%
  as_tibble() %>%
  rename(ccn = provider_number) %>%
  filter(year >= 2010) %>%
  mutate(
    ccn = as.character(ccn),
    zip5 = str_sub(as.character(zip), 1, 5),
    ccn_prefix = as.integer(str_sub(ccn, 3, 4)),
    acute = (ccn_prefix >= 0 & ccn_prefix <= 8)
  )

message("Full HCRIS (2010+): ", format(nrow(hcris_all), big.mark = ","), " rows")
message("  Acute (CCN 00-08): ", format(sum(hcris_all$acute), big.mark = ","),
        "  Non-acute: ", format(sum(!hcris_all$acute), big.mark = ","))

# 2. Construct Dafny price for acute hospitals only --------------------------

hcris_acute <- hcris_all %>%
  filter(acute) %>%
  mutate(
    discount_factor = 1 - abs(tot_discounts) / tot_charges,
    price_num = (ip_charges + icu_charges + ancillary_charges) * discount_factor - tot_mcare_payment,
    price_denom = tot_discharges - mcare_discharges,
    price = price_num / price_denom
  ) %>%
  filter(
    price_denom > 100,
    !is.na(price_denom),
    price_num > 0,
    !is.na(price_num),
    price < 100000,
    beds > 30,
    !is.na(beds),
    !is.na(state),
    state != ""
  )

message("Acute after Dafny filters: ", format(nrow(hcris_acute), big.mark = ","), " rows, ",
        n_distinct(hcris_acute$ccn), " hospitals")

# 3. HRR crosswalk and market structure --------------------------------------

zip_hrr <- read_xls("D:/research-data/geography/ZipHsaHrr15.xls") %>%
  mutate(zip5 = str_pad(as.character(as.integer(zipcode15)), 5, pad = "0")) %>%
  select(zip5, hrrnum, hrrcity, hrrstate) %>%
  distinct(zip5, .keep_all = TRUE)

message("ZIP-HRR crosswalk: ", format(nrow(zip_hrr), big.mark = ","), " zips, ",
        n_distinct(zip_hrr$hrrnum), " HRRs")

hcris_acute <- hcris_acute %>%
  left_join(zip_hrr, by = "zip5")

matched_hrr <- sum(!is.na(hcris_acute$hrrnum))
message("HRR match: ", format(matched_hrr, big.mark = ","), " / ",
        format(nrow(hcris_acute), big.mark = ","), " (",
        round(100 * matched_hrr / nrow(hcris_acute), 1), "%)")

# Count acute hospitals per HRR-year
hrr_counts <- hcris_acute %>%
  filter(!is.na(hrrnum)) %>%
  group_by(hrrnum, year) %>%
  summarize(n_hosp_hrr = n(), .groups = "drop")

hcris_acute <- hcris_acute %>%
  left_join(hrr_counts, by = c("hrrnum", "year")) %>%
  mutate(
    mkt_monopoly  = as.integer(!is.na(n_hosp_hrr) & n_hosp_hrr == 1),
    mkt_duopoly   = as.integer(!is.na(n_hosp_hrr) & n_hosp_hrr == 2),
    mkt_triopoly  = as.integer(!is.na(n_hosp_hrr) & n_hosp_hrr == 3),
    mkt_small     = as.integer(!is.na(n_hosp_hrr) & n_hosp_hrr >= 4 & n_hosp_hrr <= 10),
    mkt_medium    = as.integer(!is.na(n_hosp_hrr) & n_hosp_hrr >= 11 & n_hosp_hrr <= 25),
    mkt_large     = as.integer(!is.na(n_hosp_hrr) & n_hosp_hrr > 25)
  )

message("Market structure (2015 snapshot):")
snap <- hcris_acute %>% filter(year == 2015)
message("  Monopoly: ", sum(snap$mkt_monopoly), "  Duopoly: ", sum(snap$mkt_duopoly),
        "  Triopoly: ", sum(snap$mkt_triopoly))
message("  Small (4-10): ", sum(snap$mkt_small), "  Medium (11-25): ", sum(snap$mkt_medium),
        "  Large (>25): ", sum(snap$mkt_large))

# 4. State-year FE residualization -------------------------------------------

hcris_acute <- hcris_acute %>%
  mutate(log_price = log(price),
         state_year = paste0(state, "_", year))

fe_model <- lm(log_price ~ state_year, data = hcris_acute)
hcris_acute$log_price_resid <- residuals(fe_model)

message("\nState-year FE model: ", length(coef(fe_model)) - 1, " FEs, ",
        "R-sq = ", round(summary(fe_model)$r.squared, 3))

# 5. Build predictors --------------------------------------------------------

hcris_acute <- hcris_acute %>%
  mutate(
    mcaid_share = ifelse(tot_discharges > 0, mcaid_discharges / tot_discharges, NA_real_),
    operating_margin = ifelse(net_pat_rev > 0,
                              (net_pat_rev - tot_operating_exp) / net_pat_rev, NA_real_),
    log_beds = log(beds)
  )

# Only predictors NOT mechanically in the Dafny price formula.
# Dropped: log_charges, log_discharges, mcare_share (in numerator/denominator),
#          cost_to_charge, uncomp_share, bad_debt_share (high NA + in price).
pred_vars <- c("log_price_resid", "mcaid_share", "operating_margin", "log_beds")

na_rates <- sapply(pred_vars, function(v) mean(is.na(hcris_acute[[v]])))
message("\nNA rates in predictors (acute only):")
for (v in pred_vars) {
  message("  ", v, ": ", round(100 * na_rates[v], 1), "%")
}

# 6. RAND overlap and training -----------------------------------------------

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
  inner_join(round_years, by = "round", relationship = "many-to-many")

overlap <- rand_expanded %>%
  select(ccn, round, year, rp_combined) %>%
  inner_join(
    hcris_acute %>%
      select(ccn, year, log_price_resid, price, log_price,
             all_of(pred_vars), state, hrrnum, n_hosp_hrr,
             mkt_monopoly, mkt_duopoly, mkt_triopoly,
             mkt_small, mkt_medium, mkt_large),
    by = c("ccn", "year")
  ) %>%
  filter(!is.na(rp_combined), rp_combined > 0)

message("\nRAND-HCRIS overlap: ", format(nrow(overlap), big.mark = ","), " rows, ",
        n_distinct(overlap$ccn), " hospitals")

overlap <- overlap %>%
  mutate(log_rp = log(rp_combined))

train_vars <- c("log_rp", pred_vars,
                "mkt_monopoly", "mkt_duopoly", "mkt_triopoly",
                "mkt_small", "mkt_medium")
train_data <- overlap %>%
  select(all_of(train_vars)) %>%
  filter(complete.cases(.))

message("Training rows (complete cases): ", format(nrow(train_data), big.mark = ","))

# mkt_large is reference category (omitted)
adj_model <- lm(log_rp ~ log_price_resid + mcaid_share +
                   operating_margin + log_beds +
                   mkt_monopoly + mkt_duopoly +
                   mkt_triopoly + mkt_small + mkt_medium,
                 data = train_data)

message("\nAdjustment model:")
message("  R-sq = ", round(summary(adj_model)$r.squared, 3),
        "  Adj R-sq = ", round(summary(adj_model)$adj.r.squared, 3))
message("  N = ", format(nrow(train_data), big.mark = ","))

coefs <- summary(adj_model)$coefficients
message("\n  Coefficients:")
for (v in rownames(coefs)) {
  if (v != "(Intercept)") {
    message("    ", v, ": ", round(coefs[v, "Estimate"], 4),
            " (se = ", round(coefs[v, "Std. Error"], 4), ")",
            ifelse(coefs[v, "Pr(>|t|)"] < 0.01, " ***",
                   ifelse(coefs[v, "Pr(>|t|)"] < 0.05, " **",
                          ifelse(coefs[v, "Pr(>|t|)"] < 0.1, " *", ""))))
  }
}

# 7. Predict for acute hospitals, output full panel --------------------------

# Predict rp_hat for all acute hospitals with valid predictors
acute_out <- hcris_acute %>%
  select(ccn, year, state, hrrnum, price, log_price, log_price_resid,
         all_of(pred_vars), beds, tot_discharges, net_pat_rev, tot_operating_exp,
         n_hosp_hrr, mkt_monopoly, mkt_duopoly, mkt_triopoly,
         mkt_small, mkt_medium, mkt_large, name) %>%
  mutate(
    log_rp_hat = predict(adj_model, newdata = .),
    rp_hat = exp(log_rp_hat),
    acute = TRUE
  )

# Flag RAND-observed rows
rand_ccn_years <- overlap %>% select(ccn, year) %>% distinct()
acute_out <- acute_out %>%
  mutate(has_rand = paste0(ccn, "_", year) %in% paste0(rand_ccn_years$ccn, "_", rand_ccn_years$year))

# Non-acute rows: carry through with NAs for price fields
nonacute_out <- hcris_all %>%
  filter(!acute) %>%
  select(ccn, year, state, beds, tot_discharges, net_pat_rev, tot_operating_exp, name) %>%
  mutate(acute = FALSE, has_rand = FALSE)

message("\nAcute predictions: ", format(sum(!is.na(acute_out$rp_hat)), big.mark = ","),
        " / ", format(nrow(acute_out), big.mark = ","))
message("Non-acute rows (no price): ", format(nrow(nonacute_out), big.mark = ","))

n_predicted <- sum(!is.na(acute_out$rp_hat))
message("  Predicted rp_hat: mean = ", round(mean(acute_out$rp_hat, na.rm = TRUE), 2),
        "  median = ", round(median(acute_out$rp_hat, na.rm = TRUE), 2))

# In-sample validation
in_sample <- overlap %>%
  mutate(log_rp_hat = predict(adj_model, newdata = .),
         rp_hat = exp(log_rp_hat))
message("  In-sample cor(rp_combined, rp_hat): ",
        round(cor(in_sample$rp_combined, in_sample$rp_hat, use = "complete.obs"), 3))

# Combine and write
predict_data <- bind_rows(acute_out, nonacute_out) %>%
  arrange(ccn, year)

write_csv(predict_data, "data/output/price-predicted.csv")
message("\nWrote data/output/price-predicted.csv (",
        format(nrow(predict_data), big.mark = ","), " rows)")
