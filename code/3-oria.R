# Meta --------------------------------------------------------------------

## Title:         Oria MRF Extraction and Processing
## Author:        Ian McCarthy
## Date Created:  2026-03-09
## Description:   Extracts hospital metadata and negotiated rates from Oria
##                parquet files. Cleans codes, harmonizes payer names, and
##                produces two outputs:
##                  (1) oria-hospital-rates.csv — detailed hospital × service × payer
##                  (2) oria-hospital-summary.csv — hospital-level medians for panel join

# 1. Hospital metadata -------------------------------------------------------

if (!file.exists("data/output/oria-hospital.csv")) {
  message("Extracting hospital metadata...")

  con_meta <- dbConnect(duckdb(), dbdir = "data/input/oria/mrf_lake.duckdb", read_only = TRUE)

  hosp <- dbGetQuery(con_meta, "
    SELECT hospital_id, hospital_name, hospital_address, hospital_city,
           hospital_state, total_charges_count, status
    FROM hospitals
    WHERE status = 'completed'
  ") %>% as_tibble()

  mrf <- dbGetQuery(con_meta, "
    SELECT hospital_id, filename FROM mrf_metadata WHERE filename IS NOT NULL
  ") %>% as_tibble()

  dbDisconnect(con_meta, shutdown = TRUE)

  mrf <- mrf %>%
    mutate(ein = str_extract(filename, "^\\d{9}")) %>%
    filter(!is.na(ein)) %>%
    select(hospital_id, ein) %>%
    distinct(hospital_id, .keep_all = TRUE)

  hosp <- hosp %>% left_join(mrf, by = "hospital_id")

  message("  ", nrow(hosp), " hospitals, ", sum(!is.na(hosp$ein)), " with EIN")
  write_csv(hosp, "data/output/oria-hospital.csv")
}

# 2. Rate extraction ---------------------------------------------------------

if (!file.exists("data/output/oria-rates-drg.csv") ||
    !file.exists("data/output/oria-rates-cpt.csv")) {

  con <- dbConnect(duckdb())

  states <- list.dirs("data/input/oria/parquet/standard_charge_details",
                      recursive = FALSE, full.names = FALSE) %>%
    str_extract("(?<=hospital_state=).+") %>%
    na.omit()

  message("Extracting rates across ", length(states), " states...")

  query_state <- function(state, code_filter) {
    path <- paste0("data/input/oria/parquet/standard_charge_details/hospital_state=",
                   state, "/*.parquet") %>%
      gsub("\\\\", "/", .)
    sql <- paste0(
      "SELECT hospital_id, description, ms_drg, cpt, hcpcs, ",
      "payer_name, plan_name, ",
      "standard_charge_dollar, gross_charge, discounted_cash, minimum, maximum, ",
      "setting, billing_class ",
      "FROM read_parquet('", path, "') WHERE ", code_filter
    )
    tryCatch(dbGetQuery(con, sql), error = function(e) {
      message("  ERROR in ", state, ": ", conditionMessage(e))
      NULL
    })
  }

  extract_all_states <- function(code_filter) {
    results <- list()
    for (st in states) {
      df <- query_state(st, code_filter)
      if (!is.null(df) && nrow(df) > 0) results[[st]] <- as_tibble(df)
    }
    if (length(results) == 0) return(tibble())
    bind_rows(results)
  }

  unpivot_rates <- function(raw) {
    if (nrow(raw) == 0) return(tibble())
    dedup_cols <- c("hospital_id", "description", "ms_drg", "cpt", "hcpcs")
    bind_rows(
      raw %>% filter(!is.na(standard_charge_dollar), standard_charge_dollar > 0) %>%
        mutate(standard_charge = standard_charge_dollar, rate_category = "negotiated"),
      raw %>% filter(!is.na(gross_charge), gross_charge > 0) %>%
        distinct(across(all_of(dedup_cols)), .keep_all = TRUE) %>%
        mutate(standard_charge = gross_charge, rate_category = "gross",
               payer_name = NA_character_, plan_name = NA_character_),
      raw %>% filter(!is.na(discounted_cash), discounted_cash > 0) %>%
        distinct(across(all_of(dedup_cols)), .keep_all = TRUE) %>%
        mutate(standard_charge = discounted_cash, rate_category = "cash",
               payer_name = NA_character_, plan_name = NA_character_),
      raw %>% filter(!is.na(minimum), minimum > 0) %>%
        distinct(across(all_of(dedup_cols)), .keep_all = TRUE) %>%
        mutate(standard_charge = minimum, rate_category = "min",
               payer_name = NA_character_, plan_name = NA_character_),
      raw %>% filter(!is.na(maximum), maximum > 0) %>%
        distinct(across(all_of(dedup_cols)), .keep_all = TRUE) %>%
        mutate(standard_charge = maximum, rate_category = "max",
               payer_name = NA_character_, plan_name = NA_character_)
    ) %>%
      mutate(hcpcs_cpt = coalesce(cpt, hcpcs)) %>%
      select(hospital_id, description, ms_drg, hcpcs_cpt,
             payer_name, plan_name, standard_charge, rate_category,
             setting, billing_class)
  }

  drg_in <- paste0("'", target_drgs$code, "'", collapse = ", ")
  cpt_in <- paste0("'", target_cpts$code, "'", collapse = ", ")

  if (!file.exists("data/output/oria-rates-drg.csv")) {
    drg_rates <- extract_all_states(paste0("ms_drg IN (", drg_in, ")")) %>%
      unpivot_rates()
    message("  DRG: ", format(nrow(drg_rates), big.mark = ","), " rows")
    write_csv(drg_rates, "data/output/oria-rates-drg.csv")
  }

  if (!file.exists("data/output/oria-rates-cpt.csv")) {
    cpt_rates <- extract_all_states(
      paste0("(cpt IN (", cpt_in, ") OR hcpcs IN (", cpt_in, "))")
    ) %>% unpivot_rates()
    message("  CPT: ", format(nrow(cpt_rates), big.mark = ","), " rows")
    write_csv(cpt_rates, "data/output/oria-rates-cpt.csv")
  }

  dbDisconnect(con, shutdown = TRUE)
}

message("Oria extraction complete.")

# 3. Code standardization and payer harmonization ----------------------------

rates <- bind_rows(
  fread("data/output/oria-rates-drg.csv") %>% as_tibble(),
  fread("data/output/oria-rates-cpt.csv") %>% as_tibble()
) %>%
  rename(payer = payer_name, plan = plan_name) %>%
  mutate(standard_charge = as.numeric(standard_charge))

# Unified code column
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

# Keep target codes, drop invalid charges
rates <- rates %>%
  inner_join(target_codes %>% select(code, code_type, label),
             by = c("code", "code_type")) %>%
  filter(!is.na(standard_charge), is.finite(standard_charge), standard_charge > 0)

message("Cleaned rates: ", format(nrow(rates), big.mark = ","), " rows")

# Payer classification
classify_payer <- function(payer_name) {
  # Strip escaped quotes (e.g., ""Aetna"" -> Aetna)
  clean <- str_replace_all(payer_name, '"+', "")
  payer_upper <- toupper(trimws(clean))
  case_when(
    is.na(payer_name) | payer_upper == "" ~ NA_character_,
    str_detect(payer_upper, "BLUE\\s*CROSS|BLUE\\s*SHIELD|BCBS|ANTHEM|CAREFIRST|HIGHMARK|PREMERA|REGENCE|WELLMARK|HORIZON|INDEPENDENCE|EXCELLUS|EMPIRE") ~ "BCBS",
    str_detect(payer_upper, "UNITED\\s*HEALTH|^UNITED$|UHC|OPTUM|UHCSR|UNITED\\s*BEHAVIORAL") ~ "UHC",
    str_detect(payer_upper, "AETNA|CVS\\s*HEALTH") ~ "Aetna",
    str_detect(payer_upper, "CIGNA|EVERNORTH") ~ "Cigna",
    str_detect(payer_upper, "HUMANA|BRAVO\\s*HEALTH") ~ "Humana",
    str_detect(payer_upper, "KAISER") ~ "Kaiser",
    str_detect(payer_upper, "CENTENE|WELLCARE|AMBETTER|HEALTH\\s*NET|FIDELIS|PEACH\\s*STATE|SUNSHINE\\s*HEALTH|AMERIGROUP") ~ "Centene",
    str_detect(payer_upper, "MOLINA") ~ "Molina",
    str_detect(payer_upper, "MULTIPLAN") ~ "MultiPlan",
    str_detect(payer_upper, "BRIGHT\\s*HEALTH") ~ "Bright Health",
    str_detect(payer_upper, "HARVARD\\s*PILGRIM") ~ "Harvard Pilgrim",
    str_detect(payer_upper, "SELECT\\s*HEALTH") ~ "Select Health",
    str_detect(payer_upper, "HEALTHFIRST") ~ "Healthfirst",
    str_detect(payer_upper, "EMBLEM") ~ "EmblemHealth",
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

# 4. EIN crosswalk to CCN ---------------------------------------------------

hosp <- fread("data/output/oria-hospital.csv") %>%
  as_tibble() %>%
  rename(oria_name = hospital_name, oria_state = hospital_state) %>%
  mutate(ein = as.character(ein)) %>%
  select(hospital_id, oria_name, hospital_city, oria_state, ein)

crosswalk_ein <- fread("../hc-atlas/data/output/npi-ccn-crosswalk-enriched.csv") %>%
  as_tibble() %>%
  filter(!is.na(ein), ein != "") %>%
  mutate(ein = str_pad(ein, width = 9, side = "left", pad = "0")) %>%
  select(ein, ccn) %>%
  distinct(ein, .keep_all = TRUE)

hosp <- hosp %>% left_join(crosswalk_ein, by = "ein")

matched_ein <- sum(!is.na(hosp$ccn))
message("Oria EIN-to-CCN match: ", matched_ein, " / ", nrow(hosp),
        " (", round(100 * matched_ein / nrow(hosp), 1), "%)")

# 4b. Fuzzy name matching for unmatched hospitals ---------------------------

# Name preprocessing (adapted from hc-atlas/5-enrich.R)
preprocess_name <- function(x) {
  x %>%
    tolower() %>%
    str_replace_all("\\(.*?\\)", "") %>%
    str_replace_all("\\b(inc|llc|corp|corporation|ltd|lp|co)\\b\\.?", "") %>%
    str_replace_all("\\b(hshs|ssm|hca|banner|ascension|commonspirit|advocate|mercy|trinity|christus|providence|dignity|sutter|intermountain|kaiser|tenet|community health|lifepoint|universal health|prime healthcare|ardent|quorum)\\b", "") %>%
    str_replace_all("\\bst\\.?\\b", "saint") %>%
    str_replace_all("\\bmt\\.?\\b", "mount") %>%
    str_replace_all("\\bgen\\.?\\b", "general") %>%
    str_replace_all("\\bhosp\\.?\\b", "hospital") %>%
    str_replace_all("\\bmed\\.?\\b", "medical") %>%
    str_replace_all("\\bctr\\.?\\b", "center") %>%
    str_replace_all("\\breg\\.?\\b", "regional") %>%
    str_replace_all("\\bmem\\.?\\b", "memorial") %>%
    str_replace_all("\\buniv\\.?\\b", "university") %>%
    str_replace_all("\\bhlth\\.?\\b", "health") %>%
    str_replace_all("\\bmedical center\\b", "hospital") %>%
    str_replace_all("\\bhealth system\\b", "hospital") %>%
    str_replace_all("[^a-z0-9 ]", "") %>%
    # Strip generic facility terms so matching focuses on distinctive words
    str_replace_all("\\b(hospital|medical|center|memorial|regional|general|community|health|healthcare|system|authority|district|county)\\b", "") %>%
    str_squish()
}

jaccard_words <- function(a, b) {
  wa <- str_split(a, "\\s+")[[1]]
  wb <- str_split(b, "\\s+")[[1]]
  if (length(wa) == 0 || length(wb) == 0) return(0)
  length(intersect(wa, wb)) / length(union(wa, wb))
}

# Reference: HCRIS hospitals (most recent name per CCN)
hcris_ref <- fread("data/output/hcris-panel.csv") %>%
  as_tibble() %>%
  mutate(ccn = as.character(ccn)) %>%
  filter(acute == TRUE) %>%
  group_by(ccn) %>%
  slice_max(year, n = 1) %>%
  ungroup() %>%
  select(ccn, hcris_name = name, hcris_city = city, hcris_state = state) %>%
  distinct(ccn, .keep_all = TRUE) %>%
  mutate(clean_name = preprocess_name(hcris_name),
         clean_city = tolower(trimws(hcris_city)))

# Unmatched Oria hospitals
unmatched <- hosp %>%
  filter(is.na(ccn)) %>%
  select(hospital_id, oria_name, oria_city = hospital_city, oria_state) %>%
  mutate(clean_name = preprocess_name(oria_name),
         clean_city = tolower(trimws(oria_city)))

message("Fuzzy matching ", nrow(unmatched), " unmatched Oria hospitals against ",
        nrow(hcris_ref), " HCRIS hospitals...")

# Pass 1: Jaro-Winkler with state + city blocking (looser threshold)
fuzzy_results <- list()

for (st in unique(unmatched$oria_state)) {
  oria_st <- unmatched %>% filter(oria_state == st)
  hcris_st <- hcris_ref %>% filter(hcris_state == st)
  if (nrow(oria_st) == 0 || nrow(hcris_st) == 0) next

  for (i in seq_len(nrow(oria_st))) {
    # Try city match first
    hcris_city <- hcris_st %>% filter(clean_city == oria_st$clean_city[i])
    if (nrow(hcris_city) > 0) {
      dists <- stringdist(oria_st$clean_name[i], hcris_city$clean_name, method = "jw")
      scores <- 1 - dists
      best <- which.max(scores)
      if (scores[best] >= 0.85) {
        fuzzy_results[[length(fuzzy_results) + 1]] <- tibble(
          hospital_id = oria_st$hospital_id[i],
          ccn = hcris_city$ccn[best],
          score = scores[best],
          strategy = "jw_state_city"
        )
        next
      }
    }
    # Fall back to state-only with stricter threshold
    dists <- stringdist(oria_st$clean_name[i], hcris_st$clean_name, method = "jw")
    scores <- 1 - dists
    best <- which.max(scores)
    if (scores[best] >= 0.92) {
      fuzzy_results[[length(fuzzy_results) + 1]] <- tibble(
        hospital_id = oria_st$hospital_id[i],
        ccn = hcris_st$ccn[best],
        score = scores[best],
        strategy = "jw_state_only"
      )
    }
  }
}

# Pass 2: Word Jaccard with state + city blocking (for remaining unmatched)
matched_ids <- map_int(fuzzy_results, ~ .x$hospital_id)
still_unmatched <- unmatched %>% filter(!hospital_id %in% matched_ids)

for (st in unique(still_unmatched$oria_state)) {
  oria_st <- still_unmatched %>% filter(oria_state == st)
  hcris_st <- hcris_ref %>% filter(hcris_state == st)
  if (nrow(oria_st) == 0 || nrow(hcris_st) == 0) next

  for (i in seq_len(nrow(oria_st))) {
    hcris_city <- hcris_st %>% filter(clean_city == oria_st$clean_city[i])
    if (nrow(hcris_city) > 0) {
      scores <- map_dbl(hcris_city$clean_name, ~ jaccard_words(oria_st$clean_name[i], .x))
      best <- which.max(scores)
      if (scores[best] >= 0.75) {
        fuzzy_results[[length(fuzzy_results) + 1]] <- tibble(
          hospital_id = oria_st$hospital_id[i],
          ccn = hcris_city$ccn[best],
          score = scores[best],
          strategy = "jaccard_state_city"
        )
      }
    }
  }
}

fuzzy_matches <- bind_rows(fuzzy_results)

if (nrow(fuzzy_matches) > 0) {
  # Deduplicate: one CCN per hospital_id (best score wins)
  fuzzy_matches <- fuzzy_matches %>%
    group_by(hospital_id) %>%
    slice_max(score, n = 1) %>%
    ungroup()

  # Diagnostic: save match pairs for review
  fuzzy_diagnostic <- fuzzy_matches %>%
    left_join(unmatched %>% select(hospital_id, oria_name, oria_city = clean_city, oria_state),
              by = "hospital_id") %>%
    left_join(hcris_ref %>% select(ccn, hcris_name, hcris_city = clean_city, hcris_state),
              by = "ccn") %>%
    arrange(score) %>%
    select(hospital_id, oria_name, oria_city, oria_state, ccn, hcris_name, hcris_city, hcris_state,
           score, strategy)
  write_csv(fuzzy_diagnostic, "data/output/oria-fuzzy-matches.csv")
  message("Wrote data/output/oria-fuzzy-matches.csv: ", nrow(fuzzy_diagnostic),
          " matches for review")

  # Apply fuzzy matches to unmatched hospitals
  hosp <- hosp %>%
    left_join(fuzzy_matches %>% select(hospital_id, ccn_fuzzy = ccn), by = "hospital_id") %>%
    mutate(ccn = coalesce(ccn, ccn_fuzzy)) %>%
    select(-ccn_fuzzy)
}

matched_total <- sum(!is.na(hosp$ccn))
matched_fuzzy <- matched_total - matched_ein
message("Fuzzy name match: ", matched_fuzzy, " new matches")
message("Total Oria-to-CCN: ", matched_total, " / ", nrow(hosp),
        " (", round(100 * matched_total / nrow(hosp), 1), "%)")

# 5. Detailed output: hospital × service × payer ----------------------------

rates_detail <- rates %>%
  inner_join(hosp %>% select(hospital_id, oria_name, oria_state, ccn),
             by = "hospital_id") %>%
  select(hospital_id, ccn, oria_name, oria_state,
         code, code_type, label, setting,
         rate_category, standard_charge,
         payer, plan, payer_category)

write_csv(rates_detail, "data/output/oria-hospital-rates.csv")
message("Wrote data/output/oria-hospital-rates.csv: ",
        format(nrow(rates_detail), big.mark = ","), " rows")

# 6. Hospital-level summary for panel join -----------------------------------

# Commercial negotiated rates only (exclude Medicaid MCO, Medicare, Medicare Advantage)
commercial_cats <- c("BCBS", "UHC", "Aetna", "Cigna", "Humana",
                     "Kaiser", "Centene", "Molina", "MultiPlan",
                     "Bright Health", "Harvard Pilgrim", "Select Health",
                     "Healthfirst", "EmblemHealth", "Other")

oria_summary <- rates_detail %>%
  filter(rate_category == "negotiated",
         payer_category %in% commercial_cats,
         !is.na(ccn)) %>%
  group_by(ccn) %>%
  summarize(
    oria_median_rate = median(standard_charge, na.rm = TRUE),
    oria_mean_rate = mean(standard_charge, na.rm = TRUE),
    oria_n_rates = n(),
    oria_n_codes = n_distinct(code),
    oria_n_payers = n_distinct(payer_category),
    .groups = "drop"
  ) %>%
  # Also compute IP vs OP medians
  left_join(
    rates_detail %>%
      filter(rate_category == "negotiated",
             payer_category %in% commercial_cats,
             !is.na(ccn),
             code_type == "MS-DRG") %>%
      group_by(ccn) %>%
      summarize(oria_median_ip = median(standard_charge, na.rm = TRUE),
                oria_n_ip = n(), .groups = "drop"),
    by = "ccn"
  ) %>%
  left_join(
    rates_detail %>%
      filter(rate_category == "negotiated",
             payer_category %in% commercial_cats,
             !is.na(ccn),
             code_type == "CPT/HCPCS") %>%
      group_by(ccn) %>%
      summarize(oria_median_op = median(standard_charge, na.rm = TRUE),
                oria_n_op = n(), .groups = "drop"),
    by = "ccn"
  )

write_csv(oria_summary, "data/output/oria-hospital-summary.csv")
message("Wrote data/output/oria-hospital-summary.csv: ",
        format(nrow(oria_summary), big.mark = ","), " hospitals with CCN match")
