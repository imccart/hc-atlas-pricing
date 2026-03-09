# Meta --------------------------------------------------------------------

## Title:         Oria Data Extraction
## Author:        Ian McCarthy
## Date Created:  2026-02-19
## Description:   Extracts hospital metadata and rate data from Oria parquet files.
##                Reads data/input/oria/ (symlink to raw data), writes to data/output/.

# 1. Hospital metadata ----------------------------------------------------

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

  # Extract EIN from filename (first 9 digits)
  mrf <- mrf %>%
    mutate(ein = str_extract(filename, "^\\d{9}")) %>%
    filter(!is.na(ein)) %>%
    select(hospital_id, ein) %>%
    distinct(hospital_id, .keep_all = TRUE)

  hosp <- hosp %>% left_join(mrf, by = "hospital_id")

  message("  ", nrow(hosp), " hospitals, ", sum(!is.na(hosp$ein)), " with EIN")
  write_csv(hosp, "data/output/oria-hospital.csv")
}

# 2. Rate data for target DRGs and CPTs -----------------------------------

if (!file.exists("data/output/oria-rates-drg.csv") ||
    !file.exists("data/output/oria-rates-cpt.csv")) {

  con <- dbConnect(duckdb())

  # Discover state partitions
  states <- list.dirs("data/input/oria/parquet/standard_charge_details",
                      recursive = FALSE, full.names = FALSE) %>%
    str_extract("(?<=hospital_state=).+") %>%
    na.omit()

  message("Extracting rates across ", length(states), " states...")

  # Query one state's parquet for target codes
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

  # Run across all states, skip errors (e.g. Nebraska corrupt parquet)
  extract_all_states <- function(code_filter) {
    results <- list()
    for (st in states) {
      df <- query_state(st, code_filter)
      if (!is.null(df) && nrow(df) > 0) results[[st]] <- as_tibble(df)
    }
    if (length(results) == 0) return(tibble())
    bind_rows(results)
  }

  # Unpivot wide charge columns into (standard_charge, rate_category) rows
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

  # Build SQL filters
  drg_in <- paste0("'", target_drgs$code, "'", collapse = ", ")
  cpt_in <- paste0("'", target_cpts$code, "'", collapse = ", ")

  # Extract and write DRG rates
  if (!file.exists("data/output/oria-rates-drg.csv")) {
    drg_rates <- extract_all_states(paste0("ms_drg IN (", drg_in, ")")) %>%
      unpivot_rates()
    message("  DRG: ", format(nrow(drg_rates), big.mark = ","), " rows")
    write_csv(drg_rates, "data/output/oria-rates-drg.csv")
  }

  # Extract and write CPT rates
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
