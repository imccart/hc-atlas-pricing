# Meta --------------------------------------------------------------------

## Title:         RAND Price Transparency Extraction
## Author:        Ian McCarthy
## Date Created:  2026-03-08
## Description:   Extracts hospital-level prices and service-line prices from
##                RAND Hospital Price Transparency Study supplemental Excel files
##                (Rounds 2-5). Reads data/input/rand/ (symlink), writes to data/output/.

# Round configs: file paths, sheet names, skip rows
rounds <- list(
  list(round = 2, years = "2015-2017",
       file = "data/input/rand/round2/Supplement-National-Hospital-Price-Transparency-Report-20190509.xlsx",
       t1 = list(sheet = "Table 1. Hospitals", skip = 10),
       t4 = list(sheet = "Table 4. Outpatient Services", skip = 10),
       t5 = list(sheet = "Table 5. Inpatient Services", skip = 9)),
  list(round = 3, years = "2016-2018",
       file = "data/input/rand/round3/RAND-3.0-Supplemental-Material-10-29-2021.xlsx",
       t1 = list(sheet = "Table 1. PF Hospitals", skip = 9),
       t4 = list(sheet = "Table 4. PF Outpatient Servi", skip = 9),
       t5 = list(sheet = "Table 5. PF Inpatient Servic", skip = 8)),
  list(round = 4, years = "2018-2020",
       file = "data/input/rand/round4/Supplemental_Material.xlsx",
       t1 = list(sheet = "Table 1. Hospitals", skip = 4),
       t4 = list(sheet = "Table 5. Outpatient Services", skip = 3),
       t5 = list(sheet = "Table 6. Inpatient Services", skip = 3)),
  list(round = 5, years = "2020-2022",
       file = "data/input/rand/round5/RR-A1144-3-v2.annex.xlsx",
       t1 = list(sheet = "Table 1.  Hospitals", skip = 4),
       t4 = list(sheet = "Table 4. Outpatient Services", skip = 3),
       t5 = list(sheet = "Table 5. Inpatient Services", skip = 3))
)

# 1. Hospital-level prices (Table 1) ----------------------------------------

if (!file.exists("data/output/rand-hospitals.csv")) {
  message("Extracting RAND hospital-level prices...")

  read_t1 <- function(cfg) {
    d <- read_excel(cfg$file, sheet = cfg$t1$sheet, skip = cfg$t1$skip)
    names(d) <- str_replace_all(names(d), "\\n", " ") %>% str_squish()

    # Standardize column names across rounds
    d <- d %>% rename_with(~ "ccn", matches("^Medicare provider"))
    d <- d %>% rename_with(~ "name", matches("^Hospital name"))
    d <- d %>% rename_with(~ "state", matches("^State$"))
    d <- d %>% rename_with(~ "system", matches("^Hospital system"))
    d <- d %>% rename_with(~ "cah", matches("critical access"))
    d <- d %>% rename_with(~ "star_rating", matches("Hospital Compare"))

    # Price columns — names vary slightly but pattern is consistent
    d <- d %>% rename_with(~ "n_op", matches("^Number of outpatient services$"))
    d <- d %>% rename_with(~ "rp_op", matches("^Relative price for outpatient services$"))
    d <- d %>% rename_with(~ "sp_op", matches("^Standardized price per outpatient service$"))
    d <- d %>% rename_with(~ "n_ip", matches("^Number of inpatient stays$"))
    d <- d %>% rename_with(~ "rp_ip", matches("^Relative price for inpatient services$"))
    d <- d %>% rename_with(~ "sp_ip", matches("^Standardized price per inpatient stay$"))
    d <- d %>% rename_with(~ "rp_combined", matches("^Relative price for inpatient and outpatient services$"))

    # Rounds 4-5 extras
    if ("Leapfrog grade (Fall 2021)" %in% names(d)) {
      d <- d %>% rename_with(~ "leapfrog", matches("^Leapfrog"))
    }
    if (any(str_detect(names(d), "latitude"))) {
      d <- d %>%
        rename_with(~ "lat", matches("latitude")) %>%
        rename_with(~ "lon", matches("longitude"))
    }
    if (any(str_detect(names(d), "health.sys.id|health_sys_id"))) {
      d <- d %>% rename_with(~ "health_sys_id", matches("health.sys.id|health_sys_id"))
    }

    # Select common + available columns
    keep <- intersect(c("ccn", "name", "state", "system", "cah", "star_rating",
                        "n_op", "rp_op", "sp_op", "n_ip", "rp_ip", "sp_ip",
                        "rp_combined", "health_sys_id", "leapfrog", "lat", "lon"),
                      names(d))
    d %>%
      select(all_of(keep)) %>%
      filter(!is.na(ccn), ccn != "All") %>%
      mutate(round = cfg$round, years = cfg$years,
             across(c(n_op, rp_op, sp_op, n_ip, rp_ip, sp_ip, rp_combined),
                    ~ suppressWarnings(as.numeric(.x))))
  }

  hospitals <- map_dfr(rounds, read_t1)
  message("  ", format(nrow(hospitals), big.mark = ","), " hospital-rounds across ",
          n_distinct(hospitals$ccn), " unique CCNs")
  write_csv(hospitals, "data/output/rand-hospitals.csv")
}

# 2. Service-line prices (Tables 4 & 5) -------------------------------------

if (!file.exists("data/output/rand-service-lines.csv")) {
  message("Extracting RAND service-line prices...")

  read_service <- function(cfg, setting) {
    info <- if (setting == "outpatient") cfg$t4 else cfg$t5
    d <- read_excel(cfg$file, sheet = info$sheet, skip = info$skip)
    names(d) <- str_replace_all(names(d), "\\n", " ") %>% str_squish()

    d <- d %>% rename_with(~ "ccn", matches("^Medicare provider"))
    d <- d %>% rename_with(~ "name", matches("^Hospital name"))

    # Drop the "All" summary row and any non-CCN rows
    d <- d %>% filter(!is.na(ccn), ccn != "All", str_detect(ccn, "^\\d{6}$"))

    # Columns come in triplets: count, relative price, standardized price per service line
    # Extract service lines by position (cols 3-20, triplets of 3)
    col_names <- names(d)[3:ncol(d)]
    n_lines <- length(col_names) %/% 3

    lines <- list()
    for (i in seq_len(n_lines)) {
      idx <- (i - 1) * 3 + 3  # column index in original df
      count_col <- names(d)[idx]
      rp_col <- names(d)[idx + 1]
      sp_col <- names(d)[idx + 2]

      # Extract service line name from count column
      svc <- count_col %>%
        str_remove("^Number of (outpatient|inpatient) services,\\s*") %>%
        str_remove("^all (outpatient services|inpatient stays)$") %>%
        str_squish()
      if (svc == "") svc <- "all"

      lines[[i]] <- d %>%
        select(ccn, name, count = all_of(count_col),
               relative_price = all_of(rp_col), standardized_price = all_of(sp_col)) %>%
        mutate(service_line = svc, setting = setting,
               across(c(count, relative_price, standardized_price),
                      ~ suppressWarnings(as.numeric(.x))))
    }

    bind_rows(lines) %>%
      mutate(round = cfg$round, years = cfg$years)
  }

  service_lines <- map_dfr(rounds, function(cfg) {
    bind_rows(
      read_service(cfg, "outpatient"),
      read_service(cfg, "inpatient")
    )
  })

  # Drop rows where all price columns are NA
  service_lines <- service_lines %>%
    filter(!is.na(relative_price) | !is.na(standardized_price) | !is.na(count))

  message("  ", format(nrow(service_lines), big.mark = ","), " rows across ",
          n_distinct(service_lines$service_line), " service lines")
  write_csv(service_lines, "data/output/rand-service-lines.csv")
}

message("RAND extraction complete.")
