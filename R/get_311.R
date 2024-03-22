#' Get Memphis 311 Data
#'
#' @param ... parameters passed on to query (i.e. department = "code enforcement")
#'
#' @export
#'
#' @import stringr httr2 arrow janitor dplyr rappdirs
#' @importFrom lubridate mdy_hms ymd_hms
#' @importFrom httr GET write_disk progress
get_311 <- function(...) {

  dots <- c(...)
  names <- names(dots)

  if(length(dots) == 0) {

    temp_csv <- file.path(user_cache_dir("memphis311"), "temp.csv")

    message("Getting entire dataset. This will take a while.")
    httr::GET("https://data.memphistn.gov/api/views/hmd4-ddta/rows.csv?accessType=DOWNLOAD&api_foundry=true",
              httr::write_disk(temp_csv), httr::progress())

    df <- read_csv_arrow(temp_csv, as_data_frame = FALSE) |>
      janitor::clean_names() |>
      select(any_of(ce_columns)) |>
      distinct() |>
      arrange(desc(reported_date)) |>
      collect()

    df <- df |>
      mutate(department = str_to_lower(department),
             across(ends_with("_date"), ~ mdy_hms(.)),
             across(c(followup_date, mlgw_on), ~ as.Date(.)))

    unlink(temp_csv)

  } else {

    url <- "https://data.memphistn.gov/resource/hmd4-ddta.json?"

    add_to_call <- function(name, value) {
      if (str_ends(name, "_date")) {
        url_query <- paste0("date_trunc_ymd(", name, ")=%27", value, "%27")
      } else if (name %in% c("incident_id", "incident_number")) {
        url_query <- paste0(name, "=", value)
      } else {
        value <- str_to_upper(value)
        url_query <- paste0("upper(", name, ")=%27", value, "%27")
      }
    }

    the_query <- purrr::map2_chr(names, dots, add_to_call) |> str_flatten(collapse = " AND ")
    url <- paste0(url, "$where=(", the_query, ")&$order=:id&$limit=50000")
    url <- gsub(" ", "%20", url)
    message(url)

    # request data
    req <- httr2::request(url)

    # function which tells when there are no more responses
    # https://httr2.r-lib.org/reference/iterate_with_offset.html
    is_complete <- function(resp) {
      length(resp_body_json(resp)$data) == 0
    }


    # socrata limits requests to 1000. perform the call until no valid responses
    # https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset
    resp <- req_perform_iterative(
      req,
      next_req = iterate_with_offset("offset", offset = 1000, resp_complete = is_complete),
      max_reqs = Inf
    )

    # convert responses to table
    df <- resp |>
      resps_successes() |>
      resps_data(\(resp) resp_body_json(resp, simplifyVector = TRUE))

    if (length(df) == 0) {
      stop("Your API call returned no results.")
    }

    df <- df |> select(any_of(ce_columns)) |> distinct()

  }

  df

}
