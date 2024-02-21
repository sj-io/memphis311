#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom lubridate mdy_hms
#' @importFrom lubridate ymd_hms
#' @importFrom utils globalVariables
## usethis namespace: end

utils::globalVariables(c(
  "close_date", "collection_day", "created_by", "followup_date", "incident_id",
  "incident_number", "incident_resolved_date", "incident_type_id",
  "last_update_date", "last_update_login", "mlgw_on", "next_open_task_date",
  "number_of_tasks", "reported_date"
))

NULL
