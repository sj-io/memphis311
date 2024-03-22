tidy_311 <- function(df) {
  # the_raw <- get_311(department = "code enforcement")

  # the_narrow <- the_raw |> select(incident_number, ce_category, request_type, resolution_code:request_priority, reported_date)
  # the_narrow <- the_narrow |> arrange(desc(reported_date)) |> distinct()
  # the_narrow$og_summary <- the_narrow$resolution_summary
  # the_narrow <- the_narrow |> mutate(across(c(ce_category:request_priority), ~ str_to_lower(.)))
  # the_narrow <- the_narrow |> mutate(resolution_summary = if_else(resolution_summary == resolution_code | resolution_summary == resolution_code_meaning, NA_character_, resolution_summary, NA_character_))



  # PART 1
  # narrow columns
  df <- df |> select(incident_number, ce_category, request_type, resolution_code:request_priority, reported_date)
  # df <- df |> select(category:request_type, incident_number:incident_resolved_date, created_by_user, owner_name, parcel_id, full_address)
  df <- df |> arrange(desc(reported_date)) |> distinct()
  # save copy of original resolution summary column
  df$og_summary <- df$resolution_summary
  # all lower case
  df <- df |> mutate(across(c(ce_category:request_priority), ~ str_to_lower(.)))
  # if res summary just duplicated resolution code, change to NA
  df <- df |> mutate(resolution_summary = if_else(resolution_summary %in% c(resolution_code, resolution_code_meaning), NA_character_, resolution_summary, NA_character_))

  # PART 1.5
  # add land_use
  df1 <- df |> mutate(
    land_use_request_type = str_extract(request_type, "^comm") |> str_replace("comm", "commercial"),
    issue_category_request_type = str_extract(request_type, "junk|vehicle|covid|graffiti|pot holes|storage|substandard|weeds"),
    issue_area_request_type = str_extract(request_type, "vacant lot|substandard|pot holes|weeds|vehicle") |> str_replace_all(c("substandard" = "building", "pot holes" = "street", "weeds" = "yard")),
    occupancy_request_type = str_extract(request_type, "occupied|vacant"),
    request_type = str_remove(request_type, "^c(omm|e)\\W+|junky.*"),
    resolution_code = str_remove(resolution_code, "\\Wcomm$"),
    .after = incident_number
  ) |> select(-request_type)
  # add violation_found
  df1 <- df1 |> mutate(
    violation_found_ce_category = str_extract(ce_category, "violation|board-up|cd|court|npa"),
    issue_error_ce_category = str_extract(ce_category, "shelby") |> str_replace("shelby", "county-owned property"),
    issue_order_ce_category = str_extract(ce_category, "board-up|cd"),
    issue_area_ce_category = str_extract(ce_category, "board-up|cd") |> str_replace("board-up|cd", "building"),
    in_court_ce_category = str_extract(ce_category, "court|npa"),
    .after = incident_number
  ) |> select(-ce_category)
  # add in_court
  df1 <- df1 |> mutate(
    issue_area_resolution_code = str_extract(resolution_code, "voyc")
    # issue_error_resolution_code = str_extract()
  )

  # clean up categories
  df1 <- df |> mutate(
    issue_category = case_match(
      request_type,
      c("ce-vehicle violations", "comm-vehicle violation", "ce-recreational vehicle illeg") ~ "auto",
      c("ce-weeds occupied property", "comm-junky property", "ce-vacant lot code violation", "comm-weeds occupied property", "ce-open storage and furnishin", "ce-junky yard") ~ "yard",
      c("comm-substandard delelict, struc", "ce-substandard,derelict struc") ~ "building",
      "comm-graffiti" ~ "graffiti",
      "comm-pot holes" ~ "street",
      c("ce-covid19(mask)", "ce-covid19(eo)", "ce-covid19") ~ "covid",
      "ce-tool bank request" ~ "tool bank",
      .default = NA_character_
    ),
    issue_category = case_match(
      resolution_code,
      c("cd", "cdc", "cdo", "demo-assigned", "hold", "id2", "jd-dm", "jw-rh", "occ", "occ-r", "occ-ri1-wp", "occ-ri2-wp", "rh-comm", "sc") ~ "building",
      "vob" ~ "accessory building",
      c("cvoar", "cvoat", "voar") ~ "auto",
      c("cvoyc", "voyc") ~ "yard",
      .default = issue_category
    ),
    issue_error = case_match(
      ce_category,
      "shelby county" ~ "county-owned property",
      .default = NA_character_
    ),
    issue_error = case_match(
      resolution_code,
      "ja" ~ "duplicate",
      "back to mcsc" ~ "wrong department",
      "shelby" ~ "county-owned property",
      .default = issue_error
    ),
    violation_found = case_match(
      ce_category,
      c("violation (ns)", "violation (s)") ~ TRUE
    ),
    violation_found = case_match(
      resolution_code,
      c("nj") ~ FALSE,
      .default = violation_found
    ),
    violation_order = case_match(
      ce_category,
      "cd" ~ "condemned",
      "board-up" ~ "board-up",
      .default = NA_character_
    ),
    in_court = case_match(
      ce_category,
      c("court", "npa") ~ TRUE,
      .default = NA
    ),
    resolution_code = str_remove_all(resolution_code, v_rc),
    .after = incident_number
  ) |> select(-c(request_type, ce_category))

  v_rc <- str_c("\\b(", str_flatten(c("covid19-incompliance", "back to mcsc", "ja", "nj"), collapse = "|"), ")\\b")


  # request_type ignored: c("ce-code miscellaneous", "comm - miscellaneous", "ce-accumulation of stagnant", "ce-downtown cleanup")
  # ce_category ignored: c(NA, "not-categorized", "open", "other")



  # remove NAs
  df_na <- df |> filter(is.na(resolution_summary))
  df <- df |> anti_join(df_na)

  # PART 2
  # remove punctuation
  dfm <- df |> mutate(resolution_summary = str_remove_all(resolution_summary, "'") |> str_replace_all("[:punct:]|[:symbol:]", " ") |> str_squish())

  # extract numbers
  # phone numbers
  dfm <- dfm |> mutate(
    phone = str_extract_all(resolution_summary, v_phone),
    resolution_summary = str_remove_all(resolution_summary, v_phone)
    )
  # service requests
  dfm <- dfm |> mutate(
    related_sr = str_extract_all(resolution_summary, v_sr),
    resolution_summary = str_remove_all(resolution_summary, v_sr) |> str_squish()
  )
  # dates
  dfm <- dfm |> mutate(
    follow_up_date = str_extract_all(resolution_summary, paste0("\\bf u ", v_date)),
    other_date = str_extract_all(resolution_summary, v_date),
    resolution_summary = str_remove_all(resolution_summary, paste0("(\\bf u )?", v_date)) |> str_squish()
  )
  # remove nas
  dfm <- dfm |> mutate(resolution_summary = na_if(resolution_summary, ""))
  dfm_na <- dfm |> filter(is.na(resolution_summary))
  dfm <- dfm |> anti_join(dfm_na)

  # PART 3
  # correct typos
  v_typos <- lut_typos$output
  names(v_typos) <- lut_typos$input

  dfo <- dfm |> mutate(
    resolution_summary = str_replace_all(resolution_summary, v_typos)
  )

  # extract inspectors
  v_inspectors <- str_c("(per )?((inspector|supervisor) )?\\b(", str_flatten(lut_inspectors$input, collapse = "|"), ")\\b")

  dfp <- dfo |> mutate(
    inspector = str_extract_all(resolution_summary, v_inspectors),
    resolution_summary = str_remove_all(resolution_summary, v_inspectors) |> str_squish()
  )
  # remove nas
  dfp <- dfp |> mutate(resolution_summary = na_if(resolution_summary, ""))
  dfp_na <- dfp |> filter(is.na(resolution_summary))
  dfp <- dfp |> anti_join(dfp_na)

  # PART 4
  # extract update events (inspection, reinspection, attempt)
  v_update_event <- str_c("\\b(", str_flatten(unique(lut_update_event$output), collapse = "|"), ")\\b")
  r_update_event <- lut_update_event$output
  names(r_update_event) <- lut_update_event$input

  dfq <- dfp |> mutate(
    resolution_summary = str_replace_all(resolution_summary, r_update_event),
    update_event = str_extract_all(resolution_summary, v_update_event),
    resolution_summary = str_remove_all(resolution_summary, v_update_event) |> str_squish()
  )

  # create general categories to find building violations
  dfr <- dfq |> mutate(
    issue_category = case_match(
      request_type,
      c("CE-Vehicle Violations", "CE-Recreational Vehicle Illeg") ~ "auto",
      c("CE-Junky Yard", "CE-Open Storage and Furnishin", "CE-Weeds Occupied Property", "CE-Vacant Lot Code Violation") ~ "yard",
      "CE-Substandard,Derelict Struc" ~ "building",
      c("CE-Covid19", "CE-Covid19(Mask)", "CE-Covid19(EO)") ~ "covid",
      "CE-Tool Bank Request" ~ "tool bank",
      .default = NA_character_
    ),
    issue_category = case_match(
      resolution_code,
      c("cd", "cdc", "cdo", "demo-assigned", "hold", "id2", "jd-dm", "jw-rh", "occ", "occ-r", "occ-ri1-wp", "occ-ri2-wp", "rh-comm", "sc") ~ "building",
      "vob" ~ "accessory building",
      c("cvoar", "cvoat", "voar") ~ "auto",
      c("cvoyc", "voyc") ~ "yard",
      .default = issue_category
    )
  )

  # REMOVE THIS SEPARATION LATER
  not_buildings <- dfr |>
    filter(issue_category %in% c("auto", "yard", "covid", "tool bank"))

  maybe_buildings <- dfr |>
    anti_join(not_buildings)

  # occupancy


  # extract update tags
  v_update_tags <- str_c("\\b(", str_flatten(unique(lut_update_tags$output), collapse = "|"), ")\\b")
  r_update_tags <- lut_update_tags$output
  names(r_update_tags) <- lut_update_tags$input

  dfs <- maybe_buildings |> mutate(
    resolution_summary = str_replace_all(resolution_summary, r_update_tags),
    update_tags = str_extract_all(resolution_summary, v_update_tags),
    resolution_summary = str_remove_all(resolution_summary, v_update_tags) |> str_squish()
  )
}



c("cd", "cdc", "cdo", "demo-assigned", "hold", "id2", "jd-dm", "jw-rh", "occ", "occ-r", "occ-ri1-wp", "occ-ri2-wp", "rh-comm", "sc")

"building"

v_phone <- "\\b(\\d{3}( )?)?\\d{3} \\d{4}\\b(?! days)"
v_date <- "\\b\\d{1,2} \\d{1,2}( \\d{2,4})?\\b"
v_sr <- "\\b((see|ref(er)? to|duplicate|dupe|active) )?(of )?((s( )?r|service( request)?) )?(number )?\\d{7,8}\\b"

lut_typos <- tibble(
  input = c(
    "attemp\\b",
    "accrptable",
    "addrsss",
    "\\baddres\\b",
    "(?<=this )compliant",
    "in( )?compl[ia]+nce",
    "\\binsuf\\b",
    "insuffient",
    "\\binsp\\b",
    "insp[ect]+[io]+n",
    "informayion",
    "jusitified",
    "montior",
    "st[ru]+(c)?tural",
    "t[enat]{3,}t",
    "tenante(d)?",
    "rrplced",
    "reslove",
    "vo(i)?lation",
    "violstion",
    "\\bv[iola]+tion",
    "viola(t|y)?(i)+on",
    "vuolation",
    "\\bun( )?c[opr]+er[at]+[iv]+e\\b",
    "\\buncooper\\b",
    "\\buc\\b",
    "\\bwete\\b"
  ),
  output = c(
    "attempt",
    "acceptable",
    "address",
    "address",
    "complaint",
    "in compliance",
    "insufficient",
    "insufficient",
    "inspection",
    "inspection",
    "information",
    "justified",
    "monitor",
    "structural",
    "tenant",
    "tenant",
    "replaced",
    "resolve",
    "violation",
    "violation",
    "violation",
    "violation",
    "violation",
    "uncooperative",
    "uncooperative",
    "uncooperative",
    "were"
  )
)

lut_inspectors <- tibble(
  input = c(
    "jn",
    "ma",
    "mc",
    "(e )?blow",
    "(k )?boone",
    "(c )?boykins",
    "b bratten",
    "(d )?burton",
    "(s )?burgess",
    "l busby",
    "(d )?butler",
    "chambers",
    "(c(heryl)? )?clausel",
    "(t )?cobb",
    "a collins",
    "(j )?cooper",
    "a davis",
    "l davis",
    "v ervin",
    "j freeman",
    "(t )?fry",
    "k funches",
    "s gasper",
    "(q )?gilchrest",
    "j golden",
    "d grafton",
    "(r(obert)? )?gray",
    "d hearn",
    "r hill",
    "c higgenbottom",
    "d house",
    "(s )?howell",
    "sh",
    "j hymon",
    "(j(ames)? )?jackson",
    "(m )?james",
    "(a )?knox",
    "r lake",
    "lewis",
    "a lewis",
    "m lewis",
    "w lewis",
    "m middlebrooks",
    "s milan",
    "d mitchell",
    "(v)?( )?moses",
    "r mosley",
    "t neff",
    "r nelson",
    "l(ewis)? nickson",
    "j(udith)? norman",
    "t( )?norwood",
    "(r )?peete",
    "(t )?pegues",
    "(b )?pat(t)?erson",
    "k payne",
    "a powell",
    "n rashada",
    "(s )?rice",
    "t richardson",
    "c sager",
    "b smith",
    "(i )?stig(l)?er",
    "t strong",
    "t swift",
    "sykes",
    "p( )?tyler",
    "(m )?taylor",
    "(b )?thomas",
    "(c )?thom(a)?s",
    "thornton",
    "j( )?tumbrink",
    "(j )?walker",
    "(m )?ward",
    "k washington",
    "a williams",
    "c williams",
    "(r )?witter",
    "(b )?woodland",
    "(v )?woods( jr)?"
  ),
  output = c(
    "JN",
    "ma",
    "mc",
    "E. Blow",
    "K. Boone",
    "c. boykins",
    "b. bratten",
    "D. Burton",
    "s. burgess",
    "l. busby",
    "D. Butler",
    "chambers",
    "c. clausel",
    "T. Cobb",
    "a. collins",
    "J. Cooper",
    "a. davis",
    "l. davis",
    "v. ervin",
    "j. freeman",
    "T. Fry",
    "k. funches",
    "s. gasper",
    "Q. Gilchrest",
    "j golden",
    "d. grafton",
    "R. Gray",
    "d. hearn",
    "R. Hill",
    "c. higgenbottom",
    "d. house",
    "s. howell",
    "s. howell",
    "J. Hymon",
    "j. jackson",
    "m. james",
    "a. knox",
    "R. Lake",
    "lewis",
    "a. lewis",
    "m. lewis",
    "w. lewis",
    "m. middlebrooks",
    "s. milan",
    "d. mitchell",
    "v. moses",
    "r. mosley",
    "t. neff",
    "r. nelson",
    "l. nickson",
    "J. Norman",
    "t. norwood",
    "r. peete",
    "t. pegues",
    "B. Patterson",
    "k. payne",
    "a. powell",
    "n. rashada",
    "S. Rice",
    "T. Richardson",
    "c. sager",
    "b. smith",
    "i. stigler",
    "T. Strong",
    "t. swift",
    "sykes",
    "p. tyler",
    "Taylor",
    "Thomas",
    "Thomas",
    "thornton",
    "J. Tumbrink",
    "J. Walker",
    "M. Ward",
    "k. washington",
    "a. williams",
    "c. williams",
    "R. Witter",
    "b. woodland",
    "v. woods"
  )
)

lut_update_event <- tibble(
  input = c(
    "(at )?((the|this) )?time of ((the|this|my) )?inspection",
    "(during|at|upon|per) ((the|my) )?inspection",
    "at (the )?time of re( )?(inspection|check)",
    "upon (follow( )?up|fu|further) inspection",
    "(after|made)?( )?(1(st)?|one|first) (unsuccessful )?(attempt|try|visit)(s)?( (have been|was|were))?( made)?( to (property|inspect|make contact|address))?( this complaint)?",
    "(after|made)?( )?(2(nd)?|two|second) (unsuccessful )?(attempt|try|visit)(s)?( (have been|was|were))?( made)?( to (property|inspect|make contact|address))?( this complaint)?",
    "(after|made)?( )?(3(rd)?|three|third) (unsuccessful )?(attempt|try|visit)(s)?( (have been|was|were))?( made)?( to (property|inspect|make contact|address))?( this complaint)?"
  ),
  output = c(
    "inspection",
    "inspection",
    "reinspection",
    "reinspection",
    "attempt 1",
    "attempt 2",
    "attempt 3"
  )
)

lut_occupancy <- tibble(
  input = c(),
  output = c()
)

lut_update_tags <- tibble(
  input = c(
    # outside of jurisdiction
    "not in city limits",
    "customer lives in the county",
    # wrong address
    "violation is at another address",
    "wrong (property|parcel|location|house|address|building|apartment)( (address|unit|number))?( given)?",
    "address (doesnt exist|incorrect)",
    # insufficient information
    "unable to find correct address",
    "insufficient (address|information)( (for complaint|to (locate|issue a) violation))?((was)? given)?",
    "need(s)? more information",
    "not enough information",
    # in compliance
    "(the )?property (was )?in( )?compliance( with the city)?( code)?",
    "in compliance",
    # no violation
    "((there|this) (was|were|is) )?no(t a)?( (residential|housing))?( code)?( )?violation(s)?( )?((was|w(h)?ere) )?(found(ed)?|present|observed|existed|seen|noticed)?( at this address)?",
    "(the )?(violation was )?not justified",
    "(the )?violation (did not|didnt) exist",
    "\\bnj\\b",
    "(the )?property (was not|(does not|didnt) appear to be) (substandard )?(and )?derelict( structure)?",
    "all is ok",
    "there was no valid complaint",
    # violation corrected
    "((all|the) )?(code )?(issue|violation|problem)(s)? (have|has|had|is|was)?( )?(been|w(h)?ere|was)?( )?(resolve(d)?|corrected|taken care of|no longer exists|in compliance|fixed|addressed|repaired)",
    "these repairs were done",
    # yard clean
    "(the )?(yard|grass) ((is|was) )?(cut and )?clean(ed)?",
    "(the )?(yard|grass) (is|has been|was( being)?)?( )?cut",
    "cvoyc",
    # property secured
    "((the|this) )?(property|house|structure|home) (is now|ha[ds] been|was)?( )?secure(d)?",
    # property boarded
    "((the|this) )?(property|house|structure) (is( now)?|ha[ds] been|was)?( )?board(ed)?( up)?( and secure(d)?)?",
    # property repaired
    "(the )?property (was|has been)?( )?(repaired|rehabbed)",
    "((the|all) )?repair(s)? (was|w(h)?ere|(have|had) been)?( )?(made|completed)( to)?(( this)? (unit|structure|property))?",
    # property demolished
    "((property|house) )?demolished( by the owner)?",
    # property vacant
    "vacant (property|(town )?house|home|apt|apartment|unit)",
    "unit ((is|was) )?(now )?vacant",
    "((the|this) )?(property|house|structure) (is (now)?|ha[ds] been|was)?( )?vacant",
    # property occupied
    "((the|this) )?(property|house|structure) (is (now)?|ha[ds] been|was)?( )?occupied",
    # vacant lot
    "(this )?(is )?(a )?vacant lot",
    # tenant moved
    "(the )?(tenant|renter|resident)(s)? ((has|have|is|had) )?(mov(ed|ing)( out)?|vacated|no longer ((lives )?at|occup(ies|y)))(( from)? ((of )?the|to another|(of )?this) (unit|property|address))?",
    # no one home
    "no( )?one (was )?home",
    "\\bnoh\\b",
    # no access to property
    "could not access the property",
    # no response
    "no (one )?(response|answer(ed)?|came to this door)( the phone)?",
    # uncooperative
    "uncooperative",
    # no further action
    "no further actions will be taken at this time",
    # issued notice
    "a violation order was issued to the owner",
    # issued notice to correct
    "(a )?(final )?notice to correct (was )?issued",
    # issued notice to rehab
    "issued notice to rehab property",
    "wrote up property (to be|for) rehabbed",
    "wrote property up (to be|for) rehabbed",
    # issued notice to condemn
    "wrote up property (to be in|for) condemnation",
    # will monitor
    "will (continue to )?monitor (the )?property",
    "will monitor for structural violations",
    # will reissue
    "will (re)?( )?(create|issue) new sr",
    "will (be )?re( )?issue(d)?( )?(for )?(at a later date|proper service)?",
    # cancelled
    "this sr is cancelled",
    # active npa case
    "active npa case",
    # court order
    "(with a )?court order",
    # shelby county property
    "unincorporated",
    "property is (a )?sh[le]+by county (owned )?(property)?",
    "sh[le]+by county (owned )?(property)?",
    # duplicate
    "justified active( open violation)?",
    "\\bja\\b",
    # test
    "test sr.*",
    "training( purposes)?",
    # error
    "created by mistake",
    "(created|written( up)?|placed) in error",
    "wrong (ticket|type|request( type)?)( (of|for) code)?",
    "when system comes back online"
  ),
  output = c(
    "outside of jurisdiction",
    "outside of jurisdiction",
    "wrong address",
    "wrong address",
    "wrong address",
    "insufficient information",
    "insufficient information",
    "insufficient information",
    "insufficient information",
    "in compliance",
    "in compliance",
    "no violation",
    "no violation",
    "no violation",
    "no violation",
    "no violation",
    "no violation",
    "no violation",
    "violation corrected",
    "violation corrected",
    "yard clean",
    "yard clean",
    "yard clean",
    "property secured",
    "property boarded",
    "property repaired",
    "property repaired",
    "property demolished",
    "property vacant",
    "property vacant",
    "property vacant",
    "property occupied",
    "vacant lot",
    "tenant moved",
    "no one home",
    "no one home",
    "no access to property",
    "no response",
    "uncooperative",
    "no further action",
    "issued notice",
    "issued notice to correct",
    "issued notice to rehab",
    "issued notice to rehab",
    "issued notice to rehab",
    "issued notice to condemn",
    "will monitor",
    "will monitor",
    "cancelled",
    "active npa case",
    "court order",
    "will reissue",
    "will reissue",
    "shelby county property",
    "shelby county property",
    "shelby county property",
    "duplicate",
    "duplicate",
    "test",
    "test",
    "error",
    "error",
    "error",
    "error"
  )
)
