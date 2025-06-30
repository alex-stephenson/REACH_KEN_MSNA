# ─────────────────────────────────────────────────────────────────────────────────
# This code uses the Somalia standardised approach for producing clogs, applying
# them and making raw, clean, clogs and dlogs (deletion logs) on a daily basis.
#
# Within the MSNA framework it is more complicated because we have to allow for
# multiple repeat sections within the data. This code extensively uses iterative
# code to achieve this, primarily from the {purrr} library.
#
# The code basically follows these steps
# ➡️ Read in  raw data, clogs and deletion logs
# ➡️ splits the raw data and clogs into a list of dataframes, where we have
# one element within the list for each repeat section in the tool, and also a 'main'
# section.
# ➡️ Review each nested cleaning log and data combination, apply the cleaning logs
# and then apply the recreate parent column if there is any select multiple
# questions.
# ➡️ The created clogs are also reviewed on a daily basis. Again this happens
# itertively.
# ➡️ If there are no clogs identified, most of these steps are skipped.
# ➡️ Finally, all of the outputs outputted, and then can be fed into the field
# monitoring dashboard.
# ─────────────────────────────────────────────────────────────────────────────────



rm(list = ls())
### post collection checks

library(tidyverse)
library(cleaningtools)
library(readxl)
library(ImpactFunctions)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Read in all data (raw, clogs, FO, tool)
# ──────────────────────────────────────────────────────────────────────────────


# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  rename(fo_in_charge = FO_In_Charge)

## raw data
all_raw_data <- read_rds("03_output/01_raw_data/all_raw_data.rds")


## tool
kobo_tool_name <- "04_tool/REACH_KEN_2025_MSNA-Tool_v7.xlsx"
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")


# Read in all the clogs
file_list <- list.files(path = "01_cleaning_logs", recursive = TRUE, full.names = TRUE)

file_list <- file_list %>%
  keep(~ str_detect(.x, "complete"))

#file_list <- file_list %>%
#  keep(~ str_detect(.x, "Complete"))

# Function to read and convert all columns to character
read_and_clean <- function(file, sheet) {
  read_excel(file, sheet = sheet) %>%
    mutate(across(everything(), as.character)) %>%   # Convert all columns to character
    mutate(file_path = file)
}

# ──────────────────────────────────────────────────────────────────────────────
# 2. Split the code - the following process runs if we have identified any clogs.
#    If there are no clogs, then most of this gets skipped and instead we just
#    produce a raw and clean data output using the deletion log.
# ──────────────────────────────────────────────────────────────────────────────


if(! is_empty(file_list)) {

  message("Cleaning logs identified, completing full script")

  Sys.sleep(2)

  # Read and combine all files into a single dataframe
  cleaning_logs <- map_dfr(file_list, sheet = 'cleaning_log', read_and_clean)

  cleaning_logs <- cleaning_logs %>%
    filter(! is.na(change_type))

  ## now we split the clogs by the clog type, we manually coded at the end of the last script.

  clogs_split <- cleaning_logs %>%
    group_by(clog_type) %>%
    group_split() %>%
    set_names(map_chr(., ~ unique(.x$clog_type)))


  common_rosters <- intersect(
    names(all_raw_data),
    cleaning_logs$clog_type %>% unique() %>% na.omit()
  )

  list_of_df_and_clog <- map(common_rosters, function(g) {
    raw <- all_raw_data[[g]]
    clog <- clogs_split[[g]] %>% mutate(across(everything(), as.character))

    list(
      raw_data = raw,
      cleaning_log = clog
    )
  })

  names(list_of_df_and_clog) <- common_rosters

  message("✅ Clogs successfully split")
  Sys.sleep(2)

  # Read in all the dlogs
  all_dlogs <- readxl::read_excel(r"(03_output/02_deletion_log/deletion_log.xlsx)", col_types = "text")
  #manual_dlog <- readxl::read_excel("03_output/02_deletion_log/MSNA_2025_Manual_Deletion_Log.xlsx", col_types = "text")

  cleaning_log_deletions <- cleaning_logs %>%
    filter(change_type == "remove_survey") %>%
    mutate(interview_duration = "") %>%
    select(uuid, admin1, admin_2_camp, admin_3_camp, enum_id,interview_duration, index, comment = issue) %>%
    mutate(across(everything(), as.character))

  deletion_log <- bind_rows(all_dlogs, cleaning_log_deletions) %>%
    bind_rows(all_dlogs) %>%
    distinct(uuid, .keep_all = T)


  # ──────────────────────────────────────────────────────────────────────────────
  # 3. Apply the clog to each item in the list of dfs and clogs
  # ──────────────────────────────────────────────────────────────────────────────

  ## first just review the clog against the raw data

  message("Reviewing clogs...")
  Sys.sleep(1)

  clog_review <- purrr::map(list_of_df_and_clog, function(x) {
    clog_review <- cleaningtools::review_cleaning_log(
      raw_dataset = x$raw_data,
      raw_data_uuid_column = "uuid",
      cleaning_log = x$cleaning_log,
      cleaning_log_change_type_column = "change_type",
      change_response_value = "change_response",
      cleaning_log_question_column = "question",
      cleaning_log_uuid_column = "uuid",
      cleaning_log_new_value_column = "new_value")
  })

  message("Updating clogs and recoding others")
  Sys.sleep(1)

  cleaning_log_summaries <- purrr::map(list_of_df_and_clog, function(x) {

    # process the clogs to update the others and remove items from the clogs
    cleaned_log <- x$cleaning_log %>%
      filter(!index %in% deletion_log$index) %>% ## needs to be the index not the UUID because index is the same across all rosters, where as UUID is unique to the roster
      mutate(new_value = as.character(new_value))

    temp_clog_type <- cleaned_log$clog_type[1]

    updated_log <- update_others(kobo_survey, kobo_choice, cleaning_log = cleaned_log) %>%
      mutate(clog_type = temp_clog_type)

    ## remove any dlogs from the raw data
    cleaned_data <- x$raw_data %>%
      filter(!index %in% deletion_log$index)

    ## easy list output
    list(
      pre_raw_data = x$raw_data, ##this is the data without the time deletion removed... the other one isnt really raw.
      cleaning_log = updated_log,
      raw_data = cleaned_data
    )
  })

  ### apply the cleaning log to each items in the list

  message("✅ Creating clean data...")
  Sys.sleep(3)

  clean_data_logs <- purrr::map(cleaning_log_summaries, function(x) {

    message(paste0("creating clean data for: ", x$cleaning_log$clog_type[1]))

    Sys.sleep(5)

    my_clean_data <- create_clean_data(raw_dataset = x$raw_data,
                                       raw_data_uuid_column = "uuid",
                                       cleaning_log = x$cleaning_log,
                                       cleaning_log_uuid_column = "uuid",
                                       cleaning_log_question_column = "question",
                                       cleaning_log_new_value_column = "new_value",
                                       cleaning_log_change_type_column = "change_type")



    message("✅ Successfully created clean data, filtering tool...")
    Sys.sleep(2)

    # Identify relevant select_multiple parent questions in the dataset -- this speeds it up massively
    relevant_sm_parents <- my_clean_data %>%
      names() %>%
      keep(~ str_detect(.x, "/")) %>%
      str_remove("/[^/]+$") %>%
      unique()

    # Filter kobo_survey for relevant select_multiple questions
    relevant_kobo_survey <- kobo_survey %>%
      filter(str_detect(type, "select_multiple")) %>%
      filter(name %in% relevant_sm_parents)

    # Extract relevant list_names and filter kobo_choices
    relevant_list_names <- relevant_kobo_survey %>%
      pull(type) %>%
      str_remove("select_multiple ") %>%
      unique()

    relevant_kobo_choices <- kobo_choice %>%
      filter(list_name %in% relevant_list_names)

    if(nrow(relevant_kobo_survey != 0)) {

      message(paste0("✅ Successfully filtered tool, now making parent cols for: ", x$cleaning_log$clog_type[1]))
      Sys.sleep(8)


      my_clean_data_parentcol <- recreate_parent_column(dataset = my_clean_data,
                                                        uuid_column = "uuid",
                                                        kobo_survey = relevant_kobo_survey,
                                                        kobo_choices = relevant_kobo_choices,
                                                        sm_separator = "/",
                                                        cleaning_log_to_append = x$cleaning_log)

      message(paste0("✅ Processed: ", x$cleaning_log$clog_type[1]))
      Sys.sleep(0.5)

      return(list(
        raw_data = x$pre_raw_data %>% utils::type.convert(as.is = TRUE),
        my_clean_data_final = my_clean_data_parentcol$data_with_fix_concat,
        cleaning_log = my_clean_data_parentcol$cleaning_log,
        deletion_log = deletion_log
      ))

    } else {

      message(paste0("✅ No select multiple questions for ", x$cleaning_log$clog_type[1], " returning existing clog."))
      Sys.sleep(8)

      return(list(
        raw_data = x$pre_raw_data %>% utils::type.convert(as.is = TRUE),
        my_clean_data_final = my_clean_data,
        cleaning_log = x$cleaning_log,
        deletion_log = deletion_log
      ))

    }
  })


  ### combine the clog together

  all_cleaning_logs <- purrr::map(clean_data_logs, function(x){
    cleaning_log <- x$cleaning_log
  })

  all_cleaning_logs <- bind_rows(all_cleaning_logs)


} else {


  message("No cleaning logs provided... Making clean data just from deletion logs.")


  # Read in all the dlogs
  all_dlogs <- readxl::read_excel(r"(03_output/02_deletion_log/deletion_log.xlsx)", col_types = "text")
  #manual_dlog <- readxl::read_excel(r"(03_output/02_deletion_log/MSNA_2025_Manual_Deletion_Log.xlsx)", col_types = "text")
  deletion_log <- bind_rows(all_dlogs) %>%
    distinct(uuid, .keep_all = T)

  message("✅ Outputted clean Data")


  all_raw_data$main %>%
    filter(! index %in% deletion_log$index) %>%
    writexl::write_xlsx(., "03_output/05_clean_data/final_clean_main_data.xlsx")

  message("✅ Outputted raw Data")


  all_raw_data$main %>%
    writexl::write_xlsx(., "03_output/01_raw_data/raw_data_main.xlsx")

  message("✅ Outputted deletion log")


  deletion_log %>%
    writexl::write_xlsx(., "03_output/02_deletion_log/combined_deletion_log.xlsx")
}

# ──────────────────────────────────────────────────────────────────────────────
# 3. Now review the cleaning and output the clog issues
# ──────────────────────────────────────────────────────────────────────────────


if(! is_empty(file_list)) {

  clog_issues <- purrr::map(clean_data_logs, function(x) {


    review_cleaning <- review_cleaning(x$raw_data,
                                       raw_dataset_uuid_column = "uuid",
                                       x$my_clean_data_final,
                                       clean_dataset_uuid_column = "uuid",
                                       cleaning_log = x$cleaning_log,
                                       cleaning_log_uuid_column = "uuid",
                                       cleaning_log_change_type_column = "change_type",
                                       cleaning_log_question_column = "question",
                                       cleaning_log_new_value_column = "new_value",
                                       cleaning_log_old_value_column = "old_value",
                                       cleaning_log_added_survey_value = "added_survey",
                                       cleaning_log_no_change_value = c("no_action", "no_change"),
                                       deletion_log = x$deletion_log,
                                       deletion_log_uuid_column = "uuid",
                                       check_for_deletion_log = T
    )

    review_cleaning <- review_cleaning %>%
      left_join(cleaning_logs %>% select(uuid, file_path) %>% distinct(), by = "uuid")

    review_cleaning = review_cleaning

  })


  writexl::write_xlsx(clog_issues, paste0("01_cleaning_logs/00_clog_review/cleaning_log_review_", lubridate::today(), ".xlsx"))


  ### add indicators to the cleaned main data

  clean_data_logs$main$my_clean_data_final <- clean_data_logs$main$my_clean_data_final %>%
    mutate(fsl_hhs_nofoodhh = ifelse(fsl_hhs_nofoodhh == "dnk" | fsl_hhs_nofoodhh == "pnta", NA, fsl_hhs_nofoodhh),
           fsl_hhs_alldaynight = ifelse(fsl_hhs_alldaynight == "dnk" | fsl_hhs_alldaynight == "pnta", NA, fsl_hhs_alldaynight),
           fsl_hhs_sleephungry = ifelse(fsl_hhs_sleephungry == "dnk" | fsl_hhs_sleephungry == "pnta", NA, fsl_hhs_sleephungry)) %>%
    add_eg_hhs(
      hhs_nofoodhh_1 = "fsl_hhs_nofoodhh",
      hhs_nofoodhh_1a = "fsl_hhs_nofoodhh_freq",
      hhs_sleephungry_2 = "fsl_hhs_sleephungry",
      hhs_sleephungry_2a = "fsl_hhs_sleephungry_freq",
      hhs_alldaynight_3 = "fsl_hhs_alldaynight",
      hhs_alldaynight_3a = "fsl_hhs_alldaynight_freq",
      yes_answer = "yes",
      no_answer = "no",
      rarely_answer = "rarely",
      sometimes_answer = "sometimes",
      often_answer = "often"
    ) %>%
    add_eg_lcsi(
      lcsi_stress_vars = c("fsl_lcsi_stress1", "fsl_lcsi_stress2", "fsl_lcsi_stress3", "fsl_lcsi_stress4"),
      lcsi_crisis_vars = c("fsl_lcsi_crisis1", "fsl_lcsi_crisis2", "fsl_lcsi_crisis3"),
      lcsi_emergency_vars = c("fsl_lcsi_emergency1", "fsl_lcsi_emergency2", "fsl_lcsi_emergency3"),
      yes_val = "yes",
      no_val = "no_had_no_need",
      exhausted_val = "no_exhausted",
      not_applicable_val = "not_applicable") %>%
    add_eg_rcsi(
      rCSILessQlty = "fsl_rcsi_lessquality",
      rCSIBorrow = "fsl_rcsi_borrow",
      rCSIMealSize = "fsl_rcsi_mealsize",
      rCSIMealAdult = "fsl_rcsi_mealadult",
      rCSIMealNb = "fsl_rcsi_mealnb",
      new_colname = "rcsi"
    ) %>%
    add_eg_fcm_phase(
      fcs_column_name = "FCSGName",
      rcsi_column_name = "rcsi_cat",
      hhs_column_name = "hhs_cat_ipc",
      fcs_categories_acceptable = "Acceptable",
      fcs_categories_poor = "Poor",
      fcs_categories_borderline = "Borderline",
      rcsi_categories_low = "No to Low",
      rcsi_categories_medium = "Medium",
      rcsi_categories_high = "High",
      hhs_categories_none = "None",
      hhs_categories_little = "Little",
      hhs_categories_moderate = "Moderate",
      hhs_categories_severe = "Severe",
      hhs_categories_very_severe = "Very Severe"
    )

  # ──────────────────────────────────────────────────────────────────────────────
  # 4. Output everything
  # ──────────────────────────────────────────────────────────────────────────────

  all_cleaning_logs %>%
    writexl::write_xlsx(., "03_output/06_final_cleaning_log/final_agg_cleaning_log.xlsx")

  clean_data_logs %>%
    write_rds(., "03_output/06_final_cleaning_log/final_all_r_object.rds")


  clean_data_logs$main$my_clean_data_final %>%
    writexl::write_xlsx(., "03_output/05_clean_data/final_clean_main_data.xlsx")



  clean_data_logs$main$raw_data %>%
    writexl::write_xlsx(., "03_output/01_raw_data/raw_data_main.xlsx")



  deletion_log %>%
    writexl::write_xlsx(., "03_output/02_deletion_log/combined_deletion_log.xlsx")

}



# ──────────────────────────────────────────────────────────────────────────────
# 5. Review the soft duplicates
# ──────────────────────────────────────────────────────────────────────────────

## read in already approved ones
exclusions <- read_excel("02_input/02_duplicate_exclusions/exclusions.xlsx")

clean_data_logs <- list(main = list(my_clean_data_final = readxl::read_excel("03_output/05_clean_data/final_clean_main_data.xlsx"), raw_data = readxl::read_excel("03_output/01_raw_data/raw_data_main.xlsx")))
my_clean_data_added <- clean_data_logs[[1]]$my_clean_data_final %>%
  left_join(fo_district_mapping)

enum_typos <- my_clean_data_added %>%
  dplyr::count(enum_id) %>%
  filter(n >= 5) %>%
  pull(enum_id)

group_by_enum <- my_clean_data_added %>%
  filter(enum_id %in% enum_typos) %>%
  group_by(enum_id)

soft_per_enum <- group_by_enum %>%
  dplyr::group_split() %>%
  purrr::map(~ check_soft_duplicates(dataset = .,
                                     kobo_survey = kobo_survey,
                                     uuid_column = "uuid",
                                     idnk_value = "dnk",
                                     sm_separator = "/",
                                     log_name = "soft_duplicate_log",
                                     threshold = 5
  )
  )



# recombine the similar survey data
similar_surveys <- soft_per_enum %>%
  purrr::map(~ .[["soft_duplicate_log"]]) %>%
  purrr::map2(
    .y = dplyr::group_keys(group_by_enum) %>% unlist(),
    ~ dplyr::mutate(.x, enum = .y)
  ) %>%
  do.call(dplyr::bind_rows, .)


similar_surveys_with_info <- similar_surveys %>%
  left_join(my_clean_data_added, by = "uuid") %>%
  left_join(my_clean_data_added %>% select(uuid, similiar_survey_date = today), by = join_by("id_most_similar_survey" == "uuid")) %>%
  select(district, idp_hc_code, fo, today, start, end, uuid, issue, enum_name, num_cols_not_NA, total_columns_compared, num_cols_dnk, similiar_survey_date, id_most_similar_survey, number_different_columns) %>%
  filter(! uuid %in% exclusions$uuid & ! id_most_similar_survey %in% exclusions$id_most_similar_survey,
         today == similiar_survey_date)


similar_survey_raw_data <- my_clean_data %>%
  filter(uuid %in% (similar_surveys_with_info$uuid))

similar_survey_export_path <- paste0("03_output/04_similar_survey_check/similar_surveys_", today(), ".xlsx")

# create a workbook with our data

similar_survey_output <- list("similar_surveys" = similar_surveys_with_info, "similar_survey_raw_data" = similar_survey_raw_data)

similar_survey_output %>%
  writexl::write_xlsx(., similar_survey_export_path)




