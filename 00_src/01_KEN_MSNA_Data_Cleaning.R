library(cleaningtools)
library(tidyverse)
library(readxl)
library(openxlsx)
library(ImpactFunctions)
library(robotoolbox)
library(healthyr)

date_to_filter <- "2025-05-20"
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load all the data
# ──────────────────────────────────────────────────────────────────────────────

asset_id <- "a7Ex9v6jKQT4aHtL9ag8A2"

raw_kobo <- ImpactFunctions::get_kobo_data(asset_id = asset_id, un = "alex_stephenson")

raw_kobo_data <- raw_kobo %>%
  pluck("main") %>%
  select(-uuid) %>%
  dplyr::rename(uuid =`_uuid`,
                index = `_index`) %>%
  distinct(uuid, .keep_all = T) %>%
  mutate(across(ends_with("_other"), as.character))

roster_uuids <- data.frame(
  name = names(raw_kobo)[-1],
  uuids = c("person_id", "prot_civil_status_parent_instance_name","shock_instance_name","health_instance_name", "vaccine_instance_name" , "nut_instance_name", "nut_instance_name_2")
)

# Define a function that processes each element
process_roster <- function(name, uuids) {
  raw_kobo[[name]] %>%
    rename(index = `_parent_index`,
           roster_index = `_index`) %>%
    mutate(uuid = .data[[uuids]]) %>%  # .data pronoun handles dynamic column name
    distinct(uuid, .keep_all = TRUE)
}

roster_outputs <- purrr::pmap(roster_uuids, process_roster)

names(roster_outputs) <- roster_uuids$name




list_all_data %>%
  write_rds(., "03_output/01_raw_data/all_raw_data.rds")

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print("~~~~~~There are multiple versions of the tool in use~~~~~~")
}


kobo_tool_name <- "04_tool/REACH_KEN_2025_MSNA_Tool_final.xlsx"
questions <- read_excel(kobo_tool_name, sheet = "survey")
choices <- read_excel(kobo_tool_name, sheet = "choices")

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  select(location, fo_in_charge = FO_In_Charge)

# # join the fo to the dataset
data_with_fo <- raw_kobo_data %>%
  left_join(fo_district_mapping, by = join_by("camp" == "location"))


# ──────────────────────────────────────────────────────────────────────────────
# 2. Filter for time and export deleted surveys
# ──────────────────────────────────────────────────────────────────────────────

mindur <- 25
maxdur <- 120

set.seed(123)
data_with_time <- data_with_fo %>%
  mutate(interview_duration = runif(nrow(data_with_fo), 20, 130))

kobo_data_metadata <- get_kobo_metadata(dataset = data_with_fo, un = "alex_stephenson", asset_id = "antAdT3siLrnjTdfcdYcFY", remove_geo = T)

data_with_time <- kobo_data_metadata$df_and_duration

raw_metadata_length <- kobo_data_metadata$audit_files_length

write_rds(raw_metadata_length, "03_output/01_raw_data/raw_metadata.rds")


data_in_processing <- data_with_time %>%
  mutate(length_valid = case_when(
    interview_duration < mindur ~ "Too short",
    interview_duration > maxdur ~ "Too long",
    TRUE ~ "Okay"
  ))


deletion_log <- data_in_processing %>%
  filter(length_valid != "Okay") %>%
  select(uuid, length_valid, camp, enum_id, interview_duration) %>%
  left_join(raw_kobo_data %>% select(uuid, index), by = "uuid")


deletion_log %>%
  mutate(comment = paste0("Interview length is ", length_valid)) %>%
  select(-length_valid) %>%
  writexl::write_xlsx(., paste0("03_output/02_deletion_log/deletion_log.xlsx"))

## filter only valid surveys and for the specific date
data_valid_date <- data_in_processing %>%
  filter(length_valid == "Okay") %>%
  filter(today == date_to_filter)

main_data <- data_valid_date


# ──────────────────────────────────────────────────────────────────────────────
# 3. GIS Checks
# ──────────────────────────────────────────────────────────────────────────────

gps<-main_data %>% filter(consent=="yes") %>%
  select(uuid,enum_id,today, contains("camp"),contains("sub_camp"),contains("point_number"), contains("check_ptno_insamples"), contains("validate_ptno"), contains("pt_sample_lat"), contains("pt_sample_lon"), contains("dist_btn_sample_collected"), contains("reasons_why_far"), contains("geopoint"))

write.xlsx(gps , paste0("03_output/03_gps/gps_checks_", lubridate::today(), ".xlsx"))


# ──────────────────────────────────────────────────────────────────────────────
# 4. Apply the checks to the main data
# ──────────────────────────────────────────────────────────────────────────────


df_list_logical_checks <- read_csv("02_input/01_logical_checks/check_list.csv")

excluded_questions <- questions %>%
  filter(type != "integer" & type != "calculate") %>%
  pull(name) %>%
  unique()

# intersect
excluded_questions_in_data <- intersect(colnames(main_data), excluded_questions)
outlier_cols_not_4_checking <- main_data %>%
  select(matches("geopoint|gps|_index|_submit|submission|_sample_|^_id$|rand|_calc|_Have|count_|_prop|_n|_score|Emergency|Stress|Crisis|TotalHHEXP6|TotalHHEXP|FCSG|rCSI|enum_id|hh_size|LhCSICategory")) %>%
  colnames()


checked_main_data <-  main_data %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(main_data|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains("."))))%>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(-999,-1)
  ) %>%
  # check for outliers
  check_outliers(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    kobo_survey = questions,
    kobo_choices = choices,
    cols_to_add_cleaning_log = NULL,
    strongness_factor = 1.5,
    minimum_unique_value_of_variable = NULL,
    remove_choice_multiple = TRUE,
    sm_separator = "/",
    columns_not_to_check = c(excluded_questions_in_data ,outlier_cols_not_4_checking)
  ) %>%
  check_logical_with_list(uuid_column = "uuid",
                          list_of_check = df_list_logical_checks,
                          check_id_column = "check_id",
                          check_to_perform_column = "check_to_perform",
                          columns_to_clean_column = "columns_to_clean",
                          description = "description",
                          bind_checks = TRUE)


main_cleaning_log <-  checked_main_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("camp","sub_camp", "today","enum_id")
  )



# ──────────────────────────────────────────────────────────────────────────────
# ~~~ Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────
main_data$fo_in_charge <- 'TEST'

main_to_join <- main_data %>%
  dplyr::select(camp, sub_camp,today,enum_id,resp_gender, enum_gender,
                hoh_gender,fo_in_charge,deviceid,instance_name, index)


trans_roster <- function(data) {
  data %>%
    dplyr::filter(!.data$index %in% deletion_log$index) %>%
    dplyr::filter(.data$index %in% main_data$index) %>%
    dplyr::left_join(main_to_join, by = join_by(`index` == index)) %>%
    dplyr::filter(!is.na(.data$fo_in_charge)) ### THIS WOULD NEED REMOVING
}

roster_outputs <- map(roster_outputs, trans_roster)

purrr::walk2(roster_outputs, roster_uuids$name, ~assign(.y, .x, envir = .GlobalEnv))

# ──────────────────────────────────────────────────────────────────────────────
#  HH Roaster data cleaning
# ──────────────────────────────────────────────────────────────────────────────



