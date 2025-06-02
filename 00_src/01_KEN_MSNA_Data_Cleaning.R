rm(list = ls())

library(cleaningtools)
library(tidyverse)
library(readxl)
library(openxlsx)
library(ImpactFunctions)
library(robotoolbox)
library(healthyr)

date_to_filter <- "2025-06-01"
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load all the data
# ──────────────────────────────────────────────────────────────────────────────

asset_id <- "a4kprTX3YyfQXvnko6KTRS"

raw_kobo <- ImpactFunctions::get_kobo_data(asset_id = asset_id, un = "alex_stephenson")

#form <- robotoolbox::kobo_form(asset_id)
raw_kobo_data <- raw_kobo %>%
  pluck("main") %>%
  select(-uuid) %>%
  dplyr::rename(uuid =`_uuid`,
                index = `_index`) %>%
  distinct(uuid, .keep_all = T) %>%
  mutate(across(ends_with("_other"), as.character))

roster_uuids <- data.frame(
  name = names(raw_kobo)[-1],
  uuids = c("person_id", "edu_uuid", "health_uuid", "nut_uuid")
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


roster_outputs %>%
  write_rds(., "03_output/01_raw_data/all_raw_data.rds")

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print("~~~~~~There are multiple versions of the tool in use~~~~~~")
}


kobo_tool_name <- "04_tool/REACH_KEN_2025_MSNA-Tool_v1.xlsx"
questions <- read_excel(kobo_tool_name, sheet = "survey")
choices <- read_excel(kobo_tool_name, sheet = "choices")

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  select(location, fo_in_charge = FO_In_Charge)

# # join the fo to the dataset
data_with_fo <- raw_kobo_data %>%
  mutate(admin_2_camp = ifelse(is.na(admin2), refugee_camp, admin2),
         admin_3_camp = ifelse(is.na(admin3), sub_camp, admin3)) %>%
  left_join(fo_district_mapping, by = join_by("admin1" == "location"))


# ──────────────────────────────────────────────────────────────────────────────
# 2. Filter for time and export deleted surveys
# ──────────────────────────────────────────────────────────────────────────────

mindur <- 25
maxdur <- 120

set.seed(123)
data_with_time <- data_with_fo %>%
  mutate(interview_duration = runif(nrow(data_with_fo), 10, 130))


#kobo_data_metadata <- get_kobo_metadata(dataset = data_with_fo, un = "alex_stephenson", asset_id = "antAdT3siLrnjTdfcdYcFY", remove_geo = T)
#data_with_time <- kobo_data_metadata$df_and_duration
#raw_metadata_length <- kobo_data_metadata$audit_files_length
#write_rds(raw_metadata_length, "03_output/01_raw_data/raw_metadata.rds")


data_in_processing <- data_with_time %>%
  mutate(length_valid = case_when(
    interview_duration < mindur ~ "Too short",
    interview_duration > maxdur ~ "Too long",
    TRUE ~ "Okay"
  ))


deletion_log <- data_in_processing %>%
  filter(length_valid != "Okay") %>%
  select(uuid, length_valid, admin1, admin_2_camp, admin_3_camp, enum_id, interview_duration) %>%
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
  select(uuid,enum_id,today, survey_modality, contains("camp"),contains("sub_camp"),contains("point_number"), contains("check_ptno_insamples"), contains("validate_ptno"), contains("pt_sample_lat"), contains("pt_sample_lon"), contains("dist_btn_sample_collected"), contains("reasons_why_far"), contains("geopoint"))
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
  select(matches("geopoint|gps|_index|_submit|submission|_sample_|^_id$|rand|_calc|_Have|count_|_prop|_n|_score|Emergency|Stress|Crisis|TotalHHEXP6|TotalHHEXP|FCSG|rCSI|enum_id|hh_size|LhCSICategory|interview_duration")) %>%
  colnames()


checked_main_data <-  main_data %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(main_data%>%
                               dplyr::select(starts_with("other_")) %>%
                               dplyr::select(-contains(".")))) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(-999,-1)
  ) %>%
  # check_outliers(
  #   uuid_column = "uuid",
  #   element_name = "checked_dataset",
  #   kobo_survey = questions,
  #   kobo_choices = choices,
  #   cols_to_add_cleaning_log = NULL,
  #   strongness_factor = 3,
  #   minimum_unique_value_of_variable = 10,
  #   remove_choice_multiple = TRUE,
  #   sm_separator = "/",
  #   columns_not_to_check = c(excluded_questions_in_data ,outlier_cols_not_4_checking)
  # ) %>%
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
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp", "today","enum_id", "fo_in_charge", "index")
  )



# ──────────────────────────────────────────────────────────────────────────────
# 5. Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

main_to_join <- main_data %>%
  dplyr::select(admin1, admin_2_camp, admin_3_camp, today,enum_id,resp_gender, enum_gender,
                hoh_gender,fo_in_charge,deviceid,instance_name, index)


trans_roster <- function(data) {
  data %>%
    dplyr::filter(!.data$index %in% deletion_log$index) %>%
    dplyr::filter(.data$index %in% main_data$index) %>%
    dplyr::left_join(main_to_join, by = join_by(index == index))
}

roster_outputs_trans <- map(roster_outputs, trans_roster)

purrr::walk2(roster_outputs_trans, roster_uuids$name, ~assign(.y, .x, envir = .GlobalEnv))

# ──────────────────────────────────────────────────────────────────────────────
#  5.1 HH Roaster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

excluded_questions_in_data <- intersect(colnames(roster), excluded_questions)

# Define the pattern for columns you want to exclude
exclude_patterns <- c("geopoint", "gps", "_index", "_submit", "submission", "_sample_", "^_id$", "^rand", "^_index$","_n","enum_id", "ind_potentially_hoh")

# Use `matches` with `|` to combine patterns
outlier_cols_not_4_checking <- roster %>%
  select(matches(paste(exclude_patterns, collapse = "|"))) %>%
  colnames()

checked_hh_roster <- roster %>%
  check_value(
  uuid_column = "uuid",
  element_name = "checked_dataset",
  values_to_look = c(-999,-1)) %>%
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
    columns_not_to_check = c(excluded_questions_in_data,outlier_cols_not_4_checking)
  )

hh_roster_cleaning_log <-  checked_hh_roster %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp", "today","enum_id", "fo_in_charge", "index")
  )


# ──────────────────────────────────────────────────────────────────────────────
#  5.2 Health Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

df_list_logical_checks_health <- read_csv("02_input/01_logical_checks/check_list_health.csv")

checked_health_data <- health_ind %>%
    check_duplicate(
    uuid_column = "uuid",
    columns_to_check = c("uuid","health_parent_instance")
  ) %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(health_ind%>%
                               dplyr::select(starts_with("other_")) %>%
                               dplyr::select(-contains(".")))


  ) %>%
  # Check for "I don't know" responses in numerical questions
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(-999,-1)
  ) %>%
  check_logical_with_list(uuid_column = "uuid",
                          list_of_check = df_list_logical_checks_health,
                          check_id_column = "check_id",
                          check_to_perform_column = "check_to_perform",
                          columns_to_clean_column = "columns_to_clean",
                          description = "description",
                          bind_checks = TRUE)

health_cleaning_log <-  checked_health_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp","today","enum_id", "fo_in_charge", "index")
  )


# ──────────────────────────────────────────────────────────────────────────────
#  5.2 Child nutrition feeding Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

nut_ind_formatted <- impactR4PHU::add_iycf(.dataset = nut_ind,
                                                 yes_value = "yes",
                                                 no_value = "no",
                                                 dnk_value = "dnk",
                                                 pna_value = "pnta",
                                                 age_months = "nut_ind_under5_age_months",
                                                 iycf_1 = "child_breastfed", # ever breastfed (y/n)
                                                 iycf_2 = "child_first_breastfeeding", # how long the child started breastfeeding after birth
                                                 iycf_4 = "breastfeeding", # breastfed yesterday during the day or night (y/n)
                                                 iycf_5 = "infant_bottlefed", #indicates if the child drink anything from a bottle yesterday
                                                 iycf_6a = "drink_water", # plain water
                                                 iycf_6b = "drink_formula", # infant formula (y/n)
                                                 iycf_6c = "drink_milk", # milk from animals, fresh tinned powder (y/n)
                                                 iycf_6d = "drink_yoghurt", # yoghurt drinks (y/n)
                                                 iycf_6e = "chocolate_drink", # chocolate flavoured drinks, including from syrup / powders (y/n)
                                                 iycf_6f = "juice_drink", # Fruit juice or fruit-flavoured drinks including those made from syrups or powders? (y/n)
                                                 iycf_6g = "soda_drink", # sodas, malt drinks, sports and energy drinks (y/n)
                                                 iycf_6h = "tea_drink", # tea, coffee, herbal drinks (y/n)
                                                 iycf_6i = "broth_drink", # clear broth / soup (y/n)
                                                 iycf_6j = "other_drink", # other liquids (y/n)
                                                 iycf_7a = "yoghurt_food", # yoghurt (NOT yoghurt drinks) (number)
                                                 iycf_7b = "porridge_food",
                                                 iycf_7c = "pumpkin_food", # vitamin a rich vegetables (pumpkin, carrots, sweet red peppers, squash or yellow/orange sweet potatoes) (y/n)
                                                 iycf_7d = "plantain_food", # white starches (plaintains, white potatoes, white yams, manioc, cassava) (y/n)
                                                 iycf_7e = "vegetables_food", # dark green leafy vegetables (y/n)
                                                 iycf_7f = "other_vegetables", # other vegetables (y/n)
                                                 iycf_7g = "fruits", # vitamin a rich fruits (ripe mangoes, ripe papayas) (y/n)
                                                 iycf_7h = "other_fruits", # other fruits (y/n)
                                                 iycf_7i = "liver", # organ meats (liver ,kidney, heart) (y/n)
                                                 iycf_7j = "canned_meat", # processed meats (sausages, hot dogs, ham, bacon, salami, canned meat) (y/n)
                                                 iycf_7k = "other_meat", # any other meats (beef, chicken, pork, goat, chicken, duck) (y/n)
                                                 iycf_7l = "eggs", # eggs (y/n)
                                                 iycf_7m = "fish", # fish (fresh or dried fish or shellfish) (y/n)
                                                 iycf_7n = "cereals", # legumes (beans, peas, lentils, seeds, chickpeas) (y/n)
                                                 iycf_7o = "cheese", # cheeses (hard or soft cheeses) (y/n)
                                                 iycf_7p = "sweet_food", # sweets (chocolates, candies, pastries, cakes) (y.n)
                                                 iycf_7q = "chips", # fried or empty carbs (chips, crisps, french fries, fried dough, instant noodles) (y/n)
                                                 iycf_7r = "other_solid",
                                                 iycf_8 = "times_solid", # times child ate solid/semi-solid foods (number),
                                                 uuid = "uuid") %>%
  mutate(across(starts_with("other_"), as.numeric))



checked_nut_ind_formatted <- nut_ind_formatted %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(nut_ind_formatted%>%
                               dplyr::select(contains("other_nut_ind_under5")) %>%
                               dplyr::select(-contains("."))))

child_feeding_cleaning_log <-  checked_nut_ind_formatted %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp","today","enum_id", "fo_in_charge", "index")
  )


# ──────────────────────────────────────────────────────────────────────────────
#  5.4 Education feeding Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

df_list_logical_checks_edu <- read_csv("02_input/01_logical_checks/check_list_education.csv")

checked_education_data <-  edu_ind %>%
    check_duplicate(
    uuid_column = "uuid",
    columns_to_check = c("uuid","edu_parent_instance")
  )  %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(edu_ind %>%
                               dplyr::select(ends_with("_other")) %>%
                               dplyr::select(-contains(".")))
  ) %>%
  check_logical_with_list(uuid_column = "uuid",
                          list_of_check = df_list_logical_checks_edu,
                          check_id_column = "check_id",
                          check_to_perform_column = "check_to_perform",
                          columns_to_clean_column = "columns_to_clean",
                          description = "description",
                          bind_checks = TRUE)

edu_cleaning_log <-  checked_education_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp","today","enum_id", "fo_in_charge", "index")
  )


# ──────────────────────────────────────────────────────────────────────────────
#  6. Clog output
# ──────────────────────────────────────────────────────────────────────────────


final_clog <- bind_rows(
  main_cleaning_log$cleaning_log %>% mutate(clog_type = "main"),
  hh_roster_cleaning_log$cleaning_log %>% mutate(clog_type = "hh_roster"),
  health_cleaning_log$cleaning_log %>% mutate(clog_type = "health"),
  child_feeding_cleaning_log$cleaning_log %>% mutate(clog_type = "child_feeding"),
  edu_cleaning_log$cleaning_log %>% mutate(clog_type = "education"))


final_clog <- final_clog %>%
  filter(!is.na(fo_in_charge))

final_checked_data <- main_cleaning_log$checked_dataset %>%
  filter(!is.na(fo_in_charge))

# Get distinct group levels from both datasets
common_groups <- intersect(
  final_checked_data$fo_in_charge %>% unique() %>% na.omit(),
  final_clog$fo_in_charge %>% unique() %>% na.omit()
)

# Create named lists keyed by fo_in_charge
checked_split <- final_checked_data %>%
  filter(fo_in_charge %in% common_groups) %>%
  group_split(fo_in_charge, .keep = TRUE) %>%
  set_names(map_chr(., ~ unique(.x$fo_in_charge))) %>%
  keep(~ nrow(.x) > 0)

clog_split <- final_clog %>%
  filter(fo_in_charge %in% common_groups) %>%
  group_split(fo_in_charge, .keep = TRUE) %>%
  set_names(map_chr(., ~ unique(.x$fo_in_charge))) %>%
  keep(~ nrow(.x) > 0)

cleaning_log <- map(common_groups, function(g) {
  checked <- checked_split[[g]]
  clog <- clog_split[[g]]

  list(
    checked_dataset = checked,
    cleaning_log = clog
  )
})

options(openxlsx.na.string = "")
cleaning_log %>% purrr::map(~ cleaningtools::create_xlsx_cleaning_log(.[],
                                                                      cleaning_log_name = "cleaning_log",
                                                                      change_type_col = "change_type",
                                                                      column_for_color = "check_binding",
                                                                      header_front_size = 10,
                                                                      header_front_color = "#FFFFFF",
                                                                      header_fill_color = "#ee5859",
                                                                      header_front = "Calibri",
                                                                      body_front = "Calibri",
                                                                      body_front_size = 10,
                                                                      use_dropdown = T,
                                                                      sm_dropdown_type = "numerical",
                                                                      kobo_survey = questions,
                                                                      kobo_choices = choices,
                                                                      output_path = paste0("01_cleaning_logs/",
                                                                                           unique(.[]$checked_dataset$fo_in_charge),
                                                                                           "/",
                                                                                           "cleaning_log_",
                                                                                           unique(.[]$checked_dataset$fo_in_charge),
                                                                                           "_",
                                                                                           date_time_now,
                                                                                           ".xlsx")))



