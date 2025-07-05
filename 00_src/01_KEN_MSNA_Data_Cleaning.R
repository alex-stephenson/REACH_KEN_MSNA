# ─────────────────────────────────────────────────────────────────────────────────
# This code uses the Somalia standardised approach for producing clogs, applying
# them and making raw, clean, clogs and dlogs (deletion logs) on a daily basis.
#
# Within the MSNA framework it is more complicated because we have to allow for
# multiple repeat sections within the data. This code extensively uses iterative
# code to achieve this, primarily from the {purrr} library.
#
# The code basically follows these steps
# ➡️ Read in  raw data via the Kobo API, see package {ImpactFunctions} from
# https://github.com/alex-stephenson/ImpactFunctions. The output is dm object,
# where each element in the object is either the main data or a part of the repeat.
# ➡️ The main df is filtered using the metadata duration, also calculated from
# {ImpactFunctions}, creating a deletion log in the process
# ➡️ Produces clogs for each element in the dm object, doing a standardised set of
# transformations in the process.
# Combining all the sets of clogs, and outputting one clog for every repeat, split
# by field officer.
# ─────────────────────────────────────────────────────────────────────────────────


rm(list = ls())

library(cleaningtools, exclude = c('create_xlsx_cleaning_log', 'create_validation_list')) ## we use an updated local version which has drop down for others. See utils
library(tidyverse)
library(readxl)
library(openxlsx)
library(ImpactFunctions)
library(robotoolbox)
library(impactR4PHU)


date_to_filter <- "2025-06-26"
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

source("00_src/00_utils.R")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load all the data
# ──────────────────────────────────────────────────────────────────────────────

geo_ref_data <- readxl::read_excel("02_input/07_geo_reference_data/ken_geo_ref_data.xlsx", sheet = "ward")

geo_admin1 <- geo_ref_data %>%
  distinct(admin_1_pcode, admin_1_name)
geo_admin2 <- geo_ref_data %>%
  distinct(admin_2_pcode, admin_2_name)
geo_admin3 <- geo_ref_data %>%
  distinct(admin_3_pcode, admin_3_name)



kobo_tool_name <- "04_tool/REACH_KEN_2025_MSNA-Tool_v7.xlsx"
questions <- read_excel(kobo_tool_name, sheet = "survey")
choices <- read_excel(kobo_tool_name, sheet = "choices")


asset_id <- "ak5iQFpNGQpXcgGRrpEKjN"

data_file_path <- "02_input/00_data_download/REACH_KEN_2025_MSNA.xlsx"
sheet_names <- readxl::excel_sheets(data_file_path)

raw_kobo <- map(sheet_names, ~ read_excel(data_file_path, sheet = .x, guess_max =  10000))

sheet_names[1] <- "main"
names(raw_kobo) <- sheet_names

#raw_kobo <- ImpactFunctions::get_kobo_data(asset_id = asset_id, un = "alex_stephenson")

#form <- robotoolbox::kobo_form(asset_id)
raw_kobo_data <- raw_kobo %>%
  pluck("main") %>%
  dplyr::rename(uuid =`_uuid`,
                index = `_index`) %>%
  mutate(across(starts_with("_other"), as.character)) %>%
  ### this code is quite ugly but basically just adds the admin names instead of pcodes, and also combines the admin and refugee site into one
  left_join(geo_admin1, by = join_by("admin1" == "admin_1_pcode")) %>%
  mutate(admin_1_camp = admin1) %>%
  left_join(geo_admin2, by = join_by("admin2" == "admin_2_pcode")) %>%
  mutate(admin_2_camp = ifelse(is.na(admin2), refugee_camp, admin_2_name)) %>%
  left_join(geo_admin3, by = join_by("admin3" == "admin_3_pcode")) %>%
  mutate(admin_3_camp = ifelse(is.na(admin3), sub_camp, admin_3_name)) %>%
  relocate(admin_1_camp, .after = admin1) %>%
  relocate(admin_2_camp, .after = admin2) %>%
  relocate(admin_3_camp, .after = admin3) %>%
  mutate(sampling_id = ifelse(camp_or_hc == "host_community", admin_2_camp, admin_3_camp)) %>%
  mutate(sampling_id =
           case_when(sampling_id == "Loiyangalani Sub County" ~ "Laisamis Sub County",
                     sampling_id == "Marsabit South Sub County" ~ "Laisamis Sub County",
                     sampling_id == "Marsabit North Sub County" ~ "North Horr Sub County",
                     sampling_id == "Sololo Sub County" ~ "Moyale Sub County",
                     sampling_id == "Marsabit Central Sub County" ~ "Saku Sub County",
                     TRUE ~ sampling_id))

# here we will do some transformation of the repeat sections
source("00_src/00_coerce_columns.R")

roster_uuids <- tibble(
  name         = names(raw_kobo)[-1],
  uuids        = c("person_id","edu_uuid","health_uuid","nut_uuid"),
  repeat_table = names(raw_kobo)[-1]
)

# Define a function that processes each element
process_roster <- function(name, uuids, repeat_table) {
  child_names <- repeat_fields[[ repeat_table ]]
  raw_kobo[[ name ]] %>%
    rename(
      index        = `_parent_index`,
      roster_index = `_index`
    ) %>%
    mutate(uuid = .data[[ uuids ]]) %>%
    distinct(uuid, .keep_all = TRUE) %>%
    filter_by_survey(child_names)
}

roster_outputs <- pmap(
  roster_uuids,
  process_roster
)

names(roster_outputs) <- roster_uuids$name

roster_outputs[['main']] <- raw_kobo_data

roster_outputs %>%
  write_rds(., "03_output/01_raw_data/all_raw_data.rds")

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print("~~~~~~There are multiple versions of the tool in use~~~~~~")
}


# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  rename(fo_in_charge = FO_In_Charge)

# # join the fo to the dataset
data_with_fo <- raw_kobo_data %>%
  left_join(fo_district_mapping %>% select(-admin_1_name), by = join_by("admin1" == "admin_1_pcode"))

data_with_fo <- data_with_fo   %>%
  filter(!is.na(fo_in_charge))

# ──────────────────────────────────────────────────────────────────────────────
# 2. Filter for time and export deleted surveys
# ──────────────────────────────────────────────────────────────────────────────

mindur <- 19
maxdur <- 150


kobo_data_metadata <- get_kobo_metadata(dataset = data_with_fo, un = "alex_stephenson", asset_id = asset_id, remove_geo = T)
data_with_time <- kobo_data_metadata$df_and_duration
raw_metadata_length <- kobo_data_metadata$audit_files_length
write_rds(raw_metadata_length, "03_output/01_raw_data/raw_metadata.rds")


data_in_processing <- data_with_time %>%
  mutate(length_valid = case_when(
    interview_duration <= mindur ~ "Too short",
    interview_duration >= maxdur ~ "Too long",
    TRUE ~ "Okay"
  )) %>%
  mutate(length_valid = ifelse(interview_duration > 15 & interview_duration < 20 & hh_size < 5, "Okay", length_valid),
         length_valid = ifelse(interview_duration > 10 & interview_duration < 15 & hh_size < 2, "Okay", length_valid))

remove_deletions <- readxl::read_excel("02_input/08_remove_deletions/remove_deletions.xlsx")

deletion_log <- data_in_processing %>%
  filter(length_valid != "Okay") %>%
  select(uuid, length_valid, admin1, admin_2_camp, admin_3_camp, enum_id, interview_duration) %>%
  left_join(raw_kobo_data %>% select(uuid, index), by = "uuid") %>%
  filter(! uuid %in% remove_deletions$uuid)


deletion_log %>%
  mutate(comment = paste0("Interview length is ", length_valid)) %>%
  select(-length_valid) %>%
  writexl::write_xlsx(., paste0("03_output/02_deletion_log/deletion_log.xlsx"))

## filter only valid surveys and for the specific date
data_valid_date <- data_in_processing %>%
  mutate(length_valid = case_when(
    uuid %in% remove_deletions$uuid ~ "Okay",
    TRUE ~ length_valid
  )) %>%
 filter(length_valid == "Okay") %>%
  filter(today == max(today))

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

## calculate other questions
others_to_check <- questions %>%
  filter(type == "text",
         str_detect(name, "other")) %>%
  pull(name)


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
    columns_to_check = names(main_data %>% select(contains(others_to_check)))) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(-999,-1)
  ) %>%
   check_outliers(
     uuid_column = "uuid",
     element_name = "checked_dataset",
     kobo_survey = questions,
     kobo_choices = choices,
     cols_to_add_cleaning_log = NULL,
     strongness_factor = 4,
     minimum_unique_value_of_variable = 5,
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


roster_outputs$main <- NULL
roster_outputs_trans <- map(roster_outputs, trans_roster)

purrr::walk2(roster_outputs_trans, roster_uuids$name, ~assign(.y, .x, envir = .GlobalEnv))

# ──────────────────────────────────────────────────────────────────────────────
#  5.1 HH Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

excluded_questions_in_data <- intersect(colnames(roster), excluded_questions)

# Define the pattern for columns you want to exclude
exclude_patterns <- c("geopoint", "gps", "_index", "_submit", "submission", "_sample_", "^_id$", "^rand", "^_index$","_n","enum_id", "ind_potentially_hoh")

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
    strongness_factor = 4,
    minimum_unique_value_of_variable = 5,
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
  ) %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(health_ind %>% select(contains(others_to_check)))
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
                                                 iycf_6b = "drink_formula_yn", # infant formula (y/n)
                                                 iycf_6c = "drink_milk_yn", # milk from animals, fresh tinned powder (y/n)
                                                 iycf_6d = "drink_yoghurt_yn", # yoghurt drinks (y/n)
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
  mutate(across(starts_with("other_"), as.numeric)) %>%
  impactR4PHU::check_iycf_flags(.dataset = .,
                              age_months = "nut_ind_under5_age_months",
                              iycf_4 = "breastfeeding", # breastfed yesterday during the day or night (y/n)
                              iycf_6a = "drink_water", # plain water
                              iycf_6b = "drink_formula_yn", # infant formula (y/n)
                              iycf_6c = "drink_milk_yn", # milk from animals, fresh tinned powder (y/n)
                              iycf_6d = "drink_yoghurt_yn", # yoghurt drinks (y/n)
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
                              iycf_8 = "times_solid",
                              iycf_6b_num = "drink_formula",
                              iycf_6c_num = "drink_milk",
                              iycf_6d_num = "drink_yoghurt")


excluded_questions_in_data <- intersect(colnames(nut_ind), excluded_questions)

# Define the pattern for columns you want to exclude
exclude_patterns <- c("geopoint", "gps", "_index", "_submit", "submission", "_sample_", "^_id$", "^rand", "^_index$","_n","enum_id", "ind_potentially_hoh")

# Use matches with | to combine patterns
outlier_cols_not_4_checking <- nut_ind %>%
  select(matches(paste(exclude_patterns, collapse = "|"))) %>%
  colnames()


checked_nut_ind_formatted <- nut_ind_formatted %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(nut_ind_formatted %>% select(contains(others_to_check)))) %>%
  check_duplicate(
    uuid_column = "uuid"
  ) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(-999,-1)
  ) %>%
  check_outliers(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    kobo_survey = questions,
    kobo_choices = choices,
    cols_to_add_cleaning_log = NULL,
    strongness_factor = 3,
    minimum_unique_value_of_variable = NULL,
    remove_choice_multiple = TRUE,
    sm_separator = "/",
    columns_not_to_check = c(excluded_questions_in_data,outlier_cols_not_4_checking, "time_breastfeeding_hours")
  )

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
    uuid_column = "uuid"
  )  %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(edu_ind %>% select(contains(others_to_check)))
  ) %>%
  check_logical_with_list(uuid_column = "uuid",
                          list_of_check = df_list_logical_checks_edu,
                          check_id_column = "check_id",
                          check_to_perform_column = "check_to_perform",
                          columns_to_clean_column = "columns_to_clean",
                          description = "description",
                          bind_checks = TRUE) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(-999,-1)
  )

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
  hh_roster_cleaning_log$cleaning_log %>% mutate(clog_type = "roster"),
  health_cleaning_log$cleaning_log %>% mutate(clog_type = "health_ind"),
  child_feeding_cleaning_log$cleaning_log %>% mutate(clog_type = "nut_ind"),
  edu_cleaning_log$cleaning_log %>% mutate(clog_type = "edu_ind"))


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
cleaning_log %>% purrr::map(~ create_xlsx_cleaning_log(.[],
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
                                                                      use_others = T,
                                                                      sm_dropdown_type = "numerical",
                                                                      kobo_survey = questions,
                                                                      kobo_choices = choices,
                                                                      output_path = paste0("01_cleaning_logs/",
                                                                                           unique(.[]$checked_dataset$fo_in_charge),
                                                                                           "/",
                                                                                           "final_cleaning_log_",
                                                                                           unique(.[]$checked_dataset$fo_in_charge),
                                                                                           "_",
                                                                                           date_time_now,
                                                                                           ".xlsx")))



