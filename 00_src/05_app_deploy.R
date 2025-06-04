rm(list = ls())

library(dplyr)
library(ggplot2)
library(readxl)
library(readr)
library(purrr)
library(tidyr)
library(stringr)
library(lubridate)
library(shiny)
library(shinydashboard)
library(reactable)

print("----------PACAKAGES SUCCESSFULLY LOADED-----------------")

### read in relevant data
message("Loading data...")
sampling_frame <- readxl::read_excel("02_input/03_sampling/sampling_frame_final.xlsx") %>%
  janitor::clean_names()

### clean data

message("Loading cleaned data...")

tryCatch({
  clean_data <- readxl::read_excel("03_output/05_clean_data/final_clean_main_data.xlsx")
}, error = function(e) {
  message("❌ Failed to load clean Kobo data: ", e$message)
})

message("successfully loaded clean data")

### deletion log

all_dlogs <- readxl::read_excel("03_output/02_deletion_log/combined_deletion_log.xlsx")

  # read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
    rename(fo = FO_In_Charge)

  # # join the fo to the dataset
clean_data <- clean_data %>%
    left_join(fo_district_mapping %>% select(-admin_1_name), by = join_by("admin1" == "admin_1_pcode"))

clean_data <- clean_data   %>%
    filter(!is.na(fo))

message("----------DATA SUCCESSFULLY LOADED-----------------")



#--------------------------------------------------------
# Admin level Completion
#--------------------------------------------------------


interview_count_a2 <- clean_data %>%
  count(admin_2_camp, name = "Surveys_Done") %>%
  rename(admin_2_name = admin_2_camp) %>%
  mutate(admin_2_name = tolower(admin_2_name))

admin_2_done <- sampling_frame %>%
  left_join(fo_district_mapping %>% select(admin_1_pcode, fo), join_by(admin_1_pcode)) %>%
  mutate(admin_2_name = tolower(admin_2_name)) %>%
  rename(Surveys_Target = sample_size) %>%
  left_join(interview_count_a2) %>%
  select(fo, admin_1_name, admin_2_name, Surveys_Done, Surveys_Target) %>%
  mutate(Complete = ifelse(Surveys_Done >= Surveys_Target, "Yes", "No")) %>%
  mutate(Complete = ifelse(is.na(Complete), "No", Complete))

admin_2_done %>%
  writexl::write_xlsx(., paste0("02_input/06_dashboard_inputs/completion_report.xlsx"))

admin_2_done %>%
  writexl::write_xlsx(., paste0("03_output/07_daily_completion/completion_report_", today(), ".xlsx"))


#--------------------------------------------------------
# Site level Completion
#--------------------------------------------------------


geo_ref_data <- readxl::read_excel("02_input/07_geo_reference_data/ken_geo_ref_data.xlsx", sheet = "ward")

geo_admin3 <- geo_ref_data %>%
  distinct(admin_3_pcode, admin_3_name) %>%
  mutate(admin_3_name = tolower(admin_3_name))

interview_count_a3 <- clean_data %>%
  count(admin_3_camp, name = "Surveys_Done") %>%
  rename(admin_3_name = admin_3_camp) %>%
  mutate(admin_3_name = tolower(admin_3_name))

admin_3_level_completion <- geo_admin3 %>%
  left_join(interview_count_a3)


admin_3_level_completion %>%
  writexl::write_xlsx(., paste0("03_output/07_daily_completion/admin_3_level_completion_", today(), ".xlsx"))

## Completion by FO

completion_by_FO <- admin_2_done %>%
  group_by(fo) %>%
  summarise(total_surveys = sum(Surveys_Target, na.rm = T),
            total_done = sum(Surveys_Done, na.rm = T)) %>%
  mutate(Completion_Percent = round((total_done / total_surveys) * 100, 1)) %>%
  mutate(Completion_Percent = ifelse(Completion_Percent > 100, 100, Completion_Percent))

completion_by_FO %>%
  writexl::write_xlsx("02_input/06_dashboard_inputs/completion_by_FO.xlsx")

### OPZ burndown

total_tasks <- sum(admin_2_done$Surveys_Target)

actual_burndown <- clean_data %>%
  mutate(today = as.Date(today),
         Day = as.integer(today - min(today)) + 1) %>%  # Calculate day number s
  group_by(Day) %>%  # Group by FO, Region, and District
  summarise(
    Tasks_Completed = n(),  # Count tasks completed on each day
    .groups = "drop"
  ) %>%
  mutate(
    Remaining_Tasks = total_tasks - cumsum(Tasks_Completed)  # Calculate running total
  )

day_zero <- data.frame(Day = 0, Tasks_Completed = 0, Remaining_Tasks = total_tasks)

actual_burndown <- rbind(day_zero, actual_burndown)

actual_burndown %>%
  write_csv(., "02_input/06_dashboard_inputs/actual_burndown.csv")


### enumerator performance

deleted_data <- all_dlogs %>%
  rename(enum_code = enum_id) %>%
  count(enum_code, name = "deleted") %>%
  mutate(enum_code = as.character(enum_code))

valid_data <- clean_data %>%
  rename(enum_code = enum_id) %>%
  count(enum_code, name = "valid") %>%
  mutate(enum_code = as.character(enum_code))

enum_performance <- deleted_data %>%
  full_join(valid_data) %>%
  mutate(valid = replace_na(valid, 0),
         deleted = replace_na(deleted, 0),
         total = deleted + valid,
         pct_valid = round((valid / (deleted + valid)) * 100)) %>%
  filter(total > 5)


mean_per_day <- clean_data %>%
  rename(enum_code = enum_id) %>%
  group_by(fo, enum_code, today) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  group_by(fo, enum_code) %>%
  summarise("Average per day" = round(mean(n))) %>%
  mutate(enum_code = as.character(enum_code))

enum_performance <- enum_performance %>%
  left_join(mean_per_day) %>%
  select(fo, enum_code, valid, deleted, total, pct_valid, `Average per day`)

enum_performance %>%
  write_csv(., "02_input/06_dashboard_inputs/enum_performance.csv")


###########################

deploy_app_input <- list.files(full.names = T, recursive = T) %>%
  keep(~ str_detect(.x, "02_input/06_dashboard_inputs")
       | str_detect(.x, "app.R")
  )

rsconnect::deployApp(appFiles =deploy_app_input,
                     appDir = ".",
                     appPrimaryDoc = "./00_src/04_app.R",
                     appName = "REACH_KEN_2025_MSNA",
                     account = "impact-initiatives")
