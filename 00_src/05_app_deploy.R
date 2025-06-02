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
sampling_frame <- readr::read_csv("02_input/03_sampling/sampling_frame.csv") %>%
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


### FO Data
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  select(admin1, camp_or_hc, "fo" = FO_In_Charge)

message("----------DATA SUCCESSFULLY LOADED-----------------")

clean_data <- clean_data %>%
  left_join(fo_district_mapping, by = join_by(camp_or_hc, admin1)) %>%
  mutate(admin_2_camp = ifelse(is.na(admin2), refugee_camp, admin2),
         admin_3_camp = ifelse(is.na(admin3), sub_camp, admin3))


#--------------------------------------------------------
# Admin level Completion
#--------------------------------------------------------


interview_count <- clean_data %>%
  count(admin1, admin_2_camp, name = "Surveys_Done")

KIIs_Done <- sampling_frame %>%
  rename(Surveys_Target = total,
         admin_2_camp = location_code) %>%
  left_join(interview_count) %>%
  select(admin_2_camp, Surveys_Done, Surveys_Target) %>%
  mutate(Complete = ifelse(Surveys_Done >= Surveys_Target, "Yes", "No"))

KIIs_Done %>%
  writexl::write_xlsx(., "02_input/06_dashboard_inputs/completion_report.xlsx")

#--------------------------------------------------------
# Site level Completion
#--------------------------------------------------------

sampling_df_site <- site_data %>%
  count(name, `label::Somali`, district, operational_zone, name = "Total_Surveys") %>%
  left_join(fo_district_mapping) %>%
  select(-district_name) %>%
  rename(site_name = `label::Somali`)

idp_count_site <- clean_data %>%
  mutate(district = tolower(district)) %>%
  count(settlement, name = "Surveys_Done")

sites_complete <- sampling_df_site %>%
  left_join(idp_count_site, by = join_by("name" == "settlement")) %>%
  mutate(Complete = ifelse(Surveys_Done >= Total_Surveys, "Yes", "No"))

sites_complete %>%
  writexl::write_xlsx(., paste0("03_output/09_completion_report/site_level_completion_report_", today(), ".xlsx"))


## Completion by FO

completion_by_FO <- KIIs_Done %>%
  group_by(fo) %>%
  summarise(total_surveys = sum(Total_Surveys, na.rm = T),
            total_done = sum(Surveys_Done, na.rm = T)) %>%
  mutate(Completion_Percent = round((total_done / total_surveys) * 100, 1)) %>%
  mutate(Completion_Percent = ifelse(Completion_Percent > 100, 100, Completion_Percent))

completion_by_FO %>%
  writexl::write_xlsx("03_output/10_dashboard_output/completion_by_FO.xlsx")

### OPZ burndown

total_tasks <- sum(KIIs_Done$Total_Surveys)

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
  write_csv(., "03_output/10_dashboard_output/actual_burndown.csv")


### enumerator performance

deleted_data <- all_dlogs %>%
  count(enum_code, name = "deleted") %>%
  mutate(enum_code = as.character(enum_code))

valid_data <- clean_data %>%
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
  group_by(fo, enum_code, today, district) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  group_by(fo, enum_code, district) %>%
  summarise("Average per day" = round(mean(n))) %>%
  mutate(enum_code = as.character(enum_code))

enum_performance <- enum_performance %>%
  left_join(mean_per_day) %>%
  select(fo, enum_code, district, valid, deleted, total, pct_valid, `Average per day`)

enum_performance %>%
  write_csv(., "03_output/10_dashboard_output/enum_performance.csv")




###########################

deploy_app_input <- list.files(full.names = T, recursive = T) %>%
  keep(~ str_detect(.x, "03_output/10_dashboard_output/enum_performance.csv")
       | str_detect(.x, "03_output/10_dashboard_output")
       | str_detect(.x, "app.R")

  )

rsconnect::deployApp(appFiles =deploy_app_input,
                     appDir = ".",
                     appPrimaryDoc = "./src/app.R",
                     appName = "REACH_SOM_HSM_Field_Dashboard",
                     account = "impact-initiatives")
