
library(tidyverse)



clean_data <- read_rds("03_output/06_final_cleaning_log/final_all_r_object.rds")


#1
age <- clean_data$main$my_clean_data_final %>%
  filter(resp_age > 79)
#2
hoh_age <- clean_data$main$my_clean_data_final %>%
  filter(resp_hoh_yn == "yes" & resp_age < 20)

#3
ages_match <- clean_data$main$my_clean_data_final %>%
  filter(resp_gender == hoh_gender & resp_age == hoh_age & resp_hoh_yn == "no")

#4
setting_issue <- clean_data$main$my_clean_data_final %>%
  filter(setting %in% c("urban", "rural") & !is.na(refugee_camp))

#5
setting_admin_na <- clean_data$main$my_clean_data_final %>%
  filter(setting %in% c("informal_camp", "formal_camp") & is.na(refugee_camp))

#6
aap_no_challenge <- clean_data$main$my_clean_data_final %>%
  filter(`aap_priority_challenge/none` == 1 & (fsl_hhs_sleephungry == "yes" | fsl_hhs_alldaynight == "yes"))

#7
challenge_check <- clean_data$main$my_clean_data_final %>%
  filter(
    `aap_priority_challenge/none` == 1 &
      if_any(
        c(
          fsl_lcsi_crisis1,
          fsl_lcsi_crisis2,
          fsl_lcsi_crisis3,
          fsl_lcsi_emergency1,
          fsl_lcsi_emergency2
        ),
        ~ .x == "yes"
      )
  )


#8
food_check <- clean_data %>%
  filter(str_detect(aap_priority_challenge, "food") & rowSums(select(., c(fsl_lcsi_stress1,fsl_lcsi_stress2,
                          fsl_lcsi_stress3,fsl_lcsi_stress4, fsl_lcsi_crisis1,fsl_lcsi_crisis2, fsl_lcsi_crisis3, fsl_lcsi_emergency1,
                          fsl_lcsi_emergency2)) == "yes") >= 4)

#9
support_food_check <- clean_data  %>%
  filter(str_detect(aap_priority_support_ngo, "food") &
      rowSums(select(., c(
        fsl_lcsi_stress1,
        fsl_lcsi_stress2,
        fsl_lcsi_stress3,
        fsl_lcsi_stress4,
        fsl_lcsi_crisis1,
        fsl_lcsi_crisis2,
        fsl_lcsi_crisis3,
        fsl_lcsi_emergency1,
        fsl_lcsi_emergency2
      )) == "yes") >= 3
  ) ## returns 156 rows

#10
no_support_fsl_problem <- clean_data$main$my_clean_data_final %>%
  filter(
    str_detect(aap_priority_support_ngo, "food") &
      if_all(
        c(fsl_hhs_nofoodhh,
          fsl_hhs_sleephungry,
          fsl_hhs_alldaynight),
        ~ .x == "yes")
  )

#11
fcs_score_high <- clean_data$main$my_clean_data_final %>%
  filter(
    fsl_fcs_score > 70 &
      if_all(
        c(
          fsl_hhs_nofoodhh,
          fsl_hhs_sleephungry,
          fsl_hhs_alldaynight
        ),
        ~ .x == "yes"
      )
  )

#12
crisis_no_need <- clean_data %>%
  filter(
    fsl_fcs_cat == "Poor" &
      if_all(
        c(
          fsl_lcsi_crisis1,
          fsl_lcsi_crisis2,
          fsl_lcsi_crisis3,
          fsl_lcsi_emergency1,
          fsl_lcsi_emergency2,
          fsl_lcsi_emergency3,
          fsl_lcsi_stress1,
          fsl_lcsi_stress2,
          fsl_lcsi_stress3,
          fsl_lcsi_stress4
        ),
        ~ .x == "no_had_no_need"
      ) & camp_or_hc == "host_community"
  )

fcs_cat_hhs_cat <- clean_data$main$my_clean_data_final %>%
  rowwise() %>%
  filter(
    (fsl_fcs_cat == "Acceptable") &
      hhs_cat_ipc %in% c('Severe', 'Very Severe')) ## 22 rows

rcs_cat_hhs_cat <- clean_data$main$my_clean_data_final %>%
  rowwise() %>%
  filter(
    (rcsi_cat == "No to Low") &
      hhs_cat_ipc %in% c('Severe', 'Very Severe', 'Medium')) ## 600 rows

fcs_cat_lcsi_cat <- clean_data$main$my_clean_data_final %>%
  rowwise() %>%
  filter(
    (fsl_fcs_cat == "Acceptable") &
      lcsi_cat %in% c('Emergency', 'Crisis')) ## returns 600 rows




joined_checks <- rbind(age, hoh_age) %>%
  rbind(., ages_match) %>%
  rbind(., setting_issue) %>%
  rbind(., setting_admin_na) %>%
  rbind(., aap_no_challenge) %>%
  rbind(., challenge_check) %>%
  rbind(., food_check) %>%
  rbind(., support_food_check) %>%
  rbind(., no_support_fsl_problem) %>%
  rbind(., fcs_score_high) %>%
  rbind(., crisis_no_need)

joined_checks %>% count(uuid) %>% filter(n>1)




#### produce cleaning logs

library(readr)
library(cleaningtools)
check_list_fsl <- read_csv("02_input/01_logical_checks/check_list_fsl.csv")

crisis_no_need <- check_list_fsl %>% filter(check_id == "crisis_no_need")

clean_data <- clean_data$main$my_clean_data_final

clog_fsl_checks <- clean_data %>%
  check_logical_with_list(uuid_column = "uuid",
                                         list_of_check = check_list_fsl,
                                         check_id_column = "check_id",
                                         check_to_perform_column = "check_to_perform",
                                         columns_to_clean_column = "columns_to_clean",
                                         description = "description",
                                         bind_checks = TRUE)

crisis_no_needs_checks <- clean_data %>%
  check_logical_with_list(uuid_column = "uuid",
                          list_of_check = crisis_no_need,
                          check_id_column = "check_id",
                          check_to_perform_column = "check_to_perform",
                          columns_to_clean_column = "columns_to_clean",
                          description = "description",
                          bind_checks = TRUE)


clog_fsl_clog <-  clog_fsl_checks %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp", "today","enum_id", "index")
  )

create_xlsx_cleaning_log(clog_fsl_clog,
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
                         output_path = "fsl_clog.xlsx")

############################


clog_crisis_no_need <-  crisis_no_needs_checks %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp", "today","enum_id", "index")
  )

create_xlsx_cleaning_log(clog_crisis_no_need,
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
                    #    use_others = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = "crisis_no_need.xlsx")


#####
values_check <- clean_data$main$my_clean_data_final %>%
  check_value() %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = c("admin1", "admin_2_camp", "admin_3_camp", "today","enum_id", "index")
  )

create_xlsx_cleaning_log(values_check,
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
                         output_path = "values_check.xlsx")


