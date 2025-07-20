  rm(list = ls())

  library(tidyverse)
  library(cleaningtools)
  library(analysistools)
  library(presentresults)
  library(readxl)
  library(srvyr)
  # ──────────────────────────────────────────────────────────────────────────────
  # 1. Load the data
  # ──────────────────────────────────────────────────────────────────────────────

  kobo_tool_name <- "04_tool/REACH_KEN_2025_MSNA-Tool_v7.xlsx"
  questions <- read_excel(kobo_tool_name, sheet = "survey") %>%
    filter(! `label::english (en)` %in% c("If other, please specify:", "Please specify:") & ! type %in% c("note", "acknowledge")) %>%
    mutate(`label::english (en)` = case_when(
      name == "fsl_hhs_nofoodhh_freq" ~ "How often in the past 4 weeks (30 days), was there ever no food to eat of any kind in your house because of lack of resources to get food?",
      name == "fsl_hhs_sleephungry_freq" ~ "How often in the past 4 weeks (30 days), did you or any household member go to sleep at night hungry because there was not enough food?",
      name == "fsl_hhs_alldaynight_freq" ~ "How often in the past 4 weeks (30 days), did you or any household member go a whole day or night without eating anything at all because there was not enough food?",
      name == "fsl_lcsi_stress1" ~ " In the last 30 days, did your household either sell non-food items that were provided as assistance (camp resident) or spend savings (host community) because of a lack of food or money to buy food?",
      name == "fsl_lcsi_stress2" ~ " In the last 30 days, did your household either sell, share or exchange food rations (camp resident) or sell more aniamls than usual (host community) because of a lack of food or money to buy food?",
      name == "fsl_lcsi_emergency2" ~ "In the last 30 days, did your household engage in socially degrading, high-risk, exploitive or life-threatening jobs or income-generating activities (e.g., smuggling, theft, joining armed groups, prostitution) (camp resident) or sell house or land (host community) because of a lack of food or money to buy food?",
      name == "fsl_lcsi_emergency3" ~ " In the last 30 days, did your household have any female child member (under 15) married off (camp resident) or Sell last female productive animals (host community) because of a lack of food or money to buy food?",
      TRUE ~ `label::english (en)`
      ))
  choices <- read_excel(kobo_tool_name, sheet = "choices") %>%
    filter(`label::english (en)` != "4. A moderate, positive impact")

  kobo_review <- presentresults::review_kobo_labels(questions, choices, "label::english (en)")

  if(nrow(kobo_review) != 0) {
    stop("Review kobo review output")
  }

  # read in all the LOAs
  loa_path <- "02_input/09_loa/main_loa.xlsx"
  sheet_names <- readxl::excel_sheets(loa_path)
  loa <- map(sheet_names, ~ read_excel(loa_path, sheet = .x, guess_max = 10000) %>%
               mutate(group_var = ifelse(group_var == "admin_1_camp", "admin_1_name", group_var)))
  names(loa) <- sheet_names

  # read in sampling frame
  sampling_frame <- read_excel("02_input/03_sampling/sampling_frame_with_pop.xlsx", col_types =
                                 c("text","text","text","text","numeric","numeric" )) %>%
    mutate(sampling_id =
             case_when(sampling_id == "Loiyangalani Sub County" ~ "Laisamis Sub County",
                       sampling_id == "Marsabit South Sub County" ~ "Laisamis Sub County",
                       sampling_id == "Marsabit North Sub County" ~ "North Horr Sub County",
                       sampling_id == "Sololo Sub County" ~ "Moyale Sub County",
                       sampling_id == "Marsabit Central Sub County" ~ "Saku Sub County",
                       TRUE ~ sampling_id)) %>%
    distinct(sampling_id, population) %>%
    mutate(sampling_id = str_squish(sampling_id))


  # load datasets for processing
  # data_file_path <- "02_input/00_data_download/REACH_KEN_2025_MSNA.xlsx"
  # sheet_names <- readxl::excel_sheets(data_file_path)
  # clean_data <- map(sheet_names, ~ read_excel(data_file_path, sheet = .x, guess_max = 10000))
  # sheet_names[1] <- "main"
  # names(clean_data) <- sheet_names

  clean_data <- read_rds("03_output/06_final_cleaning_log/final_all_r_object.rds")

  # ──────────────────────────────────────────────────────────────────────────────
  # 2. Some data processing
  # ──────────────────────────────────────────────────────────────────────────────

  clean_data$main$my_clean_data_final <- clean_data$main$my_clean_data_final %>%
    mutate(sampling_id = str_squish(sampling_id)) %>%
    add_weights(sampling_frame,
                strata_column_dataset = "sampling_id",
                strata_column_sample = "sampling_id",
                population_column = "population") %>%
    select(-matches("geopoint|pt_num|pt_sample|point_number|point_number|threshold_msg|reasons_why_far|final_text"), -dist_btn_sample_collected)

  clean_data$main$raw_data <- clean_data$main$raw_data %>%
    select(-matches("geopoint|pt_num|pt_sample|point_number|point_number|threshold_msg|reasons_why_far|final_text"))

  clean_data$nut_ind$my_clean_data_final <- clean_data$nut_ind$my_clean_data_final %>%
    select(-nut_ind_under5_age_months)

  clean_data$roster$my_clean_data_final <- clean_data$roster$my_clean_data_final %>%
    select(-ind_under5_date, -ind_under5_event, -ind_dob_final,-ind_under5_age_months,
           -ind_under5_age_years,-check_age_under5, -note_child_age_under5_mismatch)



  main_to_join <- clean_data$main$my_clean_data_final  %>%
    dplyr::select(admin_1_camp, admin_1_name, admin_2_camp, admin_2_name, admin_3_camp, sampling_id, camp_or_hc, setting, today,enum_id,resp_gender, enum_gender,
                  hoh_gender,deviceid,instance_name, index, weights)

  clean_data_joined <- imap(clean_data, function(df, name) {
    if (name == "main") {
      df <- df$my_clean_data_final
    } else {
      df$my_clean_data_final %>%
        left_join(main_to_join, by = join_by(index == index))
    }
  })

  # ──────────────────────────────────────────────────────────────────────────────
  # 3. Produce results tables
  # ──────────────────────────────────────────────────────────────────────────────


  clean_data_analysis <- imap(clean_data_joined, function(df, name){

    message(paste0("Producing Results Table for ",  name))
    df <- df %>%
      as_survey_design(strata = "sampling_id", weights = weights) %>%
      create_analysis(., loa = loa [[ name ]], sm_separator = "/")

    results_table <- df$results_table

    message(paste0("Reviewing Kobo Labels for ",  name))

    review_kobo_labels_results <- review_kobo_labels(questions,
                                                     choices,
                                                     label_column = "label::english (en)",
                                                     results_table = results_table)


    message(paste0("Creating Label Dictionary for ",  name))

    label_dictionary <- create_label_dictionary(questions,
                                                choices,
                                                label_column = "label::english (en)",
                                                results_table = results_table)

    message(paste0("Adding label to returns table for ",  name))


    results_table_labeled <- add_label_columns_to_results_table(
      results_table,
      label_dictionary
    )

    message(paste0("Returning as list for ",  name))

    return(list(
      results_tbl = results_table_labeled,
      kobo_review = review_kobo_labels_results
    ))

  } )

  # ──────────────────────────────────────────────────────────────────────────────
  # 4. Output results tables
  # ──────────────────────────────────────────────────────────────────────────────

  walk2(clean_data_analysis, names(clean_data_analysis), function(df, name){

      ### percentage tables
    df_main_analysis_table <- presentresults::create_table_variable_x_group(
        analysis_key = "label_analysis_key",
        results_table = df$results_tbl,
        value_columns = "stat")

    # Replace NA values in list and non-listcolumns with NULL
    df_main_analysis_table <- df_main_analysis_table %>%
      mutate(across(where(is.list), ~ map(.x, ~ ifelse(is.na(.x), list(NULL), .x)))) %>%
      mutate(across(where(~ !is.list(.x) & is.numeric(.x)), ~ replace(.x, is.na(.x), NA))) %>%
      mutate(across(where(~ !is.list(.x) & !is.numeric(.x)), ~ ifelse(is.na(.x), "NA", .x))) %>%
      rename(Overall = `NA`)


    # Export the main analysis percentages table -------------------------
    presentresults::create_xlsx_variable_x_group(
      table_group_x_variable = df_main_analysis_table,
      file_path = paste0(paste0("05_HQ_validation/02_results_tables/", name, "/", name, "_results_table_long_percent.xlsx")),
      value_columns = c("stat","n"),
      overwrite = TRUE
    )

    # Create and process the statistics table (counts: n, N, weighted) ----

    df_stats_table <- presentresults::create_table_variable_x_group(
      results_table = df$results_tbl,
      analysis_key = "label_analysis_key",
      value_columns = c("n")
    )

    # Handle NA values in df_stats_table
    df_stats_table <- df_stats_table %>%
      mutate(across(where(is.list), ~ map(.x, ~ ifelse(is.na(.x), list(NULL), .x)))) %>%
      mutate(across(where(~ !is.list(.x)), ~ ifelse(is.na(.x), "", .x))) %>%
      mutate(across(where(~ !is.list(.x) & !is.numeric(.x)), ~ ifelse(is.na(.x), "NA", .x))) %>%
      rename(Overall = `NA`)

    # Export the processed stats table to Excel
    presentresults::create_xlsx_variable_x_group(
      table_group_x_variable = df_stats_table,  # Use the processed table
      file_path = paste0("05_HQ_validation/02_results_tables/",name, "/", name, "_results_table_long_values.xlsx"),
      value_columns = c("n"),
      overwrite = TRUE
    )
  }
  )





  # ──────────────────────────────────────────────────────────────────────────────
  # 5. Output IPC tables
  # ──────────────────────────────────────────────────────────────────────────────

  loa_ipc <- loa$main %>%
    filter(group_var %in% c(NA, "admin_1_name"))

  ipc_data <- clean_data_joined$main %>%
    filter(camp_or_hc == "host_community") %>%
    mutate(fsl_hhs_nofoodhh_freq = ifelse(is.na(fsl_hhs_nofoodhh_freq), "never", fsl_hhs_nofoodhh_freq),
           fsl_hhs_sleephungry_freq = ifelse(is.na(fsl_hhs_sleephungry_freq), "never", fsl_hhs_sleephungry_freq),
           fsl_hhs_alldaynight_freq = ifelse(is.na(fsl_hhs_alldaynight_freq), "never", fsl_hhs_alldaynight_freq)) %>%
    filter(!is.na(fsl_lcsi_stress4)) %>%
    filter(!is.na(hhs_cat_ipc))

  ipc_data_survey <- ipc_data %>%
    as_survey_design(strata = "sampling_id", weights = weights) %>%
    create_analysis(., loa = loa_ipc, sm_separator = "/")

  IPC_results_table <- ipc_data_survey$results_table

  IPC_label_dictionary <- create_label_dictionary(questions,
                                              choices,
                                              label_column = "label::english (en)",
                                              results_table = IPC_results_table)

  IPC_results_table_labelled <- add_label_columns_to_results_table(
    IPC_results_table,
    IPC_label_dictionary)


IPC_output <- presentresults::create_ipc_table(
      results_table = IPC_results_table,
      analysis_key = "analysis_key",
      dataset = ipc_data,
      cluster_name = "sampling_id",
      fcs_cat_var = "fsl_fcs_cat",
      fcs_cat_values = c("Poor", "Borderline", "Acceptable"),
      fcs_set = c("fsl_fcs_cereal", "fsl_fcs_legumes", "fsl_fcs_veg", "fsl_fcs_fruit",
                  "fsl_fcs_meat", "fsl_fcs_dairy", "fsl_fcs_sugar", "fsl_fcs_oil"),
      hhs_cat_var = "hhs_cat_ipc",
      hhs_cat_values = c("None", "Little", "Moderate", "Severe", "Very Severe"),
      hhs_cat_yesno_set = c("fsl_hhs_nofoodhh", "fsl_hhs_sleephungry", "fsl_hhs_alldaynight"),
      hhs_value_yesno_set = c("yes", "no"),
      hhs_cat_freq_set = c("fsl_hhs_nofoodhh_freq", "fsl_hhs_sleephungry_freq",
                           "fsl_hhs_alldaynight_freq"),
      hhs_value_freq_set = c("rarely", "sometimes", "often", "never"),
      rcsi_cat_var = "rcsi_cat",
      rcsi_cat_values = c("No to Low", "Medium", "High"),
      rcsi_set = c("fsl_rcsi_lessquality", "fsl_rcsi_borrow", "fsl_rcsi_mealsize",
                   "fsl_rcsi_mealadult", "fsl_rcsi_mealnb"),
      lcsi_cat_var = "lcsi_cat",
      lcsi_cat_values = c("None", "Stress", "Crisis", "Emergency"),
      lcsi_set = c("fsl_lcsi_stress1", "fsl_lcsi_stress2", "fsl_lcsi_stress3",
                   "fsl_lcsi_stress4", "fsl_lcsi_crisis1", "fsl_lcsi_crisis2", "fsl_lcsi_crisis3",
                   "fsl_lcsi_emergency1", "fsl_lcsi_emergency2", "fsl_lcsi_emergency3"),
      lcsi_value_set = c("yes", "no_had_no_need", "no_exhausted", "not_applicable"),
      with_hdds = FALSE,
      hdds_cat = "fsl_hdds_cat",
      hdds_cat_values = c("Low", "Medium", "High"),
      hdds_set = c("fsl_hdds_cereals", "fsl_hdds_tubers", "fsl_hdds_veg", "fsl_hdds_fruit",
                   "fsl_hdds_meat", "fsl_hdds_eggs", "fsl_hdds_fish", "fsl_hdds_legumes",
                   "fsl_hdds_dairy", "fsl_hdds_oil", "fsl_hdds_sugar", "fsl_hdds_condiments"),
      hdds_value_set = c("yes", "no"),
      with_fclc = FALSE,
      fclc_matrix_var = "fclcm_phase",
      fclc_matrix_values = c("Phase 1 FCLC", "Phase 2 FCLC", "Phase 3 FCLC", "Phase 4 FCLC",
                             "Phase 5 FCLC"),
      fc_matrix_var = "fsl_fc_phase",
      fc_matrix_values = c("Phase 1 FC", "Phase 2 FC", "Phase 3 FC", "Phase 4 FC",
                           "Phase 5 FC"),
      other_variables = NULL,
      stat_col = "stat",
      proportion_name = "prop_select_one",
      mean_name = "mean"
    )

create_xlsx_group_x_variable(
  IPC_output,
  table_name = "ipc_table",
  dataset_name = "dataset",
  file_path = "05_HQ_validation/02_results_tables/IPC/IPC_reslts_table_HC_test.xlsx",
  table_sheet = "ipc_table",
  dataset_sheet = "dataset",
  write_file = TRUE,
  overwrite = TRUE
)

# ──────────────────────────────────────────────────────────────────────────────
# 6. Final validation output
# ──────────────────────────────────────────────────────────────────────────────

date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

clean_data_output <- imap(clean_data, function(df, name) {
  if (name == "main") {
    df
  } else {
    df$my_clean_data_final <- df$my_clean_data_final %>%
      left_join(main_to_join, by = join_by(index == index))
    df
  }}
  )

## at some point the list gets reordered. Put MAIN at the first so it exports more nicely.
main <- list(main = clean_data_output[[5]])
main_name <- names(main)[1]
clean_data_output[[5]] <- NULL
clean_data_output <- append(clean_data_output, main, after = 0)


variable_tracker <- imap(clean_data, function(x, name) {
  ImpactFunctions::create_variable_tracker(x$raw_data, x$my_clean_data_final ) %>%
    mutate(sheet_name = name)})

variable_tracker_sheet <- bind_rows(variable_tracker) %>%
  mutate(Rationale = case_when(
    str_detect(Variable, "under5|ind_dob") ~ "Removed due to data cleaning",
    str_detect(Variable, "fcs|hhs|lcsi|rcsi|fc") ~ "Added as part of FCS calcs",
    str_detect(Variable, "FCS") ~ "Old FCS overwritten",
    str_detect(Variable, "weights") ~ "Added weight"))



flat_list <- imap(clean_data_output, function(sublist, clean_data_output) {
  imap(sublist, function(df, child_name) {
    df  # Just return the dataframe
  }) %>%
    set_names(~ paste(clean_data_output, ., sep = "_"))  # Prefix child names
}) %>%
  flatten()

sheets <- names(flat_list)
readme <- data.frame("Sheet_Names" = sheets, description = NA)
flat_list <- append(flat_list, list(README = readme), after = 0)
flat_list <- append(flat_list, list(Variable_Tracker = variable_tracker_sheet), after = 1)
flat_list <- append(flat_list, list(Questions = questions), after = 2)
flat_list <- append(flat_list, list(Choices = choices), after = 3)



writexl::write_xlsx(flat_list, "05_HQ_validation/01_all_data_and_logbook/all_data_logbook.xlsx")
