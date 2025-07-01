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
loa <- map(sheet_names, ~ read_excel(loa_path, sheet = .x, guess_max = 10000))
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
data_file_path <- "02_input/00_data_download/REACH_KEN_2025_MSNA.xlsx"
sheet_names <- readxl::excel_sheets(data_file_path)
clean_data <- map(sheet_names, ~ read_excel(data_file_path, sheet = .x, guess_max = 10000))
sheet_names[1] <- "main"
names(clean_data) <- sheet_names

clean_data <- read_rds("03_output/01_raw_data/all_raw_data.rds")

# ──────────────────────────────────────────────────────────────────────────────
# 2. Some data processing
# ──────────────────────────────────────────────────────────────────────────────

clean_data$main <- clean_data$main %>%
  mutate(sampling_id = str_squish(sampling_id)) %>%
  add_weights(sampling_frame,
              strata_column_dataset = "sampling_id",
              strata_column_sample = "sampling_id",
              population_column = "population")


main_to_join <- clean_data$main  %>%
  dplyr::select(admin_1_camp, admin_2_camp, admin_3_camp, sampling_id, today,enum_id,resp_gender, enum_gender,
                hoh_gender,deviceid,instance_name, index, weights)

clean_data_joined <- imap(clean_data, function(df, name) {
  if (name == "main") {
    df
  } else {
    df %>%
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

### this is not finalised yet.

presentresults::create_ipc_table(
    results_table = clean_data_analysis$main$results_tbl,
    analysis_key = "analysis_key",
    dataset = clean_data_joined$main,
    cluster_name = sampling_id,
    fcs_cat_var = "FCSGName",
    fcs_cat_values = c("Poor", "Borderline", "Acceptable"),
    fcs_set = c("fsl_fcs_cereal", "fsl_fcs_legumes", "fsl_fcs_veg", "fsl_fcs_fruit",
                "fsl_fcs_meat", "fsl_fcs_dairy", "fsl_fcs_sugar", "fsl_fcs_oil"),
    hhs_cat_var = "hhs_cat_ipc",
    hhs_cat_values = c("None", "Little", "Moderate", "Severe", "Very Severe"),
    hhs_cat_yesno_set = c("fsl_hhs_nofoodhh", "fsl_hhs_sleephungry", "fsl_hhs_alldaynight"),
    hhs_value_yesno_set = c("yes", "no"),
    hhs_cat_freq_set = c("fsl_hhs_nofoodhh_freq", "fsl_hhs_sleephungry_freq",
                         "fsl_hhs_alldaynight_freq"),
    hhs_value_freq_set = c("rarely", "sometimes", "often"),
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



# ──────────────────────────────────────────────────────────────────────────────
# 6. Final validation output
# ──────────────────────────────────────────────────────────────────────────────
