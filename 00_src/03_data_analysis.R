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
#clean_data <- clean_data[c("main", "roster")]

# now we want to add some useful metadata

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















