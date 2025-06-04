rm(list = ls())


library(dplyr)
library(ggplot2)
library(readr)
library(writexl)
library(tidyr)
library(stringr)
library(lubridate)
library(shiny)
library(shinydashboard)
library(reactable)



###### inputs


## enumerator performance
enum_performance <- read_csv("02_input/06_dashboard_inputs/enum_performance.csv")

## calculate completion per site
sites_done <- readxl::read_excel("02_input/06_dashboard_inputs/completion_report.xlsx")

## completion by FO
completion_by_FO <-  readxl::read_excel("02_input/06_dashboard_inputs/completion_by_FO.xlsx")

## actual burndown
actual_burndown <- read_csv("02_input/06_dashboard_inputs/actual_burndown.csv")


####### Shiny Dashboard

{
  # UI
  ui <- dashboardPage(
    dashboardHeader(title = "KEN MSNA 2025 Field Dashboard"),
    dashboardSidebar(
      collapsed = TRUE,
      sidebarMenu(
        menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard"))))
    ,
    dashboardBody(
      tabItems(
        tabItem(tabName = "dashboard",
                fluidRow(
                  box(title = "Site Completion", status = "primary", solidHeader = TRUE, width = 6,
                      reactableOutput("reactable_OPZ"),
                      downloadButton("download_OPZ", "Export as Excel")
                  ),
                  box(title = "Enumerator Performance", status = "primary", solidHeader = TRUE, width = 6,
                      reactableOutput("reactable_enum"),
                      downloadButton("download_enum", "Export as Excel")
                  )
                ),
                fluidRow(
                  box(title = "Total Surveys vs Surveys Completed Over Time", status = "primary", solidHeader = TRUE, width = 6,
                      plotOutput("plot_burndown")
                  ),
                  box(title = "Completion by FO", status = "primary", solidHeader = TRUE, width = 6,
                      plotOutput("plot_completion_by_FO")
                  )
                )
        )
      )
    )
  )



  # Server
  server <- function(input, output, session) {
    # Reactive filtered data

    # Reactable for OPZ Completion
    output$reactable_OPZ <- renderReactable({
      reactable(sites_done, searchable = TRUE, bordered = TRUE, filterable = TRUE)
    })

    # Download for OPZ Completion
    output$download_OPZ <- downloadHandler(
      filename = function() {
        paste("Survey_District_Done", Sys.Date(), ".xlsx", sep = "")
      },
      content = function(file) {
        write.xlsx(KIIs_Done, file)
      }
    )

    # Bar Chart for Completion by FO
    output$plot_completion_by_FO <- renderPlot({
      ggplot(completion_by_FO, aes(x = reorder(fo, -Completion_Percent), y = Completion_Percent, fill = fo)) +
        geom_col(show.legend = FALSE, width = 0.6) +
        geom_text(aes(label = paste0(Completion_Percent, "%")), vjust = -0.5, size = 5) +
        labs(title = "Completion by FO / Assessment", x = "FO / Assessment", y = "Completion Percentage") +
        theme_minimal() +
        theme(
          plot.title = element_text(hjust = 0.5, size = 16, face = "bold"),
          axis.text = element_text(size = 12),
          axis.title = element_text(size = 14)
        ) +
        scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, by = 20))  # Corrected y-axis scale
    })


    # Burndown Chart
    output$plot_burndown <- renderPlot({

      total_tasks <- sum(sites_done$Surveys_Target)
      days <- 8

      # Ideal burndown data (linear decrease)
      ideal_burndown <- data.frame(
        Day = 0:days,
        Remaining_Tasks = seq(from = total_tasks, to = 0, length.out = days + 1)
      )

      ggplot() +
        # Ideal burndown line
        geom_line(data = ideal_burndown, aes(x = Day, y = Remaining_Tasks),
                  color = "#2C3E50", size = 1, linetype = "dashed") +
        # Actual progress line
        geom_line(data = actual_burndown, aes(x = Day, y = Remaining_Tasks),
                  color = "#EE5859", size = 1.5, alpha = 0.5) +
        # Points for actual progress
        geom_point(data = actual_burndown, aes(x = Day, y = Remaining_Tasks),
                   color = "#EE5859", size = 2) +
        labs(
          x = "Days",
          y = "Remaining Surveys"
        ) +
        theme_minimal(base_family = "Arial") +
        theme(
          plot.title = element_text(hjust = 0.5, size = 18, face = "bold", color = "#2C3E50"),
          axis.title = element_text(size = 14, face = "bold", color = "#34495E"),
          axis.text = element_text(size = 12, color = "#34495E"),
          panel.grid.major = element_line(color = "#BDC3C7", size = 0.5),
          panel.grid.minor = element_blank(),
          panel.background = element_rect(fill = "white", color = NA),
          plot.background = element_rect(fill = "white", color = NA),
          legend.position = "none"
        ) +
        scale_y_continuous(breaks = seq(0, total_tasks, by = 400), labels = scales::comma) +
        scale_x_continuous(breaks = seq(0, max(ideal_burndown$Day), by = 2))




    })

    # Reactable for Enumerator Performance
    output$reactable_enum <- renderReactable({
      reactable(enum_performance, searchable = TRUE, bordered = TRUE)
    })

    # Download for Enumerator Performance
    output$download_enum <- downloadHandler(
      filename = function() {
        paste("Enumerator_Performance_", Sys.Date(), ".xlsx", sep = "")
      },
      content = function(file) {
        write.xlsx(enum_performance, file)
      }
    )
  }

  # Run the application
  shinyApp(ui = ui, server = server)#
} ## all the code used to make the dashboard in Shiny

