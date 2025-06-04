questions  <- read_excel(kobo_tool_name, sheet = "survey")

# 2a. Identify all `begin_repeat` / `end_repeat` rows:
begin_idxs <- which(questions$type == "begin_repeat")
end_idxs   <- which(questions$type == "end_repeat")

# 2b. Build `repeat_fields`: a named list of every child‐question `name` inside each repeat
repeat_fields <- list()
for (i in seq_along(begin_idxs)) {
  start_row <- begin_idxs[i]
  end_row   <- end_idxs[end_idxs > start_row][1]
  if (is.na(end_row)) next

  grp_name     <- questions$name[start_row]
  inside_names <- questions$name[(start_row + 1):(end_row - 1)]
  repeat_fields[[grp_name]] <- inside_names
}

# 2c. Build `main`: all question names NOT inside any repeat (and not `begin_repeat`/`end_repeat` themselves)
in_repeat <- rep(FALSE, nrow(questions))
for (i in seq_along(begin_idxs)) {
  start_row <- begin_idxs[i]
  end_row   <- end_idxs[end_idxs > start_row][1]
  if (!is.na(end_row)) {
    in_repeat[start_row:end_row] <- TRUE
  }
}
main_rows  <- which(
  !in_repeat &
    !(questions$type %in% c("begin_repeat","end_repeat"))
)
main <- tibble(name = questions$name[main_rows])


filter_by_survey <- function(df, allowed_basenames) {
  # Escape regex‐special characters in each base name
  esc <- str_replace_all(
    allowed_basenames,
    "([\\.\\+\\*\\?\\[\\^\\]\\$\\(\\)\\{\\}=!<>\\|:\\-])", "\\\\\\1"
  )
  # Build a single regex: ^(base1|base2|…)(?:$|/)
  patt <- paste0("^(", paste(esc, collapse = "|"), ")(?:$|/)")

  df %>% select(matches(patt), index, roster_index, `_parent_table_name`, uuid)
}
