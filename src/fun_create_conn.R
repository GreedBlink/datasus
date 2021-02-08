fun_create_conn <- function(user, pass,dataset,host,port){
  conn <- 
    tryCatch({ 
      DBI::dbConnect(
        RPostgres::Postgres(),
        db = dataset,
        user = user,
        password = pass,
        host = host,
        port = port
      )},
      error = function(e){
        usethis::ui_oops('Error in connection')
        cat(e)
      },
      warning = function(w){
        usethis::ui_warn('Error in connection')
        cat(e)
      }
    )
}