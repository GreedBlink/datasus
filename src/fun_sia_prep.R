library(dplyr)


fun_sia_prep <- function(dados, estado_param){

  df <- dados %>% 
    tibble() %>%
    dplyr::select(PA_MUNPCN, PA_PROC_ID, PA_IDADE, PA_SEXO, PA_RACACOR,ano,mes) %>%
    purrr::set_names(c("cod_municipio","proc_id","idade","sexo","racacor",'ano','mes')) %>%
    dplyr::mutate(
      cod_municipio = as.character(cod_municipio),
      idade = as.character(idade) %>% as.numeric(),
      sexo = as.character(sexo),
      racacor = as.character(racacor),
      faixa_etaria = case_when(
        idade < 15 ~ "0 a 14 anos",
        idade >= 15 & idade <= 24 ~ "15 a 24 anos",
        idade >= 25 & idade <= 34 ~ "25 a 34 anos",
        idade >= 35 & idade <= 44 ~ "35 a 44 anos",
        idade >= 45 & idade <= 54 ~ "45 a 54 anos",
        idade >= 55 & idade <= 64 ~ "55 a 64 anos",
        idade >= 65 ~ "65 anos ou mais"
      ),
      racacor = case_when(
        racacor == "01" ~ "Branca", 
        racacor == "02" ~ "Preta",
        racacor == "03" ~ "Parda",
        racacor == "04" ~ "Amarela", 
        racacor == "05" ~ "Indígena", 
        racacor == "99" ~ "Sem informação",
        TRUE ~ "Sem informação"
      )
    ) %>%
    dplyr::filter(cod_municipio != "999999")
  
  df_final <- df %>% 
    dplyr::group_by(
      cod_municipio, proc_id, ano,mes,sexo, faixa_etaria, racacor
    ) %>%
    dplyr::summarize(
      valor = length(cod_municipio)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      cod_uf = substr(cod_municipio,1,2),
      estado = estado_param
    ) %>%
    dplyr::select(cod_uf,estado, everything()) %>%
    dplyr::arrange(cod_municipio, proc_id, sexo, faixa_etaria, racacor)
  
  df_final
}

