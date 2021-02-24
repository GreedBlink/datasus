library(dplyr)
library(stringr)
'%out%'<-function(x,y){!(x%in%y)}



fun_sih_prep <- function(dados){
  
  
  
  lista_final = list()
  
  
# lista de temas e cids
tema_cid <- list(
  `cancer-colorretal` = c("C18","C19","C20","C21"),
  `cancer-de-mama` = "C50",
  `cancer-de-prostata` = "C61",
  `cancer-de-traqueia-bronquios-e-pulmao` = c("C33","C34"),
  `cancer-do-colo-do-utero` = "C53",
  `depressao` = c("F32","F33"),
  `desnutricao` = c("E43","E44","E45","E46"),
  `diabetes` = c("E10","E11","E12","E13","E14"),
  `hipertensao` = "I10",
  `obesidade` = c("E66"),
  `transtornos-de-ansiedade` = c("F40","F41","F931","F932"),
  `informacoes-sensiveis-a-aps` =
    list(
      grupo1  =  c(
        'A37','A36','A33','A34','A35','B26','B06','B05','A95','B16','G000','A170','A19',
        'A150','A151','A152','A153','A160','A161','A162','A154','A155','A156','A157','A158','A159',
        'A163','A164','A165','A166','A167','A168','A169',
        'A171','A172','A173','A174','A175','A176','A177','A178','A179',
        'A18','I00','I01','I02','A51','A52','A53','B50','B51','B52','B53','B54','B77'
      ),
      grupo2  = c('E86','A00','A01','A02','A03','A04','A05','A06','A07','A08','A09'),
      grupo3  = c('D50'),
      grupo4  = paste0("E",c(40:46,50:64)),
      grupo5  = c('H66','J00','J01','J02','J03','J06','J31'),
      grupo6  = c('J13','J14','J153','J154','J158','J159','J181'),
      grupo7  = c('J45','J46'),
      grupo8  = c('J20','J21','J40','J41','J42','J43','J47','J44'),
      grupo9  = c('I10','I11'),
      grupo10 = c('I20'),
      grupo11 = c('I50','J81'),
      grupo12 = c('I63','I64','I65','I66','I67','I69','G45','G46'),
      grupo13 = c('E10','E11','E12','E13','E14'),
      grupo14 = c('G40','G41'),
      grupo15 = c('N10','N11','N12','N30','N34','N390'),
      grupo16 = c('A46','L01','L02','L03','L04','L08'),
      grupo17 = c('N70','N71','N72','N73','N75','N76'),
      grupo18 = c('K25','K26','K27','K28','K920','K921','K922'),
      grupo19 = c('O23','A50','P350')
    )
)

cids_remover <- c("C50","C61","C53","I10","E66")

# datasus_sih_internacoes -------------------------------------------------

df <- dplyr::bind_rows(
  purrr::map_dfr(
    tema_cid[-12],
    ~{
      condition = paste0(.x, collapse = "|")
      dados %>% 
        tibble::tibble() %>%
        dplyr::filter(stringr::str_detect(DIAG_PRINC, condition))
    },
    .id = "tema"
  ),
  purrr::map_dfr(
    tema_cid$`informacoes-sensiveis-a-aps`,
    ~{
      condition = paste0(.x, collapse = "|")
      dados %>% 
        tibble::tibble() %>%
        dplyr::filter(stringr::str_detect(DIAG_PRINC, condition))
    },
    .id = "grupo_aps"
  ) %>% dplyr::mutate(tema = "informacoes-sensiveis-a-aps")
) %>%
  dplyr::select(tema, grupo_aps, ANO_CMPT, MES_CMPT, MUNIC_RES, DIAG_PRINC, MORTE, SEXO, IDADE, RACA_COR) %>%
  purrr::set_names(c("tema","grupo_aps","ano","mes","cod_municipio","cid","obito","sexo","idade","racacor")) %>%
  dplyr::mutate(
    cod_municipio = as.character(cod_municipio),
    cid = as.character(cid),
    cid = case_when(
      tema != "transtornos-de-ansiedade" ~ substr(cid,1,3),
      stringr::str_detect(cid, "F40|F41") ~ substr(cid,1,3),
      T ~ cid
    ),
    cids = grupo_aps,
    cids = ifelse(is.na(cids), cid, grupo_aps), 
    idade = as.character(idade) %>% as.numeric(),
    sexo = as.character(sexo),
    sexo = case_when(
      sexo == "1" ~ "M",
      sexo == "3" ~ "F",
      T ~ "Ignorado"
    ),
    racacor = as.character(racacor),
    faixa_etaria = case_when(
      idade < 5 ~ "0 a 4 anos",
      idade >=  5 & idade <= 15 ~ "5 a 14 anos",
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
      T ~ "Sem informação"
    )
  ) %>%
  dplyr::select(tema, cids, everything(), -grupo_aps, -cid) %>%
  dplyr::filter(cod_municipio != "999999") 

df_final <- 
  dplyr::bind_rows(
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = length(cod_municipio),
        cids = "total",
        obito = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, obito, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = length(cod_municipio),
        cids = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(obito = as.character(obito)),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, obito, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = length(cod_municipio),
        .groups = 'drop'
      ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(obito = as.character(obito))
  ) %>% 
  dplyr::select(ano, mes, cod_municipio, tema, cids, obito, everything()) %>%
  dplyr::mutate(cod_uf = substr(cod_municipio,1,2)) %>%
  dplyr::select(ano, mes, cod_uf, everything()) %>%
  dplyr::arrange(ano, mes, cod_uf, cod_municipio, tema, cids, obito) %>%
  dplyr::filter(cids %out% cids_remover)

  lista_final$datasus_sih_internacoes = df_final
# datasus_sih_dias -------------------------------------------------

df <- dplyr::bind_rows(
  purrr::map_dfr(
    tema_cid[-12],
    ~{
      condition = paste0(.x, collapse = "|")
      dados %>% 
        tibble::tibble() %>%
        dplyr::filter(stringr::str_detect(DIAG_PRINC, condition))
    },
    .id = "tema"
  ),
  purrr::map_dfr(
    tema_cid$`informacoes-sensiveis-a-aps`,
    ~{
      condition = paste0(.x, collapse = "|")
      dados %>% 
        tibble::tibble() %>%
        dplyr::filter(stringr::str_detect(DIAG_PRINC, condition))
    },
    .id = "grupo_aps"
  ) %>% dplyr::mutate(tema = "informacoes-sensiveis-a-aps")
) %>%
  dplyr::select(tema, grupo_aps, ANO_CMPT, MES_CMPT, MUNIC_RES, DIAG_PRINC, DIAS_PERM, SEXO, IDADE, RACA_COR) %>%
  purrr::set_names(c("tema","grupo_aps","ano","mes","cod_municipio","cid","dias_perm","sexo","idade","racacor")) %>%
  dplyr::mutate(
    cod_municipio = as.character(cod_municipio),
    cid = as.character(cid),
    cid = case_when(
      tema != "transtornos-de-ansiedade" ~ substr(cid,1,3),
      stringr::str_detect(cid, "F40|F41") ~ substr(cid,1,3),
      T ~ cid
    ),
    cids = grupo_aps,
    cids = ifelse(is.na(cids), cid, grupo_aps), 
    idade = as.character(idade) %>% as.numeric(),
    sexo = as.character(sexo),
    sexo = case_when(
      sexo == "1" ~ "M",
      sexo == "3" ~ "F",
      T ~ "Ignorado"
    ),
    racacor = as.character(racacor),
    faixa_etaria = case_when(
      idade < 5 ~ "0 a 4 anos",
      idade >=  5 & idade <= 15 ~ "5 a 14 anos",
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
      T ~ "Sem informação"
    )
  ) %>%
  dplyr::select(tema, cids, everything(), -grupo_aps, -cid) %>%
  dplyr::filter(cod_municipio != "999999") 

df_final <- 
  dplyr::bind_rows(
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = mean(dias_perm),
        cids = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = mean(dias_perm),
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = mean(dias_perm),
        cids = "total",
        sexo = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor = mean(dias_perm),
        sexo = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup()
  ) %>% 
  dplyr::select(ano, mes, cod_municipio, tema, cids, everything()) %>%
  dplyr::mutate(cod_uf = substr(cod_municipio,1,2)) %>%
  dplyr::select(ano, mes, cod_uf, everything()) %>%
  dplyr::arrange(ano, mes, cod_uf, cod_municipio, tema, cids) %>%
  dplyr::filter(cids %out% cids_remover)

  lista_final$datasus_sih_dias = df_final
# datasus_sih_gastos -------------------------------------------------

df <- dplyr::bind_rows(
  purrr::map_dfr(
    tema_cid[-12],
    ~{
      condition = paste0(.x, collapse = "|")
      dados %>% 
        tibble::tibble() %>%
        dplyr::filter(stringr::str_detect(DIAG_PRINC, condition))
    },
    .id = "tema"
  ),
  purrr::map_dfr(
    tema_cid$`informacoes-sensiveis-a-aps`,
    ~{
      condition = paste0(.x, collapse = "|")
      dados %>% 
        tibble::tibble() %>%
        dplyr::filter(stringr::str_detect(DIAG_PRINC, condition))
    },
    .id = "grupo_aps"
  ) %>% dplyr::mutate(tema = "informacoes-sensiveis-a-aps")
) %>%
  dplyr::select(tema, grupo_aps, ANO_CMPT, MES_CMPT, MUNIC_RES, DIAG_PRINC, VAL_TOT, VAL_SH, VAL_SP, SEXO, IDADE, RACA_COR) %>%
  purrr::set_names(c("tema","grupo_aps","ano","mes","cod_municipio","cid","valor_total","valor_sh","valor_sp","sexo","idade","racacor")) %>%
  dplyr::mutate(
    cod_municipio = as.character(cod_municipio),
    cid = as.character(cid),
    cid = case_when(
      tema != "transtornos-de-ansiedade" ~ substr(cid,1,3),
      stringr::str_detect(cid, "F40|F41") ~ substr(cid,1,3),
      T ~ cid
    ),
    cids = grupo_aps,
    cids = ifelse(is.na(cids), cid, grupo_aps), 
    idade = as.character(idade) %>% as.numeric(),
    sexo = as.character(sexo),
    sexo = case_when(
      sexo == "1" ~ "M",
      sexo == "3" ~ "F",
      T ~ "Ignorado"
    ),
    racacor = as.character(racacor),
    faixa_etaria = case_when(
      idade < 5 ~ "0 a 4 anos",
      idade >=  5 & idade <= 15 ~ "5 a 14 anos",
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
      T ~ "Sem informação"
    )
  ) %>%
  dplyr::select(tema, cids, everything(), -grupo_aps, -cid) %>%
  dplyr::filter(cod_municipio != "999999") 

df_final <- 
  dplyr::bind_rows(
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor_tipo = "medio",
        valor_total = mean(valor_total),
        valor_sh = mean(valor_sh),
        valor_sp = mean(valor_sp),
        cids = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor_tipo = "medio",
        valor_total = mean(valor_total),
        valor_sh = mean(valor_sh),
        valor_sp = mean(valor_sp),
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor_tipo = "total",
        valor_total = sum(valor_total),
        valor_sh = sum(valor_sh),
        valor_sp = sum(valor_sp),
        cids = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, sexo, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        valor_tipo = "total",
        valor_total = sum(valor_total),
        valor_sh = sum(valor_sh),
        valor_sp = sum(valor_sp),
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        sexo = "total",
        valor_tipo = "medio",
        valor_total = mean(valor_total),
        valor_sh = mean(valor_sh),
        valor_sp = mean(valor_sp),
        cids = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        sexo = "total",
        valor_tipo = "medio",
        valor_total = mean(valor_total),
        valor_sh = mean(valor_sh),
        valor_sp = mean(valor_sp),
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        sexo = "total",
        valor_tipo = "total",
        valor_total = sum(valor_total),
        valor_sh = sum(valor_sh),
        valor_sp = sum(valor_sp),
        cids = "total",
        .groups = 'drop'
      ) %>%
      dplyr::ungroup(),
    df %>% 
      dplyr::group_by(
        ano, mes, cod_municipio, tema, cids, faixa_etaria, racacor
      ) %>%
      dplyr::summarize(
        sexo = "total",
        valor_tipo = "total",
        valor_total = sum(valor_total),
        valor_sh = sum(valor_sh),
        valor_sp = sum(valor_sp),
        .groups = 'drop'
      ) %>%
      dplyr::ungroup()
  ) %>% 
  dplyr::select(ano, mes, cod_municipio, tema, cids, everything()) %>%
  dplyr::mutate(cod_uf = substr(cod_municipio,1,2)) %>%
  dplyr::select(ano, mes, cod_uf, everything()) %>%
  dplyr::arrange(ano, mes, cod_uf, cod_municipio, tema, cids, valor_tipo) %>%
  dplyr::filter(cids %out% cids_remover)
  lista_final$datasus_sih_gastos = df_final
  

  return(lista_final)
}