SELECT
    id_escola,
    sigla_uf,
    id_municipio,
    rede,
    localizacao,
    escola_publica,
    -- Cria uma chave surrogate para ser usado por exemplo como campo de incrementalidade
    ROW_NUMBER() OVER() AS sk_escola
FROM (
    SELECT DISTINCT
        id_escola,
        sigla_uf,
        id_municipio,
        rede,
        localizacao,
        flag_escola_publica AS escola_publica
    FROM {{ ref('stg_alunos') }}
)