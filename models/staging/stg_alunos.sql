-- models/staging/stg_desempenho_aluno.sql
{{
  config(
    materialized = 'table', -- Pode-se também usar incremental, para receber os dados dos 
    -- proximos anos sem ter que processar toda a tabela novamente.
    -- para isso, abaixo, se define a estrategia (merge, append, delete+insert... etc)
    -- Além disso, boa prática é indexar essas colunas para fazer joins e consultas (AJUDA MUITO)
    indexes = [
      {'columns': ['id_aluno'], 'type': 'hash'},
      {'columns': ['id_escola'], 'type': 'hash'},
      {'columns': ['ano'], 'type': 'btree'},
      {'columns': ['sk_desempenho'], 'type': 'btree', 'unique': True}
    ]
  )
}}

WITH 
-- 1. Filtra apenas colunas necessárias da tabela raw
base_alunos AS (
    SELECT
        id_aluno,
        id_escola,
        CAST(ano AS STRING) as ano,
        sigla_uf,
        id_municipio,
        proficiencia,
        erro_padrao,
        desempenho_aluno,
        nivel_inse,
        faltas_aula,
        CAST(sexo AS STRING) AS sexo,
        CAST(raca_cor AS STRING) AS raca_cor,
        CAST(faixa_etaria AS STRING) AS faixa_etaria,
        CAST(rede AS STRING) AS rede,
        CAST(localizacao AS STRING) AS localizacao,
        escola_publica,
        CAST(turno AS STRING) AS turno,
        serie,
        CAST(disciplina AS STRING) AS disciplina,
        presenca,
        CAST(escolaridade_mae AS STRING) AS escolaridade_mae,
        CAST(escolaridade_pai AS STRING) AS escolaridade_pai,
        CAST(possui_necessidade_especial AS STRING) AS possui_necessidade_especial,
        CAST(pessoa_acompanha_vida_escolar AS STRING) AS pessoa_acompanha_vida_escolar,
        CAST(responsaveis_comparecem_reuniao_pais AS STRING) AS responsaveis_comparecem_reuniao_pais,
        CAST(responsaveis_incentivam_estudos AS STRING) AS responsaveis_incentivam_estudos,
        CAST(possui_internet AS STRING) AS possui_internet,
        CAST(possui_computador AS STRING) AS possui_computador,
        CAST(pretensao_futura AS STRING) AS pretensao_futura,
        CAST(pandemia_apoio_familia AS STRING) AS pandemia_apoio_familia
    FROM {{ ref('alunos_source') }}
),

-- 2. Filtra o dicionário apenas para colunas relevantes e remove duplicatas
dicionario_filtrado AS (
    SELECT 
        nome_coluna, 
        chave, 
        valor, 
        cobertura_temporal
    FROM {{ ref('dicionario_source') }}
    WHERE nome_coluna IN (
        'sexo', 'raca_cor', 'faixa_etaria', 'rede', 'localizacao', 'turno', 'disciplina',
        'escolaridade_mae', 'escolaridade_pai', 'possui_necessidade_especial',
        'pessoa_acompanha_vida_escolar', 'responsaveis_comparecem_reuniao_pais',
        'responsaveis_incentivam_estudos', 'possui_internet', 'possui_computador',
        'pretensao_futura', 'pandemia_apoio_familia'
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY nome_coluna, chave, cobertura_temporal 
        ORDER BY id_tabela DESC
    ) = 1
),

-- 3. Junção com dicionário usando LEFT JOIN
alunos_com_traducoes AS (
    SELECT
        a.*,
        -- Tradução de sexo
        MAX(CASE WHEN d.nome_coluna = 'sexo' AND d.chave = a.sexo AND d.cobertura_temporal IS NULL 
            THEN d.valor END) AS sexo_descricao,
        
        -- Tradução de raça/cor
        MAX(CASE WHEN d.nome_coluna = 'raca_cor' AND d.chave = a.raca_cor AND d.cobertura_temporal IS NULL 
            THEN d.valor END) AS raca_descricao,
        
        -- Tradução de disciplina (com cobertura temporal)
        MAX(CASE WHEN d.nome_coluna = 'disciplina' AND d.chave = a.disciplina AND d.cobertura_temporal = a.ano 
            THEN d.valor END) AS disciplina_descricao,
        
        -- Tradução de apoio na pandemia (com cobertura temporal)
        MAX(CASE WHEN d.nome_coluna = 'pandemia_apoio_familia' AND d.chave = a.pandemia_apoio_familia AND d.cobertura_temporal = a.ano 
            THEN d.valor END) AS apoio_descricao
    FROM base_alunos a
    LEFT JOIN dicionario_filtrado d ON (
        (d.nome_coluna = 'sexo' AND d.chave = a.sexo AND d.cobertura_temporal IS NULL) OR
        (d.nome_coluna = 'raca_cor' AND d.chave = a.raca_cor AND d.cobertura_temporal IS NULL) OR
        (d.nome_coluna = 'disciplina' AND d.chave = a.disciplina AND d.cobertura_temporal = a.ano) OR
        (d.nome_coluna = 'pandemia_apoio_familia' AND d.chave = a.pandemia_apoio_familia AND d.cobertura_temporal = a.ano)
    )
    GROUP BY 
        a.id_aluno, a.id_escola, a.ano, a.sigla_uf, a.id_municipio, a.proficiencia, 
        a.erro_padrao, a.desempenho_aluno, a.nivel_inse, a.faltas_aula, a.sexo, 
        a.raca_cor, a.faixa_etaria, a.rede, a.localizacao, a.escola_publica, 
        a.turno, a.serie, a.disciplina, a.presenca, a.escolaridade_mae, 
        a.escolaridade_pai, a.possui_necessidade_especial, a.pessoa_acompanha_vida_escolar,
        a.responsaveis_comparecem_reuniao_pais, a.responsaveis_incentivam_estudos,
        a.possui_internet, a.possui_computador, a.pretensao_futura, a.pandemia_apoio_familia
)

-- 4. Resultado final com todas as traduções e campos calculados
SELECT
    -- Campos originais numéricos/ids
    id_aluno,
    id_escola,
    ano,
    sigla_uf,
    id_municipio,
    proficiencia,
    erro_padrao,
    serie,
    presenca,
    
    -- Campos originais categóricos (já traduzidos)
    COALESCE(sexo_descricao, sexo) AS sexo,
    COALESCE(raca_descricao, raca_cor) AS raca_cor,
    faixa_etaria,
    rede,
    localizacao,
    turno,
    COALESCE(disciplina_descricao, disciplina) AS disciplina,
    escolaridade_mae,
    escolaridade_pai,
    possui_necessidade_especial,
    pessoa_acompanha_vida_escolar,
    responsaveis_comparecem_reuniao_pais,
    responsaveis_incentivam_estudos,
    possui_internet,
    possui_computador,
    pretensao_futura,
    COALESCE(apoio_descricao, pandemia_apoio_familia) AS pandemia_apoio_familia,
    
    -- Campos derivados
    desempenho_aluno,
    nivel_inse,
    faltas_aula,
    CASE WHEN escola_publica = 1 THEN TRUE ELSE FALSE END AS flag_escola_publica,
    
    -- Chave surrogate (corrigido o nome do campo disciplina_descricao)
    CONCAT(id_aluno, '_', id_escola, '_', ano, '_', COALESCE(disciplina_descricao, disciplina)) AS sk_desempenho
FROM alunos_com_traducoes