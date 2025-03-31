select *
from {{ source('raw', 'alunos_9ano_parquet')}}

