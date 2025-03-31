select *
from {{ source('raw', 'dicionario_parquet')}}