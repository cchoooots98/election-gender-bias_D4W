{% macro column_exists(source_name, table_name, column_name) -%}
    {%- if execute -%}
        {%- set relation = source(source_name, table_name) -%}
        {%- set existing_relation = adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier
        ) -%}
        {%- if existing_relation is none -%}
            {{ return(false) }}
        {%- endif -%}
        {%- set columns = adapter.get_columns_in_relation(existing_relation) -%}
        {%- set column_names = columns | map(attribute='name') | map('lower') | list -%}
        {%- set normalized_column_name = column_name | lower -%}
        {{ return(normalized_column_name in column_names) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}
{%- endmacro %}
