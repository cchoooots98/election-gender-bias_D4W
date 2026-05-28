{% macro relation_exists(source_name, table_name) -%}
    {%- if execute -%}
        {%- set relation = source(source_name, table_name) -%}
        {%- set existing_relation = adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier
        ) -%}
        {{ return(existing_relation is not none) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}
{%- endmacro %}
