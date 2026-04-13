{% test row_count_equals_multiple(model, compare_model, multiplier) %}

select 1
where (select count(*) from {{ model }})
    <> (select count(*) from {{ compare_model }}) * {{ multiplier }}

{% endtest %}
