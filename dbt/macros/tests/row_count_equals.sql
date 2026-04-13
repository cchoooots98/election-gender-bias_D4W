{% test row_count_equals(model, compare_model) %}

select 1
where (select count(*) from {{ model }})
    <> (select count(*) from {{ compare_model }})

{% endtest %}
