{% test greater_than_or_equal_to(model, column_name, value) %}

    select *
    from {{ model }}
    where {{ column_name }} < {{ value }}

{% endtest %}

{% test between(model, column_name, min_value, max_value) %}

    select *
    from {{ model }}
    where {{ column_name }} < {{ min_value }}
       or {{ column_name }} > {{ max_value }}

{% endtest %}
