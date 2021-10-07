SELECT
    {%- for column in columns %}
    {{ column }}::TEXT{% if not loop.last %}, {% endif %}
    {%- endfor %}
FROM
    {%- if schema %}
    {{ schema }}.{{ table }}
    {%- else %}
    {{ table }}
    {%- endif %}