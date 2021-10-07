SELECT
    {%- for column in columns %}
    {{ column }}::TEXT,
    {%- endfor %}
FROM
    {%- if schema %}
    {{ schema }}.{{ table }}
    {%- else %}
    {{ table }}
    {%- endif %}