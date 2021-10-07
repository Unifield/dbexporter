SELECT
    {%- for column in columns %}
    {{ column }}::TEXT{{ "," if not loop.last else "" }}
    {%- endfor %}
FROM
    {%- if schema %}
    {{ schema }}.{{ table }}
    {%- else %}
    {{ table }}
    {%- endif %}