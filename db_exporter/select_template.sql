SELECT
    {%- for column in columns %}
    regexp_replace({{ column }}::TEXT, '[\r\n]+', ' ', 'g') AS {{ column }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
FROM
    {%- if schema %}
    {{ schema }}.{{ table }}
    {%- else %}
    {{ table }}
    {%- endif %}