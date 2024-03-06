{% if schema %}
GRANT USAGE ON SCHEMA {{ schema }} TO {{ user }};
{% for table in tables %}
GRANT SELECT ON {{ schema }}.{{ table }} TO {{ user }};
{% endfor %}
{% else %}
{% for table in tables %}
GRANT SELECT ON {{ table }} TO {{ user }};
{% endfor %}
{% endif %}
GRANT SELECT on {{ schema }}.t_journal_item TO {{ user }};

