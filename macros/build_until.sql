{% macro build_until(column_name, default_date='2999-12-31') %}
    
    {% set build_until_date = var('build_until_date', default_date) %}
    
    {% if execute %}
        {{ log("🔧 Build Control: Limiting " ~ column_name ~ " to " ~ build_until_date, info=True) }}
    {% endif %}
    
    AND {{ column_name }}::timestamp <= '{{ build_until_date }}'::timestamp
    
{% endmacro %}