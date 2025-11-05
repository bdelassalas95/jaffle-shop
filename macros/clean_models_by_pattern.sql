{% macro clean_models_by_pattern(table_pattern, database=target.database, schema_pattern=target.schema, dry_run=True) %}

    {%- set relations_to_drop = dbt_utils.get_relations_by_pattern(
        database = database,
        schema_pattern = schema_pattern,
        table_pattern = table_pattern
    ) -%}

    {% set sql_to_execute = [] %}

    {{ log('Statements to run:', info=True) }}

    {% for relation in relations_to_drop %}
        {% set drop_command -%}
            drop {{ relation.type }} {{ relation }} cascade;
        {%- endset %}

        {% if dry_run %}
            {% do log(drop_command, info=True) %}
        {% else %}
            {% do log('Dropping object with command: ' ~ drop_command, info=True) %}
            --{% do sql_to_execute.append(drop_command) %}
            {% do run_query(drop_command) %}
        {% endif %}
    {% endfor %}

{% endmacro %}