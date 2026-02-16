{#
    This macro resolves the schema for different model types.
    It routes core models to the target dataset and staging models to the staging dataset.
#}

{% macro resolve_schema_for(model_type) -%}

    {%- if model_type == 'core' -%}
        {{ env_var('DBT_BIGQUERY_TARGET_DATASET', target.schema) }}
    {%- elif model_type == 'stg' -%}
        {{ env_var('DBT_BIGQUERY_STAGING_DATASET', env_var('DBT_BIGQUERY_TARGET_DATASET', target.schema)) }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}

{%- endmacro %}
