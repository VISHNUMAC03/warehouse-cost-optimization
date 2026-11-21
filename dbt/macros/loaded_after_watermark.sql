{#
    The incremental filter every mart shares: rows loaded after the newest
    _loaded_at this model already holds. Each model keeps its own watermark
    (fct_events stores _loaded_at itself, the others store max_loaded_at),
    so a model that failed or was skipped catches up on its next run.
#}
{% macro loaded_after_watermark(watermark_column='max_loaded_at') -%}
    _loaded_at > (select max({{ watermark_column }}) from {{ this }})
{%- endmacro %}
