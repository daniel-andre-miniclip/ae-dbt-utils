{#
    Macro: validate_start_end_date()
    Description:
        Macro is triggered on-run-start. Will parse --vars for start_date and end_date. 
        If start_date and end_date have not been defined, it will set those variables for yesterday.
        These variables should be defined via a namespace object at model level, since this macro executes on-run-start for no model in specific.
        Will log those variables and issue a warning if they weren't sent via dbt command. 
    Args: N/A
    Returns: Void
#}
{%- macro validate_start_end_date() %}

    {%- set start_date = var('start_date', start_date) -%}
    {%- set end_date = var('end_date', end_date) -%}

    {{ log("INFO - validate start_date - Parsed variable: " ~ start_date, info=True) }}
    {{ log("INFO - validate end_date - Parsed variable: " ~ end_date, info=True) }}

    {%- if start_date is not defined %}
        {{ exceptions.warn('WARNING - start_date is undefined which can lead a model to materialize incorrectly. Consider passing them via --vars "{\'start_date\': \'YYYY-MM-DD\'}"') }}
        {{ log('WARNING - Reprocessing one day with start_date default as: ' ~ get_date_ndays_ago(1), info=True) }}
    {%- endif %}

    {%- if end_date is not defined %}
        {{ exceptions.warn('WARNING - end_date is undefined which can lead a model to materialize incorrectly. Consider passing them via --vars "{\'end_date\': \'YYYY-MM-DD\'}"') }}
        {{ log('WARNING - Reprocessing one day with end_date default as: ' ~ get_date_yesterday(), info=True) }}
    {%- endif %}

{%- endmacro %}
