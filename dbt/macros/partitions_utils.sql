
{#
    Macro: get_current_date()
    Description: 
        Returns today's date as a SQL-compatible date string.
        Useful for dynamic date filtering in queries.
    Args: N/A
    Returns: (string) Current date in 'YYYY-MM-DD' format
#}
{% macro get_current_date() %}
    {{ return(modules.datetime.date.today().strftime("%Y-%m-%d")) | trim }}

{% endmacro %}


{#
    Macro: get_date_yesterday()
    Description:
        Returns yesterday's date as a SQL-compatible date string
        Useful for dynamic date filtering in queries.
    Args: N/A
    Returns: (string) Yesterday's date in 'YYYY-MM-DD' format
#}
{%- macro get_date_yesterday() %}
    {% set date_yesterday = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d") %}
    {{ return(date_yesterday | trim) }}

{% endmacro %}


{#
    Macro: get_date_ndays_ago(n)
    Description:
        Returns the date N days before today., 
    Args: 
        n (int): Number of days to subtract from today
    Returns: (string) Date in 'YYYY-MM-DD' format
#}
{%- macro get_date_ndays_ago(n) %}
    {% set date_ndays_ago = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=n)).strftime("%Y-%m-%d") %}
    {{ return(date_ndays_ago | trim) }}

{% endmacro %}

{#
    Macro: set_start_end_date(namespace)
    Description:
        Sets start_date and end_date via var("start|end_date") or via a default.
    Args:
        namespace (namespace): A namespace() variable defined at the top of each model with relevant start_date and end_date attributes
    Returns: (void) Updates namespace with dates
#}
{%- macro set_start_end_date(namespace) %}
    {% set namespace.start_date = var("start_date", namespace.start_date) %}
    {% set namespace.end_date = var("end_date", namespace.end_date) %}
{% endmacro %}


{#
    Macro: get_default_start_date()
    Description: Used as a default date fallback for full-refresh incremental strategies. 
    
    Args:
        earliest_date (string | none, default=none): A valid SQL date string (e.g. '2024-05-01')
    Returns:
        A fallback start date ('1970-01-01') or a properly formatted user-input start date (namespace.start_date)
#}
{%- macro get_default_start_date(earliest_date=none) -%}
    {% set default_date = modules.datetime.date(1970, 1, 1).strftime("%Y-%m-%d") %}
    {% if earliest_date is none %}
        {{ return(default_date | trim) }}
    {% elif earliest_date is not none %}
        {%- set custom_date = modules.datetime.datetime.strptime(earliest_date, '%Y-%m-%d') -%}
        {{ return(custom_date.strftime("%Y-%m-%d") | trim) }}
    {% endif %}

{%- endmacro %}


{#
    Macro: development_date_filter()
    Description: 
        Used to limit processed data in development to n_days (default set to 14 days)
        If we need to test for a given range of dates, set enabled=False. 

    Args:
        enabled (bool): If set to true, will output the date filter.
        n_days (int): The lookback window in number of days
        date_col (str): The date column to be used. Default considered is 'activity_date'
    Returns:
        (default) Returns date filter for the last n_days, provided enabled is set to true.
#}
{%- macro development_date_filter(enabled=True, n_days=14, date_col='activity_date') %}
    {% if target.name in ['dev'] and enabled %}
        and {{ date_col }} >= current_date() - {{ n_days }}
    {% endif %}

{% endmacro %}


{# 
    Macro: date_incremental_filter()
    Description: ...

    Args:
        namespace (namespace): Namespace object containing start_date and end_date attributes
        enabled (bool): Flag to enable development_date_filter() macro
        n_days (int): The lookback window in number of days
        earliest_date (string | none, default=none): A valid SQL date string (e.g. '2024-05-01'), will default to 1970-01-01 to scan all data
        date_col (str): The name of date column to be used. Default considered is 'activity_date'
        ...
#}
{%- 
  macro date_incremental_filter(
    namespace,
    enabled=True,
    n_days=14,
    earliest_date=none,
    date_col='activity_date'
  )
%}
        where true
    {%- if is_incremental() %}
            and {{ date_col }} between 
                to_date('{{ namespace.start_date | trim}}', 'YYYY-MM-DD')
                and to_date('{{ namespace.end_date | trim }}', 'YYYY-MM-DD')
    {%- else %} 
            {# Pipeline or table start date #}
            and {{ date_col }} >= '{{ get_default_start_date() | trim }}'
    {%- endif %}
    {# Date filter for dev #}
    {{ development_date_filter(enabled=enabled, n_days=n_days, date_col=date_col) }}

{% endmacro %}
