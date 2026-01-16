{% macro set_query_tag() -%}
    
    {% set new_query_tag = get_custom_query_tag() %}

    {% if new_query_tag %}
        {% set original_query_tag = get_current_query_tag() %}
        {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
        {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
        {{ return(original_query_tag)}}
    {% endif %}
    {{ return(none)}}

{% endmacro %}

{% macro get_custom_query_tag() %}

    {# -- THE BELOW ASSUMES WE ONLY HAVE THESE TARGETS: DEV, PROD, STG, CI #}
    {% set env_value = target.name | upper %}

    {% set model_file_path_list = model.original_file_path.split('/') %}
    {# -- BELOW LOGIC ASSUMES MODEL STRUCTURE AS `./models/<product name>/<layer>/.../foo_bar.sql` #}
    {% set product_value = model_file_path_list[1] %}
    {% set layer_value = model_file_path_list[2] | capitalize %}
    {% set domain_value = 'Hubble' %} {# -- HARD-CODED #}

    {% set query_tag_string = '{"DOMAIN":"'~domain_value~'","ENV":"'~env_value~'","LAYER":"'~layer_value~'","PRODUCT":"'~product_value~'","DBT_MODEL":"'~model.name~'"}' %}

    {{ return(query_tag_string) }}

{% endmacro %}