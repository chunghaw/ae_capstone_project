{%- macro ft2_to_m2(column_name) -%}
    round({{ column_name }} * 0.092903, 2)
{%- endmacro -%}