{#

{% set temperate = 60.0 %}

On a day like this, I especially like
{% if temperate > 70 %}
a refreshing lemon sorbet

{% else %}
a decadent chocolate cake
    
{% endif %}

{% for j in range(26) %}
    select {{ j }} as number {% if not loop.last %} union all {% endif %}
{% endfor %}

#}

{#

{% set cool_string = 'Wow cool beans!' %}
{% set my_second_cool_string = 'This is Jinja!' %}
{% set my_fav_num = 26 %}

{{ cool_string }} {{ my_second_cool_string }} I want to write Jina for {{ my_fav_num }} years!

#}

{#
{% set animals=['lemur', 'dingo', 'rhino', 'dog'] %}

{{ animals[0] }}
{{ animals[1] }}
{{ animals[2] }}
{{ animals[3] }}

{% for animal in animals %}

My favorite animal is the {{ animal }}
    
{% endfor %}

#}

{#
{%- set foods=['radish', 'cucumber', 'chicken nugget', 'avocado'] -%}

{%- for food in foods -%}
    {%- if food == 'chicken nugget' -%}
        {%- set food_type='snack' -%}

    {%- else -%}
        {%- set food_type='vegatable' -%}

    {%- endif -%}
    The delicious {{ food }} is my favorite {{ food_type }}.
    
{% endfor %}

#}

{#
{%- set jennas_dictionary = {
    'word' : 'data',
    'part_of_speech' : 'noun',
    'definition' : 'the building block of life'
} -%}

{{ jennas_dictionary['word'] }} ({{ jennas_dictionary['part_of_speech'] }}): defined as "{{ jennas_dictionary['definition'] }}"
#}

