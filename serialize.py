"""
Serialization (XML only for now) of newsfeed articles.
"""

import re
import jinja2
import util

def xml_str(val, key=None):
	"Unicode, xml-safe variant of `val`. `key` is for debug only."
	if val is None:
		return ''
	elif type(val) in (str,unicode): 
		val = util.xmlEscape(val)
		if type(val) == str: val = val.decode('utf8','replace')
	elif type(val) in (list,tuple,set):
		val = type(val)(xml_str(x,key) for x in val)
	elif type(val) in (long, int, float):
		val = str(val)
	else: raise ValueError, "Can't handle type %r for key %r" % (type(val), key,)
	return val

def remove_xml_header(xml):
	if xml.startswith('<?xml') and '>' in xml:
		return xml[xml.find('>') + 1:]
	return xml

def mark_paras(txt):
	"""
	Given plain text where each line is a separate paragraph, returns
	a safe XML (= escaped content) with <p>-marked paras.
	The safeness is implicit; for use in a jinja2 template, use |safe.
	"""
	if not txt: return ''
	return '\n'.join('<p>%s</p>' % util.xmlEscape(line.strip()) for line in txt.splitlines() if line.strip())

env = jinja2.Environment()
env.finalize = lambda x: '' if x is None else x
env.filters['x'] = xml_str  # `e` is the built-in HTML-escaping filter; we use a stricter one. Overriding e and using autoescaping does not work :/
env.filters['remove_xml_header'] = remove_xml_header
env.filters['iso_utc'] = lambda t: util.iso_utc_time(t) if t else ''
env.filters['mark_paras'] = mark_paras

FORMAT_VERSION = '4.0'
TEMPLATE = env.from_string('''
{% macro geo_xml(geo) %}
		<location>
			{% if geo.lat and geo.lon %}<latitude>{{ geo.lat }}</latitude><longitude>{{ geo.lon }}</longitude>{% endif %}
			{% if geo.city %}<city>{{ geo.city|x }}</city>{% endif %}
			{% if geo.country %}<country>{{ geo.country|x }}</country>{% endif %}
		</location>
{% endmacro %}

<article id="{{ id|x }}">
	<source>
		{% if source_name %}<name>{{ source_name|x }}</name>{% endif %}
		<hostname>{{ source_hostname|x }}</hostname>
		{% if source_geo %}{{ geo_xml(source_geo) }}{% endif %}
		{% if source_tags %}<tags>{% for tag in source_tags %}<tag>{{ tag|x }}</tag>{% endfor %}</tags>{% endif %}
	</source>
	<feed>
		<title>{{ feed_title|x }}</title>
		<uri>{{ feed_url|x }}</uri>
	</feed>
	<uri>{{ url|x }}</uri>
	{% if publish_date %}<publish-date>{{ publish_date|iso_utc|x }}</publish-date>{% endif %}
	<retrieved-date>{{ retrieved_date|iso_utc|x }}</retrieved-date>
	<lang>{{ lang|x }}</lang>
	{% if google_story_id %}<google_cluster_id>{{ google_story_id|x }}</google_cluster_id>{% endif %}
	{% if bloomberg_score %}<x-blb>{{ bloomberg_score|x }}</x-blb>{% endif %}
	{% if geo %}{% for g in geo %}{{ geo_xml(g) }}{% endfor %}{% endif %}
	{% if tags %}<tags>{% for tag in tags %}<tag>{{ tag|x }}</tag>{% endfor %}</tags>{% endif %}
	{% if img %}<img>{{ img|x }}</img>{% endif %}
	<title>{{ title|x }}</title>
	<body-cleartext>{{ cleartext|mark_paras|safe }}</body-cleartext>
	{% if rych %}<body-rych>{{ rych|remove_xml_header|safe }}</body-rych>{% endif %}
	{% if xrych %}<body-xlike>{{ xrych|remove_xml_header|safe }}</body-xlike>{% endif %}
</article>
''')

########

def xml_encode(article):
	"XML encoding of an article"
	xml = TEMPLATE.render(article).encode('utf8','replace')
	return '\n'.join(line for line in xml.splitlines() if line.strip())

	
