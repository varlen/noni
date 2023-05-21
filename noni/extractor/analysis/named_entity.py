import re
from collections import Counter

named_entity_regexps = { k : re.compile(v) for k,v in {
    'CPF' : '(^\d{10,11}$)|(^\d{3}\.\d{3}\.\d{3}-\d{2}$)',
    'CEP' : '^(\d{8}|\d{2}\.?\d{3}\-\d{3})$',
    'CreditCardNumber' : '^(\d{4} ?){4}$',
    'CNPJ' : '^(\d{14}|\d{2}\.?\d{3}\.?\d{3}\/?\d{4}\-?\d{2})$',
    'LicensePlate' : '(^([a-z]|[A-Z]){3}-?\d{4}$)|(^([a-z]|[A-Z]){3}\d([a-z]|[A-Z])\d{2}$)',
    'ID' : '^\d{2}.?\d{3}.?\d{3}-?\d$',
    'Name' : '^(\w+ ?)+$',
    'Email' : '^[a-z0-9.]+@[a-z0-9]+\.[a-z]+\.([a-z]+)?$',
    'UUID' : '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$',
    'PhoneNumber' : '^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$',
    'PIS' : '^\d{11}$',
    'Address' : '^(\w+ ?)+.+\d+$'
}.items()}

def test_sample_for_named_entity(sample):
    for entity_name, regex in named_entity_regexps.items():
        if regex.fullmatch(sample):
            return entity_name.lower()
    return 'unknown'

def match_entity(samples):
    if not len(samples):
        return 'unknown'
    votes = Counter([ test_sample_for_named_entity(s) for s in samples ])
    return votes.most_common(1)[0][0]

def try_categories_from_samples(samples):
    categorizer = Counter(samples)
    return [ category for category, _ in categorizer.most_common() ]