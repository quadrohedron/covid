import re



LATAM_LIMITS = [-60, 25, -120, -30]

UHEIGHT = 0.102
FACTOR = 100000



def __scale(val, unitscale, power=1):
    return int(FACTOR*((1.0-UHEIGHT)*(pow(val, power)-unitscale)/(1.0-unitscale)+UHEIGHT)) if val > 0 else 0



def __scale_vals(val_table, power=1):
    maxval = max(list(map(max, val_table)))
    unitscale = pow(1/maxval, power)
    res = []
    for val_list in val_table:
        res.append([__scale(v/maxval, unitscale, power) for v in val_list])
    return res



def sp_locs():
    with open('sp_locs.txt') as f:
        source = f.read().strip()
    res = {}
    for line in source.split('\n'):
        c, lat, long = line.split('\t')
        lat, long = map(float, (lat, long))
        res[c] = (lat, long)
    return res



__SKIPPED_CHARS = '*'

__COUNTRY_REPLACEMENTS = {
    'Korea, South'          :   'South Korea',
    'Congo (Brazzaville)'   :   'Congo [Republic]',
    'Congo (Kinshasa)'      :   'Congo [DRC]'
    }

__IGNORED_PATTERNS = tuple(map(re.compile, (
    '\ARecovered,Canada,.*\Z',
    #'\ADiamond Princess,Canada,.*\Z'
    )))



def split_csv(text, c_ind = 1):
    res = []
    for line in text.split('\n'):
        
        ignored = False
        for p in __IGNORED_PATTERNS:
            if not (p.match(line) == None):
                ignored = True
                break
        if ignored:
            continue
        
        res_line = []
        field = ''
        depth = 0
        for char in line:
            if char == '"':
                depth = 1 - depth
            elif char == ',':
                if depth > 0:
                    field += char
                else:
                    if (len(res_line) == c_ind) and (field in __COUNTRY_REPLACEMENTS):
                        field = __COUNTRY_REPLACEMENTS[field]
                    res_line.append(field)
                    field = ''
            elif not (char in __SKIPPED_CHARS):
                field += char
        res_line.append(field)
        res.append(res_line)
        
    return res


def get_coordinates(countries):
    res = {c:None for c in countries}
    with open('countries.tab') as f:
        source = f.read().strip().split('\n')
    for line in source:
        line = line.split('\t')
        c = line[-1].strip()
        if c in countries:
            res[c] = tuple(map(float, line[-3:-1]))
    missing = []
    for c in res:
        if res[c] == None:
            missing.append(c)
    return res, missing



def write_tabfile(data_list, coords_list, date_list, power, filename):
    tab_vals = __scale_vals(data_list, power)
    with open(filename, 'w') as f:
        for c_i in range(len(tab_vals)):
            for d_i in range(len(date_list)):
                line = date_list[d_i].isoformat()+'\t00\t'
                line += '\t'.join(map(str, coords_list[c_i]))
                line += '\t'+str(tab_vals[c_i][d_i])
                f.write(line+'\n')
    return None



def build_country_dictionary(country_set):
    with open('country_dictionary.csv',encoding='utf-8') as f:
        source = f.read().strip()
    res = {}
    source = source.split('\n')
    keys = source[0].replace('\ufeff', '').split(',')
    eng_i = keys.index('ENG')
    n_keys = len(keys)
    for line in source[1:]:
        names = line.split(',')
        if len(tuple(filter(None, names))) == 0:
            continue
        engname = names[eng_i]
        if engname in country_set:
            translations = {}
            for k_i in range(n_keys):
                translations[keys[k_i]] = names[k_i]
            res[engname] = translations
            country_set.remove(engname)
    return keys, res, country_set
