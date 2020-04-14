import datetime, re, requests, sqlite3
from time import time, sleep, strftime, strptime, localtime

from Covid2p1_Backend_RND import *

BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{0}_global.csv'
REALTIME_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/cases_country.csv'
DATE_RE = re.compile('[0-9]{1,2}/[0-9]{1,2}/[0-9]{1,2}')
CSV_KEYS = ['confirmed', 'deaths', 'recovered']
MATH_KEYS = ['new', 'active']
LOCALES_USED = ['ENG', 'ARAB', 'ESP']
TIMEOUT_RETRIES = 5

def csvDate2date(timestring):
    t = strptime(timestring, '%m/%d/%y')
    return datetime.date(t.tm_year, t.tm_mon, t.tm_mday)

def rtDate2date(timestring):
    t = strptime(timestring, '%Y-%m-%d %H:%M:%S')
    return datetime.date(t.tm_year, t.tm_mon, t.tm_mday)



##### Data fillers

COORDINATES = {}
DATA = {k:{} for k in CSV_KEYS+MATH_KEYS}
RT_DATA = {k:{} for k in CSV_KEYS+MATH_KEYS}
DATES = []
RT_DATE = None
N_DAYS = 0
RT_COUNTRIES = None



def fetch_set(key):
    for i in range(TIMEOUT_RETRIES):
        try:
            resp = requests.get(BASE_URL.format(key))
            print('Fetch done:    \'{0}\''.format(key))
            break
        except requests.exceptions.ConnectTimeout:
            if i == TIMEOUT_RETRIES-1:
                print('Timeout {0} at key \'{1}\', skipping.'.format(i+1, key))
                return False, None
            else:
                print('Timeout {0} at key {1}, retrying...'.format(i+1, key))
                continue
    return resp.ok, resp.text.strip() if resp.ok else None



def fetch_realtime():
    for i in range(TIMEOUT_RETRIES):
        try:
            resp = requests.get(REALTIME_URL)
            print('Fetch done:    \'realtime\'')
            break
        except requests.exceptions.ConnectTimeout:
            if i == TIMEOUT_RETRIES-1:
                print('Timeout {0} at key \'realtime\', skipping.'.format(i+1))
                return False, None
            else:
                print('Timeout {0} at key realtime, retrying...'.format(i+1))
                continue
    return resp.ok, resp.text.strip() if resp.ok else None



def fill_data():	
    global DATA, DATES, N_DAYS, RT_COUNTRIES, RT_DATA, RT_DATE
    
    for k in CSV_KEYS:
        ### Fetching dataset
        ok, source = fetch_set(k)
        if not ok:
            return None
        source = split_csv(source)
        
        ### Filling dates
        if len(DATES) == 0:
             DATES = list(map(csvDate2date, source[0][4:]))
             N_DAYS = len(DATES)
        
        for line in source[1:]:
            p, c = line[:2]
            
            ### Filling coordinates
            cp_key = c+' : '+p
            if cp_key not in COORDINATES:
                COORDINATES[cp_key] = tuple(map(float, line[2:4]))
            
            ### Filling cases
            tab = DATA[k]
            for i in range(4, len(line)):
                if not (c in tab):
                    tab[c] = {}
                c_dict = tab[c]
                if not (p in c_dict):
                    c_dict[p] = []
                c_dict[p].append(int(line[i]))
        
    ### Filling realtime
    ok, source = fetch_realtime()
    if not ok:
        return None
    source = split_csv(source, 0)
    indices = {}
    line = source[0]
    RT_DATE = rtDate2date(source[1][1])
    samedayflag = not (RT_DATE == DATES[-1])
    for i in range(len(line)):
        val = line[i].lower()
        if val in CSV_KEYS:
            indices[val] = i
    for line in source[1:]:
        c = line[0]
        for k in indices:
            val = int(line[indices[k]])
            RT_DATA[k][c] = val
            if samedayflag and ('' in DATA[k][c]):
                DATA[k][c][''][-1] = val
    RT_COUNTRIES = set(RT_DATA[CSV_KEYS[0]].keys())
    #print(RT_DATA[CSV_KEYS[0]])
    
    ### Setting coordinates
    countries = []
    for c in DATA[CSV_KEYS[0]]:
        countries.append(c)
    if len(countries) > 0:
        coords, still_missing = get_coordinates(countries)
##    if len(still_missing) > 0:
##        print('Still missing the following countries:', still_missing)
    for c in coords:
        if not (coords[c] == None):
            COORDINATES[c+' : '] = coords[c]
    
    ### Setting special coordinates (from null)
    locs = sp_locs()
    for c in locs:
        COORDINATES[c] = locs[c]
    
    return None





##### Analysis

POW_GLOBAL = 1
POW_LATAM = 1
RATING_LIMIT = 30

def gen_global_dbg(power):
    for k in CSV_KEYS:
        tab = DATA[k]
        filename = f'Output/chart_{power}.tab'
        data = []
        coords = []
        for c in tab:
            c_dict = tab[c]
            if '' in c_dict:
                for p in c_dict:
                    data.append(c_dict[p])
                    coords.append(COORDINATES[c+' : '+p])
            else:
                vals = [0 for _ in range(N_DAYS)]
                for p in c_dict:
                    p_list = c_dict[p]
                    for d_i in range(N_DAYS):
                        vals[d_i] += p_list[d_i]
                data.append(vals)
                coords.append(COORDINATES[c+' : '])
        write_tabfile(data, coords, DATES, power, filename)
        print(f'Chart completed:    global     {power}')
    return None

def gen_charts_global_unified():
    for k in CSV_KEYS:
        tab = DATA[k]
        filename = f'Output/chart_global_{k}_{DATES[-1].isoformat()}.tab'
        data = []
        coords = []
        for c in tab:
            c_dict = tab[c]
            if '' in c_dict:
                for p in c_dict:
                    data.append(c_dict[p])
                    coords.append(COORDINATES[c+' : '+p])
            else:
                vals = [0 for _ in range(N_DAYS)]
                for p in c_dict:
                    p_list = c_dict[p]
                    for d_i in range(N_DAYS):
                        vals[d_i] += p_list[d_i]
                data.append(vals)
                coords.append(COORDINATES[c+' : '])
        write_tabfile(data, coords, DATES, POW_GLOBAL, filename)
        print(f'Chart completed:    global    \'{k}\'')
    return None

def gen_charts_latam():
    A, B, C, D = LATAM_LIMITS
    for k in CSV_KEYS:
        tab = DATA[k]
        filename = f'Output/chart_latam_{k}_{DATES[-1].isoformat()}.tab'
        data = []
        coords = []
        for c in tab:
            c_dict = tab[c]
            if '' in c_dict:
                lat, long = COORDINATES[c+' : ']
                if not (A < lat < B)*(C < long < D):
                    continue
                for p in c_dict:
                    data.append(c_dict[p])
                    coords.append(COORDINATES[c+' : '+p])
            else:
                vals = [0 for _ in range(N_DAYS)]
                for p in c_dict:
                    lat, long = COORDINATES[c+' : '+p]
                    if not (A < lat < B)*(C < long < D):
                        continue
                    p_list = c_dict[p]
                    for d_i in range(N_DAYS):
                        vals[d_i] += p_list[d_i]
                data.append(vals)
                coords.append(COORDINATES[c+' : '])            
        write_tabfile(data, coords, DATES, POW_GLOBAL, filename)
        print(f'Chart completed:    latam    \'{k}\'')
    return None



def gen_linegraphs():
    for k in CSV_KEYS:
        tab = DATA[k]
        filename_g = f'Output/linegraph_{k}_{DATES[-1].isoformat()}.txt'
        filename_m = f'Output/maxval_{k}_{DATES[-1].isoformat()}.txt'
        vals = [0 for _ in range(N_DAYS)]
        for c in tab:
            c_dict = tab[c]
            for p in c_dict:
                for i in range(N_DAYS):
                    vals[i] += c_dict[p][i]
        with open(filename_g, 'w') as f:
            f.write('#'.join(map(str, vals)))
        with open(filename_m, 'w') as f:
            f.write(str(max(vals)))
        print(f'Linegraph completed:    \'{k}\'')
    return None



def gen_linegraphs_rt():
    nextdayflag = not (RT_DATE == DATES[-1])
    for k in CSV_KEYS:
        tab = DATA[k]
        rt_tab = RT_DATA[k]
        filename_g = f'Output/linegraph_RT_{k}_{RT_DATE.isoformat()}.txt'
        filename_m = f'Output/maxval_RT_{k}_{RT_DATE.isoformat()}.txt'
        vals = [0 for _ in range(N_DAYS+(1 if nextdayflag else 0))]
        for c in tab:
            c_dict = tab[c]
            for p in c_dict:
                for i in range(N_DAYS):
                    vals[i] += c_dict[p][i]
        if nextdayflag:
            for c in tab:
                if c in RT_COUNTRIES:
                    vals[-1] += rt_tab[c]
                else:
                    c_dict = tab[c]
                    for p in c_dict:
                        vals[-1] += c_dict[p][-1]
        with open(filename_g, 'w') as f:
            f.write('#'.join(map(str, vals)))
        with open(filename_m, 'w') as f:
            f.write(str(max(vals)))
        print(f'RT linegraph completed:    \'{k}\'')
    return None



def gen_ratings():
    l_keys, translations = None, None
    for k in CSV_KEYS:
        tab = DATA[k]
        filename = f'Output/rating_{{0}}_{k}_{DATES[-1].isoformat()}.txt'
        data = {}
        for c in tab:
            c_dict = tab[c]
            val = 0
            for p in c_dict:
                val += c_dict[p][-1]
            data[c] = val
        
        if translations == None:
            l_keys, translations, _ = build_country_dictionary(set(tab.keys()))

        l_keys = list(filter(lambda x: x in LOCALES_USED, l_keys))
        rating = sorted(data.items(), key = lambda x: -x[1])
        text_val = rating[0][1]
        texts_c = {l_k:translations[rating[0][0]][l_k] for l_k in l_keys}
        text_val = str(text_val)
        for c, val in rating[1:]:
            for l_k in l_keys:
                texts_c[l_k] += ','+translations[c][l_k]
            text_val += '#'+str(val)
        for l_k in l_keys:
            with open(filename.format(f'countries_{l_k}'), 'w', encoding = 'utf-8') as f:
                f.write(texts_c[l_k])
            with open(filename.format(f'TOP{RATING_LIMIT}_countries_{l_k}'), 'w', encoding = 'utf-8') as f:
                f.write(','.join(texts_c[l_k].split(',')[:RATING_LIMIT]))
        with open(filename.format('values'), 'w') as f:
            f.write(text_val)
        with open(filename.format(f'TOP{RATING_LIMIT}_values'), 'w') as f:
            f.write('#'.join(text_val.split('#')[:RATING_LIMIT]))
        print(f'Rating completed:    \'{k}\'')
    return None



def gen_ratings_rt():
    l_keys, translations = None, None
    nextdayflag = not (RT_DATE == DATES[-1])
    for k in CSV_KEYS:
        tab = DATA[k]
        rt_tab = RT_DATA[k]
        filename = f'Output/rating_RT_{{0}}_{k}_{RT_DATE.isoformat()}.txt'
        data = {}
        for c in tab:
            if nextdayflag and (c in RT_COUNTRIES):
                val = rt_tab[c]
                #print(0,type(val))
            else:
                c_dict = tab[c]
                val = 0
                for p in c_dict:
                    val += c_dict[p][-1]
                #print(1,type(val))
            data[c] = val
        
        if translations == None:
            l_keys, translations, _ = build_country_dictionary(set(tab.keys()))

        l_keys = list(filter(lambda x: x in LOCALES_USED, l_keys))
        
        rating = sorted(data.items(), key = lambda x: -x[1])
        text_val = rating[0][1]
        texts_c = {l_k:translations[rating[0][0]][l_k] for l_k in l_keys}
        text_val = str(text_val)
        for c, val in rating[1:]:
            for l_k in l_keys:
                texts_c[l_k] += ','+translations[c][l_k]
            text_val += '#'+str(val)
        for l_k in l_keys:
            with open(filename.format(f'countries_{l_k}'), 'w', encoding = 'utf-8') as f:
                f.write(texts_c[l_k])
            with open(filename.format(f'TOP{RATING_LIMIT}_countries_{l_k}'), 'w', encoding = 'utf-8') as f:
                f.write(','.join(texts_c[l_k].split(',')[:RATING_LIMIT]))
        with open(filename.format('values'), 'w') as f:
            f.write(text_val)
        with open(filename.format(f'TOP{RATING_LIMIT}_values'), 'w') as f:
            f.write('#'.join(text_val.split('#')[:RATING_LIMIT]))
        print(f'RT rating completed:    \'{k}\'')
    return None





##### Run

def set_params():
    with open('params.txt') as f:
        source = f.read().strip()
    for line in source.split('\n'):
        name, val_type, val = line.split('\t')
        globals()[name] = getattr(__builtins__, val_type)(val)
    return None

if __name__ == '__main__':
    print('Started!')
    set_params()
    fill_data()
    gen_charts_global_unified()
    gen_charts_latam()
##    gen_linegraphs()
    gen_linegraphs_rt()
##    gen_ratings()
    gen_ratings_rt()
    print('Finished!')

##CKEYS,CDICT,MISSING=build_country_dictionary(set(DATA['confirmed'].keys()))
