import cProfile
import csv
import io
import json
import os
import pstats
import re
import sys
import time
import uuid

from utils.fixtures import CatStream, CONFIG, clear_db, create_schema

from target_snowflake import main

def snag_sys_output():
    out = io.StringIO()
    err = io.StringIO()

    sys.stdout = out
    sys.stderr = err

    return out, err

def reset_sys_output():
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__

OUTPUT_FOLDER = ''
PROFILES = []

def append_write(filename):
    if os.path.exists(filename):
        return 'a' # append if already exists

    return 'w' # make a new file if not

def setup_output(folder):
    global OUTPUT_FOLDER
    OUTPUT_FOLDER = folder
    os.makedirs(folder)

def setup_profiling():
    global PROFILES
    PROFILES = []

def dump_profile_stats(prof):
    global PROFILES
    profile_filename = '{}/{}.prof'.format(
            OUTPUT_FOLDER,
            uuid.uuid4())
    prof.dump_stats(profile_filename)
    PROFILES.append(profile_filename)

def spit_profile_stats(folder, profile_filenames):
    if profile_filenames:
        with open('{}/prof.log'.format(folder), 'w') as f:
            stats = pstats.Stats(profile_filenames[0], stream=f)

            for filename in profile_filenames[1:]:
                stats.add(filename)
        
            stats.sort_stats('cumulative').print_stats(2000)

def log(message, details={}):
    filename = '{}/hardcopy.log'.format(OUTPUT_FOLDER)

    with open(filename, append_write(filename)) as f:
        f.write('INFO {} {} {}\n'.format(
            time.strftime('%Y%m%d %H:%M:%S'),
            json.dumps(details),
            message))

def timer_to_dict_key(timer_metric):
    return '{}:{}'.format(
        timer_metric['metric'],
        ':'.join([str(x) for x in sorted(timer_metric['tags'].items())]))

def annotate_percentages(elapsed_time, parsed_metric):
    for _key, value in parsed_metric.items():
        value['%'] = (value['total'] / elapsed_time) * 100

def parse_to_metrics(elapsed_time, lines):
    code_accum = {'main': {'calls': 1, 'total': elapsed_time, 'details': 'MAIN'}}
    remote_accum = {'main': {'calls': 1, 'total': elapsed_time, 'details': 'MAIN'}}
    
    for line in lines:
        parsed_line = re.search(r'INFO (\w+): (.*)$', line)
        if parsed_line:
            if parsed_line.group(1) == 'METRIC':
                loaded_metric = json.loads(parsed_line.group(2))

                if loaded_metric.get('type') == 'timer':
                    key = timer_to_dict_key(loaded_metric)

                    accum_value = code_accum.get(key,
                                                 {'calls': 0,
                                                  'total': 0,
                                                  'details': {'metric': loaded_metric['metric'],
                                                              'tags': loaded_metric['tags']}})
                    accum_value['calls'] += 1
                    accum_value['total'] += loaded_metric['value'] * 1000

                    code_accum[key] = accum_value

            elif parsed_line.group(1) == 'MillisLoggingCursor':
                query_details = re.search(r'(\d+) millis spent executing:(.*)$',parsed_line.group(2))

                query = query_details.group(2)
                query_execution_time = int(query_details.group(1))

                accum_value = remote_accum.get(query,
                                               {'calls': 0,
                                                'total': 0,
                                                'details': {'query': query}})
                accum_value['calls'] += 1
                accum_value['total'] += query_execution_time

                remote_accum[query] = accum_value

    annotate_percentages(elapsed_time, code_accum)
    annotate_percentages(elapsed_time, remote_accum)

    return {
        'code': code_accum,
        'remote': remote_accum,
        'total': elapsed_time}

def mutate_metric_accum(accum, parsed_metric):
    for key, metric in parsed_metric.items():
        accum_value = accum.get(key,
                                {'calls': 0,
                                 'total': 0,
                                 'min_calls': float('inf'),
                                 'min_total': float('inf'),
                                 'max_calls': float('-inf'),
                                 'max_total': float('-inf'),
                                 'details': metric['details']})

        accum_value['calls'] += metric['calls']
        accum_value['total'] += metric['total']

        accum_value['min_calls'] = min(accum_value['min_calls'], metric['calls'])
        accum_value['max_calls'] = max(accum_value['max_calls'], metric['calls'])
        accum_value['min_total'] = min(accum_value['min_total'], metric['total'])
        accum_value['max_total'] = max(accum_value['max_total'], metric['total'])


        accum[key] = accum_value

def annotate_averages(n, parsed_metric):
    for key, metric in parsed_metric.items():
        metric['average_calls'] = metric['calls'] / n
        metric['average_total'] = metric['total'] / n

def metrics_breakdown(parsed_metrics):
    length = len(parsed_metrics)
    code_accum = {}
    remote_accum = {}

    total_time = 0

    for parsed_metric in parsed_metrics:
        total_time += parsed_metric['total']

        mutate_metric_accum(code_accum, parsed_metric['code'])
        mutate_metric_accum(remote_accum, parsed_metric['remote'])
        
    annotate_averages(length, code_accum)
    annotate_averages(length, remote_accum)
    annotate_percentages(total_time, code_accum)
    annotate_percentages(total_time, remote_accum)

    return {
        'code': code_accum,
        'remote': remote_accum}

def measure_performance(stream_size, **kwargs):
    log('Measuring performance', kwargs)
    stream = CatStream(stream_size)

    log('    Setting up db...', kwargs)
    clear_db()

    create_schema()
    log('    DB setup.', kwargs)

    _out, err = snag_sys_output()
    timestamp = time.monotonic()

    prof = cProfile.Profile()
    try:
        prof.enable()
        main(CONFIG, input_stream=stream)
    finally:
        elapsed_time = int((time.monotonic() - timestamp) * 1000)
        prof.disable()
        reset_sys_output()

        dump_profile_stats(prof)

    log('Finished measuring performance', kwargs)
    
    return parse_to_metrics(elapsed_time, err.getvalue().splitlines())

def _spit_a_breakdown(file_path, parsed_metric, ids):
    prefixed_ids = {}
    for k, v in ids.items():
        prefixed_ids["id_" + str(k)] = v

    filename = file_path + '.csv'
    append_or_write = append_write(filename)

    with open(filename, append_or_write, newline='') as csvfile:
        values = list(parsed_metric.values())
        fieldnames = list(prefixed_ids.keys()) + list(values[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if append_or_write == 'w':
            writer.writeheader()

        for value in values:
            writer.writerow({**value, **prefixed_ids})
    
    log('Breakdown spat.', {'file': file_path + '.csv', 'append_or_write': append_or_write})

def spit_breakdown(folder, prefix, breakdown, **ids):
    _spit_a_breakdown('{}/{}_{}'.format(folder, prefix, 'code'), breakdown['code'], ids)
    _spit_a_breakdown('{}/{}_{}'.format(folder, prefix, 'remote'), breakdown['remote'], ids)

def performance(calls, powers_start, powers_end):
    execution_id = time.strftime('%Y_%m_%d_%H_%M_%S')
    # Make tmp folder to hold performance files
    folder = '/code/tmp-performance/' + execution_id

    setup_output(folder)
    setup_profiling()

    total_calls = []

    for n in range(powers_start, powers_end):
        stream_size = 10**n
        log('Starting stream size {}'.format(stream_size), {'calls': calls, 'powers': [powers_start, powers_end]})
        parsed_metrics = [measure_performance(stream_size, call=i, stream_sized=stream_size) for i in range(0, calls)]
        total_calls += parsed_metrics

        spit_breakdown(
            folder,
            'streams',
            metrics_breakdown(parsed_metrics),
            stream_size=stream_size,
            execution=execution_id)
    
    spit_breakdown(
        folder,
        'total',
        metrics_breakdown(total_calls),
        execution=execution_id)
    spit_profile_stats(folder, PROFILES)

def test():
    performance(3, 1, 2)



